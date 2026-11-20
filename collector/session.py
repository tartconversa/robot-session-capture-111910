"""会话级采集引擎。

职责边界（与需求逐条对应）：
- 通知可能重复或丢段：重复幂等吸收，缺段由 backfill 按 manifest 闭区间核实；
- 身份是 (session, channel, epoch, seq)：摘要相同但归属不同各自保留，
  同身份出现不同摘要 -> 冲突隔离，绝不让最后到达者取胜；
- 授权边界只通过同步锚点对应到各通道序号，对应不出来保留 unmapped 缺口；
- 屏蔽通知覆盖已存帧时删除三处（raw/managed/exports）原件，只留墓碑；
- 完整性 = 结束声明 + 各通道范围核实 + 授权核对，缺一不可；
- 不训练、不插值，缺口如实记录。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from collector import anchors
from collector.model import Claim, Item, ItemStatus, SessionStatus, item_key
from collector.source_client import SourceError, SourceRedacted, SourceStaleEpoch, SourceUnavailable
from collector.storage import DigestMismatch, Storage


@dataclass
class SessionState:
    session: str
    revision: int = 0
    ended: bool = False
    channels: dict = field(default_factory=dict)          # channel -> ChannelRange dict
    anchor_rows: list[dict] = field(default_factory=list)
    grant: dict = field(default_factory=lambda: {"action_types": [], "session_ms": None})
    redactions: list[dict] = field(default_factory=list)
    items: dict[str, dict] = field(default_factory=dict)  # key -> Item.to_dict()
    stopped: bool = False
    channel_errors: dict = field(default_factory=dict)
    last_source_error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "session": self.session,
            "revision": self.revision,
            "ended": self.ended,
            "channels": self.channels,
            "anchors": self.anchor_rows,
            "grant": self.grant,
            "redactions": self.redactions,
            "items": self.items,
            "stopped": self.stopped,
            "channel_errors": self.channel_errors,
            "last_source_error": self.last_source_error,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SessionState":
        return cls(
            session=d["session"],
            revision=d.get("revision", 0),
            ended=d.get("ended", False),
            channels=d.get("channels", {}),
            anchor_rows=d.get("anchors", d.get("anchor_rows", [])),
            grant=d.get("grant", {"action_types": [], "session_ms": None}),
            redactions=d.get("redactions", []),
            items=d.get("items", {}),
            stopped=d.get("stopped", False),
            channel_errors=d.get("channel_errors", {}),
            last_source_error=d.get("last_source_error"),
        )


class SessionEngine:
    def __init__(self, session: str, request: dict, storage: Storage):
        self.session = session
        self.request = request
        self.storage = storage
        self.lock = threading.RLock()
        self.state = SessionState(session=session)
        self._restore()

    # ---------- 持久化与恢复 ----------
    def _restore(self) -> None:
        persisted = self.storage.load_state()
        if not persisted or persisted.get("session") != self.session:
            return
        self.state = SessionState.from_dict(persisted)
        # 采集中断后续跑：用磁盘事实重新区分暂存与已验证。
        for item in self._items():
            if item.status == ItemStatus.VERIFIED.value:
                if not item.rel_path or not self.storage.blob_exists(item.rel_path):
                    item.status = ItemStatus.STAGED.value
                    item.rel_path = None
                    self._put(item)
            elif item.status == ItemStatus.STAGED.value and item.rel_path and self.storage.blob_exists(item.rel_path):
                # 崩溃于字节落盘之后、状态更新之前
                item.status = ItemStatus.VERIFIED.value
                self._put(item)
        self._save()

    def _save(self) -> None:
        self.storage.save_state(self.state.to_dict())

    def _items(self) -> list[Item]:
        return [Item.from_dict(d) for d in self.state.items.values()]

    def _get(self, channel: str, epoch: int, seq: int) -> Optional[Item]:
        d = self.state.items.get(item_key(channel, epoch, seq))
        return Item.from_dict(d) if d else None

    def _put(self, item: Item) -> None:
        self.state.items[item_key(item.channel, item.epoch, item.seq)] = item.to_dict()

    # ---------- 来源输入 ----------
    def apply_manifest(self, manifest: dict) -> bool:
        """吸收 manifest（含 SSE 首事件与轮询）。revision 取高，旧修订不回滚。返回是否为新修订。"""
        with self.lock:
            rev = int(manifest.get("revision", 0))
            newer = rev > self.state.revision
            if not newer and self.state.revision:
                return False
            self.state.revision = rev
            self.state.ended = bool(manifest.get("ended"))
            self.state.channels = {
                ch: {"epoch": int(spec["epoch"]), "from": int(spec["from"]), "to": int(spec["to"])}
                for ch, spec in manifest.get("channels", {}).items()
            }
            if manifest.get("anchors"):
                self.state.anchor_rows = manifest["anchors"]
            self.state.grant = manifest.get("grant", self.state.grant)
            self.state.redactions = manifest.get("redactions", [])
            # 新结束修订或屏蔽通知 -> 重新判断受影响范围
            self._apply_redactions()
            self._recompute_claims()
            self._save()
            return True

    def apply_fragment(self, descriptor: dict) -> None:
        """吸收一条实时 fragment 通知（允许重复）。"""
        with self.lock:
            ch, epoch, seq, digest_hex = (
                descriptor["channel"], int(descriptor["epoch"]),
                int(descriptor["seq"]), descriptor["sha256"],
            )
            item = self._get(ch, epoch, seq)
            if item is not None and item.status == ItemStatus.REDACTED.value:
                # 屏蔽墓碑不可被后续到达的片段复活，仅留痕摘要
                if digest_hex not in item.digests:
                    item.digests.append(digest_hex)
                    self._put(item)
                    self._save()
                return
            if item is None:
                item = Item(
                    channel=ch, epoch=epoch, seq=seq,
                    action_type=descriptor.get("action_type"),
                    digests=[], status=ItemStatus.STAGED.value,
                )
            if digest_hex not in item.digests:
                item.digests.append(digest_hex)
            if len(item.digests) > 1:
                # 同一身份出现不同摘要：隔离整个该通道范围，不以后到者覆盖
                self._isolate(item, reason="conflicting_digest_notice")
            self._put(item)
            self._recompute_claims()
            self._save()

    def _isolate(self, item: Item, reason: str) -> None:
        if item.status == ItemStatus.REDACTED.value:
            return
        from collector.storage import raw_rel_path
        # 字节可能按任一已知摘要的文件名落盘（冲突通知可能晚于下载），逐个尝试移走
        moved = None
        if item.rel_path and self.storage.blob_exists(item.rel_path):
            digest_in_name = Path(item.rel_path).name.split(".")[1]
            moved = self.storage.quarantine(
                self.session, item.channel, item.epoch, item.seq,
                digest_in_name, src_rel=item.rel_path,
            )
        if moved is None:
            for digest_hex in item.digests:
                candidate_rel = raw_rel_path(item.channel, item.epoch, item.seq, digest_hex)
                if self.storage.blob_exists(candidate_rel):
                    moved = self.storage.quarantine(
                        self.session, item.channel, item.epoch, item.seq, digest_hex,
                        src_rel=candidate_rel,
                    )
                    break
        item.rel_path = moved
        item.status = ItemStatus.CONFLICT.value
        item.gap_reason = reason
        item.claim = Claim.NOT_VERIFIED.value

    def _reconcile_conflicts(self) -> None:
        """冲突项的任何原始区字节都必须移走，防止并发窗口遗留可领取原件。"""
        from collector.storage import raw_rel_path
        for item in self._items():
            if item.status != ItemStatus.CONFLICT.value:
                continue
            for digest_hex in item.digests:
                rel = raw_rel_path(item.channel, item.epoch, item.seq, digest_hex)
                if self.storage.blob_exists(rel):
                    moved = self.storage.quarantine(
                        self.session, item.channel, item.epoch, item.seq,
                        digest_hex, src_rel=rel,
                    )
                    if moved and item.rel_path is None:
                        item.rel_path = moved
                    self._put(item)

    def _apply_redactions(self) -> None:
        for red in self.state.redactions:
            ch, epoch = red["channel"], int(red["epoch"])
            lo, hi = (int(x) for x in red["seq"])
            for seq in range(lo, hi + 1):
                item = self._get(ch, epoch, seq)
                if item is None:
                    # 屏蔽先于数据到达：先立墓碑，后续任何到达都不得复活
                    item = Item(
                        channel=ch, epoch=epoch, seq=seq, action_type=None,
                        digests=[], status=ItemStatus.REDACTED.value,
                        gap_reason="redacted_before_arrival",
                    )
                if item.status == ItemStatus.REDACTED.value:
                    self._put(item)
                    continue
                if item.rel_path:
                    self.storage.delete_blob(item.rel_path)
                item.status = ItemStatus.REDACTED.value
                item.claim = Claim.REDACTED.value
                item.rel_path = None
                item.gap_reason = "redacted"
                self._put(item)

    # ---------- 按范围补取核实 ----------
    def _set_channel_error(self, channel: str, message: Optional[str]) -> None:
        errors = self.state.channel_errors
        if message:
            errors[channel] = message
        else:
            errors.pop(channel, None)
        self.state.last_source_error = "; ".join(sorted(errors.values())) or None

    def backfill_channel(self, client, channel: str) -> None:
        with self.lock:
            spec = self.state.channels.get(channel)
            if not spec:
                # 调用方可能先补取后见 manifest：先拉一次，保证锚点/授权就位
                try:
                    self.apply_manifest(client.get_manifest())
                except SourceError:
                    return
                spec = self.state.channels.get(channel)
                if not spec:
                    return
            epoch, frm, to = spec["epoch"], spec["from"], spec["to"]
            try:
                rows = client.backfill(channel, epoch, frm, to)
                self._set_channel_error(channel, None)
            except SourceUnavailable as exc:
                # 通道不可达保持欠采，已有结果不受影响
                self._set_channel_error(channel, f"{channel}: {exc}")
                self._mark_missing(channel, epoch, frm, to, ItemStatus.SOURCE_GAP, "channel_unavailable")
                self._save()
                return
            except SourceError as exc:
                self._set_channel_error(channel, f"{channel}: {exc}")
                self._save()
                return
            seen = {(int(r["epoch"]), int(r["seq"])) for r in rows}
            for seq in range(frm, to + 1):
                if (epoch, seq) not in seen:
                    # 补取接口同样缺段：来源缺段，不插值不编造
                    self._mark_missing(channel, epoch, seq, seq, ItemStatus.SOURCE_GAP, "missing_in_backfill")
            for row in rows:
                self._verify_descriptor(client, row)
            self._recompute_claims()
            self._save()
    def _mark_missing(self, channel, epoch, frm, to, status, reason) -> None:
        for seq in range(frm, to + 1):
            item = self._get(channel, epoch, seq)
            if item and item.status in (ItemStatus.VERIFIED.value, ItemStatus.CONFLICT.value,
                                        ItemStatus.REDACTED.value):
                continue
            if item is None:
                item = Item(channel=channel, epoch=epoch, seq=seq, action_type=None,
                            digests=[], status=status.value, gap_reason=reason)
            else:
                item.status = status.value
                item.gap_reason = reason
            self._put(item)

    def _verify_descriptor(self, client, descriptor: dict) -> None:
        ch = descriptor["channel"]
        epoch, seq, digest_hex = int(descriptor["epoch"]), int(descriptor["seq"]), descriptor["sha256"]
        if int(self.state.channels[ch]["epoch"]) != epoch:
            # 旧代次响应：按缺口处理，绝不并入新代次序号空间
            self._mark_missing(ch, epoch, seq, seq, ItemStatus.SOURCE_GAP, "stale_epoch")
            return
        item = self._get(ch, epoch, seq)
        if item is None:
            item = Item(ch, epoch, seq, descriptor.get("action_type"), [], ItemStatus.STAGED.value)
        if digest_hex not in item.digests:
            item.digests.append(digest_hex)
        if len(item.digests) > 1:
            # 实时阶段已发现冲突：即使此刻才拿到字节也只隔离，不验证为可领取
            self._isolate(item, "conflicting_digest_backfill")
            self._put(item)
            return
        if item.status in (ItemStatus.REDACTED.value, ItemStatus.CONFLICT.value):
            self._put(item)
            return
        if item.status == ItemStatus.VERIFIED.value and item.rel_path and self.storage.blob_exists(item.rel_path):
            self._put(item)
            return
        # 下载前复查：实时线程可能在此期间标记了冲突/屏蔽
        fresh = self._get(ch, epoch, seq)
        if fresh is not None and fresh.status in (ItemStatus.CONFLICT.value, ItemStatus.REDACTED.value):
            self._put(fresh)
            return
        try:
            raw = client.download_original(descriptor)
        except SourceRedacted:
            item.status = ItemStatus.REDACTED.value
            item.claim = Claim.REDACTED.value
            item.gap_reason = "redacted_on_download"
            self._put(item)
            return
        except (SourceUnavailable, SourceStaleEpoch) as exc:
            item.status = ItemStatus.SOURCE_GAP.value
            item.gap_reason = str(exc)
            self._put(item)
            return
        except SourceError as exc:
            item.gap_reason = f"download_failed: {exc}"
            self._put(item)
            return
        # 写盘前二次复查：冲突通知可能与下载并发到达
        fresh = self._get(ch, epoch, seq)
        if fresh is not None and fresh.status in (ItemStatus.CONFLICT.value, ItemStatus.REDACTED.value):
            # 已在隔离/屏蔽状态：刚取回的字节直接丢弃，不进入 raw 区
            self._put(fresh)
            return
        try:
            rel = self.storage.write_raw(ch, epoch, seq, digest_hex, raw)
        except DigestMismatch as exc:
            item.status = ItemStatus.SOURCE_GAP.value
            item.gap_reason = str(exc)
            self._put(item)
            return
        item.rel_path = rel
        item.status = ItemStatus.VERIFIED.value
        item.action_type = descriptor.get("action_type")
        item.gap_reason = None
        self._put(item)

    # ---------- 锚点映射与授权 ----------
    def _recompute_claims(self) -> None:
        grant_types = set(self.state.grant.get("action_types") or [])
        grant_ms = self.state.grant.get("session_ms")
        request_types = set(self.request.get("action_types") or [])
        request_ms = self.request.get("session_ms")
        for item in self._items():
            if item.status == ItemStatus.REDACTED.value:
                item.claim = Claim.REDACTED.value
                self._put(item)
                continue
            if item.status != ItemStatus.VERIFIED.value:
                item.claim = Claim.NOT_VERIFIED.value
                self._put(item)
                continue
            # 1) 锚点对应：对应不出来就保留 unmapped 缺口
            ms = anchors.seq_to_session_ms(self.state.anchor_rows, item.channel, item.seq)
            item.mapped_ms = ms
            if ms is None:
                item.claim = Claim.UNMAPPED.value
                self._put(item)
                continue
            # 2) 来源授权窗口与研究请求窗口取交集（闭区间）
            if not anchors.in_grant_window(ms, grant_ms):
                item.claim = Claim.UNAUTHORIZED.value
                self._put(item)
                continue
            if request_ms and not anchors.in_grant_window(ms, request_ms):
                item.claim = Claim.UNAUTHORIZED.value
                self._put(item)
                continue
            # 3) 动作类型：仅动作通道受 action_type 约束；相机/注记随会话
            if item.channel == "action":
                allowed_types = grant_types & request_types if request_types else grant_types
                if item.action_type not in allowed_types:
                    item.claim = Claim.UNAUTHORIZED.value
                    self._put(item)
                    continue
            item.claim = Claim.ALLOWED.value
            self._put(item)

    # ---------- 完整性评估与导出 ----------
    def evaluate(self) -> dict:
        with self.lock:
            self._apply_redactions()
            self._reconcile_conflicts()
            self._recompute_claims()
            items = self._items()
            allowed = [i for i in items if i.claim == Claim.ALLOWED.value]
            # 受管理副本只保留当前仍有依据可领取的原件
            self.storage.sync_managed(
                self.session, [i.rel_path for i in allowed if i.rel_path]
            )
            checklist = self._build_checklist(items)
            self.storage.export(self.session, checklist, allowed)
            self._save()
            return checklist

    def _build_checklist(self, items: list[Item]) -> dict:
        def tally(status: str) -> int:
            return sum(1 for i in items if i.status == status)

        def claim_tally(claim: str) -> int:
            return sum(1 for i in items if i.claim == claim)

        if self.state.stopped:
            overall = SessionStatus.STOPPED.value
        elif not self.state.ended:
            overall = SessionStatus.IN_PROGRESS.value
        elif tally(ItemStatus.CONFLICT.value):
            overall = SessionStatus.CONFLICTED.value
        elif tally(ItemStatus.REDACTED.value):
            overall = SessionStatus.PARTIALLY_REDACTED.value
        elif tally(ItemStatus.SOURCE_GAP.value) or claim_tally(Claim.UNMAPPED.value) \
                or self.state.last_source_error:
            overall = SessionStatus.UNDER_COLLECTED.value
        else:
            overall = self._check_complete(items)
        return {
            "session": self.session,
            "manifest_revision": self.state.revision,
            "ended": self.state.ended,
            "stopped": self.state.stopped,
            "overall_status": overall,
            "channel_ranges": self.state.channels,
            "anchors": self.state.anchor_rows,
            "grant": self.state.grant,
            "request": {
                "action_types": self.request.get("action_types", []),
                "session_ms": self.request.get("session_ms"),
            },
            "redactions": self.state.redactions,
            "counts": {
                "items": len(items),
                "verified": tally(ItemStatus.VERIFIED.value),
                "staged": tally(ItemStatus.STAGED.value),
                "source_gap": tally(ItemStatus.SOURCE_GAP.value),
                "conflict": tally(ItemStatus.CONFLICT.value),
                "redacted": tally(ItemStatus.REDACTED.value),
                "allowed": claim_tally(Claim.ALLOWED.value),
                "unauthorized": claim_tally(Claim.UNAUTHORIZED.value),
                "unmapped": claim_tally(Claim.UNMAPPED.value),
            },
            "items": [i.to_dict() for i in sorted(items, key=lambda i: (i.channel, i.epoch, i.seq))],
            "last_source_error": self.state.last_source_error,
        }

    def _check_complete(self, items: list[Item]) -> str:
        # 每个 manifest 通道闭区间的每个序号都必须有已核实原件（暂存/缺口均不算）
        for ch, spec in self.state.channels.items():
            epoch, frm, to = spec["epoch"], spec["from"], spec["to"]
            for seq in range(frm, to + 1):
                item = self._get(ch, epoch, seq)
                if item is None or item.status != ItemStatus.VERIFIED.value:
                    return SessionStatus.UNDER_COLLECTED.value
        # 授权核对：不得存在映射失败或未授权之外的未决原件
        if any(i.claim in (Claim.UNMAPPED.value, Claim.NOT_VERIFIED.value) for i in items):
            return SessionStatus.UNDER_COLLECTED.value
        return SessionStatus.COMPLETE.value

    def stop(self) -> None:
        """停止领取：冻结会话，保留全部已验证事实与缺口记录。"""
        with self.lock:
            self.state.stopped = True
            self._save()

    def resume(self) -> None:
        with self.lock:
            self.state.stopped = False
            self._save()

    def status(self) -> dict:
        with self.lock:
            return self._build_checklist(self._items())
