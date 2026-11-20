"""采集落盘：原件、受管理副本、导出包与状态文件。

安全约定：
- 原始字节只存在 raw/ 下按 (session, channel, epoch, seq) 归属的单独文件里，
  绝不拼接、绝不写入日志；临时文件按原件逐个创建并随失败立即清理。
- 冲突原件移入 quarantine/ 隔离，不进入受管理副本或导出。
- 屏蔽原件从 raw/、managed/、exports/ 三处同步删除，只在状态里留墓碑。
- 所有 JSON/字节写入均为同目录临时文件 + os.replace 原子替换。
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, Optional

from source.common import digest


class DigestMismatch(Exception):
    """下载字节的 SHA256 与来源描述不一致。"""


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_bytes(target: Path, data: bytes) -> None:
    _ensure_dir(target.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=target.name + ".", suffix=".part", dir=target.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()


def atomic_write_json(target: Path, payload) -> None:
    _ensure_dir(target.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=target.name + ".", suffix=".part", dir=target.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()


def raw_rel_path(channel: str, epoch: int, seq: int, digest_hex: str) -> str:
    return str(
        Path("raw") / channel / f"epoch{epoch}" / f"seq{seq}.{digest_hex[:12]}.bin"
    )


def quarantine_rel_path(session: str, channel: str, epoch: int, seq: int, digest_hex: str) -> str:
    return str(
        Path("quarantine") / session / channel / f"epoch{epoch}"
        / f"seq{seq}.{digest_hex[:12]}.bin"
    )


class Storage:
    def __init__(self, root: Path, managed_root: Path, export_root: Path):
        self.root = Path(root)
        self.managed_root = Path(managed_root)
        self.export_root = Path(export_root)
        _ensure_dir(self.root)

    # ---- 状态 ----
    @property
    def state_path(self) -> Path:
        return self.root / "state.json"

    def load_state(self) -> Optional[dict]:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        return None

    def save_state(self, state: dict) -> None:
        atomic_write_json(self.state_path, state)

    def resolve(self, rel_path: str) -> Path:
        # 禁止 rel_path 逃逸工作根
        candidate = (self.root / rel_path).resolve()
        root = self.root.resolve()
        if root not in candidate.parents and candidate != root:
            raise ValueError("path escapes storage root")
        return candidate

    # ---- 原始原件 ----
    def write_raw(self, channel: str, epoch: int, seq: int, expected_digest: str, data: bytes) -> str:
        actual = digest(data)
        if actual != expected_digest:
            raise DigestMismatch(f"{channel} epoch{epoch} seq{seq}: {actual} != {expected_digest}")
        rel = raw_rel_path(channel, epoch, seq, actual)
        target = self.resolve(rel)
        if not target.exists():
            atomic_write_bytes(target, data)
        return rel

    def quarantine(self, session: str, channel: str, epoch: int, seq: int, digest_hex: str,
                   src_rel: Optional[str] = None) -> Optional[str]:
        """把原件移入隔离区。src_rel 给定时按实际现存路径移动，否则按摘要名查找。"""
        candidates = []
        if src_rel:
            candidates.append(src_rel)
        candidates.append(raw_rel_path(channel, epoch, seq, digest_hex))
        for candidate in candidates:
            src = self.resolve(candidate)
            if src.exists():
                dst_rel = quarantine_rel_path(session, channel, epoch, seq, digest_hex)
                dst = self.resolve(dst_rel)
                _ensure_dir(dst.parent)
                os.replace(src, dst)
                return dst_rel
        return None

    def delete_blob(self, rel_path: Optional[str]) -> bool:
        if not rel_path:
            return False
        target = self.resolve(rel_path)
        if target.exists():
            target.unlink()
            return True
        return False

    def blob_exists(self, rel_path: Optional[str]) -> bool:
        return bool(rel_path) and self.resolve(rel_path).exists()

    # ---- 受管理副本 ----
    def _managed_path(self, session: str, rel_path: str) -> Path:
        return self.managed_root / session / Path(rel_path).name

    def sync_managed(self, session: str, allowed_rel_paths: Iterable[str]) -> None:
        allowed = {Path(p).name for p in allowed_rel_paths}
        session_dir = self.managed_root / session
        if session_dir.exists():
            for existing in session_dir.rglob("*.bin"):
                if existing.name not in allowed:
                    existing.unlink()
        for rel in allowed_rel_paths:
            src = self.resolve(rel)
            dst = self._managed_path(session, rel)
            if dst.exists():
                continue
            _ensure_dir(dst.parent)
            self._link_or_copy(src, dst)

    @staticmethod
    def _link_or_copy(src: Path, dst: Path) -> None:
        try:
            os.link(src, dst)
        except OSError:
            shutil.copyfile(src, dst)

    # ---- 导出 ----
    def export(self, session: str, checklist: dict, allowed_items: Iterable) -> Path:
        out_dir = _ensure_dir(self.export_root / session)
        items_dir = _ensure_dir(out_dir / "items")
        # 清理已不再允许领取的旧导出
        keep_names = set()
        for item in allowed_items:
            if not item.rel_path:
                continue
            src = self.resolve(item.rel_path)
            name = Path(item.rel_path).name
            keep_names.add(name)
            dst = items_dir / name
            if not dst.exists():
                self._link_or_copy(src, dst)
        for existing in items_dir.glob("*.bin"):
            if existing.name not in keep_names:
                existing.unlink()
        atomic_write_json(out_dir / "checklist.json", checklist)
        return out_dir

    def purge_export(self, session: str) -> None:
        out_dir = self.export_root / session
        if out_dir.exists():
            shutil.rmtree(out_dir)
