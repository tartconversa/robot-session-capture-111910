"""同步锚点映射。

边界只能在同一通道相邻锚点之间做线性对应，锚点覆盖不到或缺少两个锚点时
拒绝映射（返回 None），由调用方保留缺口。设备 clock 字段仅随描述存档，
绝不参与跨通道换算，也不存在统一时钟。
"""
from __future__ import annotations

from typing import Optional


def _anchor_seq(anchor: dict, channel: str) -> Optional[int]:
    key = f"{channel}_seq"
    value = anchor.get(key)
    return int(value) if value is not None else None


def build_channel_anchors(anchors: list[dict], channel: str) -> list[tuple[int, float]]:
    """抽取某通道的 (seq, session_ms) 锚点并按序号排序。"""
    rows = []
    for anchor in anchors:
        seq = _anchor_seq(anchor, channel)
        if seq is None or anchor.get("session_ms") is None:
            continue
        rows.append((seq, float(anchor["session_ms"])))
    rows.sort(key=lambda r: r[0])
    return rows


def seq_to_session_ms(anchors: list[dict], channel: str, seq: int) -> Optional[float]:
    """把通道序号对应到 session_ms。

    锚点本身直接命中；序号落在两个锚点之间才做线性对应；
    超出锚点跨度或锚点不足时返回 None（保留缺口，不外推）。
    """
    rows = build_channel_anchors(anchors, channel)
    if not rows:
        return None
    for anchor_seq, ms in rows:
        if seq == anchor_seq:
            return ms
    for (s0, t0), (s1, t1) in zip(rows, rows[1:]):
        if s0 < seq < s1:
            ratio = (seq - s0) / (s1 - s0)
            return t0 + ratio * (t1 - t0)
    return None


def in_grant_window(ms: Optional[float], grant_ms: Optional[list[float]]) -> bool:
    """闭区间授权窗口判定；无法映射（ms 为 None）一律不授予。"""
    if ms is None or not grant_ms or len(grant_ms) != 2:
        return False
    lo, hi = float(grant_ms[0]), float(grant_ms[1])
    return lo <= ms <= hi
