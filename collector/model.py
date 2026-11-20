"""采集领域模型：状态枚举与可序列化数据结构。"""
from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Optional


class ItemStatus(str, Enum):
    """单个原件 (session, channel, epoch, seq) 的核实状态。"""

    STAGED = "staged"            # 只收到描述，字节尚未核实
    VERIFIED = "verified"        # 字节已下载且 SHA256 与描述一致
    SOURCE_GAP = "source_gap"    # 来源不可达/拒服旧代次/补取仍缺段
    CONFLICT = "conflict"        # 同一身份出现不同摘要，已隔离
    REDACTED = "redacted"        # 被屏蔽通知覆盖，字节已清理，仅存墓碑


class Claim(str, Enum):
    """单个原件相对于研究请求与来源授权的领取判定。"""

    ALLOWED = "allowed"
    UNAUTHORIZED = "unauthorized"   # 动作类型或 session_ms 不在授权/请求范围
    UNMAPPED = "unmapped"           # 锚点无法证明该序号与 session_ms 的对应
    REDACTED = "redacted"
    NOT_VERIFIED = "not_verified"   # 未通过完整性核实，不得领取


class SessionStatus(str, Enum):
    IN_PROGRESS = "in_progress"                # 尚无结束声明
    UNDER_COLLECTED = "under_collected"        # 已结束但存在缺口/无法映射
    CONFLICTED = "conflicted"                  # 已结束但存在冲突隔离
    PARTIALLY_REDACTED = "partially_redacted"  # 已结束但部分范围被屏蔽
    COMPLETE = "complete"                      # 结束声明、通道范围、授权全部核对成立
    STOPPED = "stopped"                        # 人工停止领取，保留底层状态


@dataclasses.dataclass
class Item:
    """一个通道原件的全部已知事实。"""

    channel: str
    epoch: int
    seq: int
    action_type: Optional[str]
    digests: list[str]                     # 观测到的全部摘要，去重保序
    status: str
    rel_path: Optional[str] = None         # raw 区内相对路径；已屏蔽/未下载为 None
    mapped_ms: Optional[float] = None      # 仅由同步锚点推出，不使用设备 clock
    claim: str = Claim.NOT_VERIFIED.value
    gap_reason: Optional[str] = None

    @property
    def identity(self) -> str:
        return f"{self.channel}|{self.epoch}|{self.seq}"

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Item":
        return cls(**d)


@dataclasses.dataclass
class ChannelRange:
    epoch: int
    frm: int
    to: int

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


def item_key(channel: str, epoch: int, seq: int) -> str:
    return f"{channel}|{epoch}|{seq}"
