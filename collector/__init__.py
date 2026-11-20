"""多通道机器人示教采集端。

只负责真实流式接收、按范围补取、原件校验、授权/屏蔽判定和会话级完整性清单，
不训练模型，也不对缺帧做任何插值或补造。
"""
from collector.model import Claim, ItemStatus, SessionStatus

__all__ = ["Claim", "ItemStatus", "SessionStatus"]
