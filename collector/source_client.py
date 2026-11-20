"""来源服务 HTTP 客户端：真实 SSE 分块流、manifest、按范围补取、二进制原件。

只用标准库；SSE 直接读取 chunked 响应行，按空行切分事件。
传输 id 不作为业务序号使用。
"""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterator, Optional

from source.common import digest


class SourceError(Exception):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class SourceUnavailable(SourceError):
    """503 等通道不可达：属于来源缺段，不是客户端失败。"""


class SourceRedacted(SourceError):
    """451：原件已被屏蔽。"""


class SourceStaleEpoch(SourceError):
    """409：代次已轮换，旧代次请求必须按缺口处理。"""


class SourceClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _open(self, path: str):
        req = urllib.request.Request(self.base_url + path)
        try:
            return urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            body = exc.read()
            if exc.code == 503:
                raise SourceUnavailable("channel_unavailable", exc.code) from exc
            if exc.code == 451:
                raise SourceRedacted("redacted", exc.code) from exc
            if exc.code == 409:
                raise SourceStaleEpoch("stale_epoch", exc.code) from exc
            raise SourceError(body.decode("utf-8", "replace"), exc.code) from exc

    def get_manifest(self) -> dict:
        with self._open("/manifest") as resp:
            return json.loads(resp.read().decode("utf-8"))

    def backfill(self, channel: str, epoch: int, frm: int, to: int) -> list[dict]:
        query = urllib.parse.urlencode(
            {"channel": channel, "epoch": epoch, "from": frm, "to": to}
        )
        with self._open("/backfill?" + query) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return payload.get("items", [])

    def download_original(self, descriptor: dict) -> bytes:
        """按描述 url 取回二进制原件，并核对 X-Content-SHA256/重算摘要。"""
        url = descriptor["url"]
        if url.startswith("/"):
            url = self.base_url + url
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                data = resp.read()
                header_digest = resp.headers.get("X-Content-SHA256")
        except urllib.error.HTTPError as exc:
            if exc.code == 503:
                raise SourceUnavailable("channel_unavailable", exc.code) from exc
            if exc.code == 451:
                raise SourceRedacted("redacted", exc.code) from exc
            if exc.code == 409:
                raise SourceStaleEpoch("stale_epoch", exc.code) from exc
            raise
        if header_digest and header_digest != digest(data):
            raise SourceError("header digest mismatch")
        if digest(data) != descriptor["sha256"]:
            raise SourceError("descriptor digest mismatch")
        return data

    def stream_events(self) -> Iterator[dict]:
        """消费真实 chunked SSE，逐条产出 {'event':..., 'data':...}。

        连接结束即正常返回，由上层决定是否重连；网络异常向上抛出。
        只依赖 readline 逐行读取，服务端 flush 后即可收到，不做整段缓冲。
        """
        with self._open("/events") as resp:
            event = "message"
            data_lines: list[str] = []
            for raw_line in resp:
                line = raw_line.decode("utf-8").rstrip("\r\n")
                if line == "":
                    if data_lines:
                        payload = "\n".join(data_lines)
                        try:
                            yield {"event": event, "data": json.loads(payload)}
                        except json.JSONDecodeError:
                            yield {"event": event, "data": None}
                    event, data_lines = "message", []
                    continue
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
                # id: 行刻意忽略：传输 id 不是业务序号
