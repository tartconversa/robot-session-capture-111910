"""采集运行器：实时 SSE 接收线程与按范围补取/评估线程。

- 实时线程消费真实 chunked SSE；连接结束或中断后按退避重连，通知幂等。
- 补取线程周期性轮询 manifest（捕捉控制端的结束修订/屏蔽/授权/重连变更），
  对每个未停止会话的每个 manifest 通道做闭区间补取，然后重新评估并导出。
- 停止领取的会话冻结，不再吸收通知、不再补取。
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

from collector.session import SessionEngine
from collector.source_client import SourceClient
from collector.storage import Storage


class CollectorRunner:
    def __init__(self, base_url: str, request: dict, data_root: Path,
                 managed_root: Path, export_root: Path, interval: float = 1.0):
        self.client = SourceClient(base_url)
        self.storage = Storage(Path(data_root), Path(managed_root), Path(export_root))
        self.request = request
        self.interval = interval
        self.engines: dict[str, SessionEngine] = {}
        self._engines_lock = threading.RLock()
        self._stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    def engine(self, session: str) -> SessionEngine:
        with self._engines_lock:
            if session not in self.engines:
                session_storage = Storage(
                    self.storage.root / session,
                    self.storage.managed_root,
                    self.storage.export_root,
                )
                self.engines[session] = SessionEngine(session, self.request, session_storage)
            return self.engines[session]

    def start(self) -> None:
        self.threads = [
            threading.Thread(target=self._stream_loop, name="sse", daemon=True),
            threading.Thread(target=self._backfill_loop, name="backfill", daemon=True),
        ]
        for t in self.threads:
            t.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        for t in self.threads:
            t.join(timeout=5)

    # ---------- 实时接收 ----------
    def _stream_loop(self) -> None:
        backoff = 0.2
        while not self._stop_event.is_set():
            try:
                for event in self.client.stream_events():
                    if self._stop_event.is_set():
                        break
                    self._dispatch(event)
                backoff = 0.2
            except Exception as exc:  # 网络层异常：记录后重连，不丢已核实数据
                self._note_error(str(exc))
            # 服务端 SSE 为有限流，正常结束也要轮询式重连以接收后续修订
            self._stop_event.wait(min(backoff, 2.0))
            backoff = min(backoff * 2, 2.0)

    def _dispatch(self, event: dict) -> None:
        data = event.get("data")
        if not isinstance(data, dict):
            return
        if event.get("event") == "manifest" or "channels" in data and "session" in data:
            engine = self.engine(data["session"])
            if not engine.state.stopped:
                engine.apply_manifest(data)
            return
        if event.get("event") == "fragment" or "sha256" in data:
            engine = self.engine(data["session"])
            if not engine.state.stopped:
                engine.apply_fragment(data)

    def _note_error(self, message: str) -> None:
        with self._engines_lock:
            for engine in self.engines.values():
                engine.state.last_source_error = message

    # ---------- 按范围补取与评估 ----------
    def _backfill_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll_once()
            except Exception as exc:
                self._note_error(str(exc))
            self._stop_event.wait(self.interval)

    def _poll_once(self) -> None:
        try:
            manifest = self.client.get_manifest()
        except Exception as exc:
            self._note_error(str(exc))
            return
        engine = self.engine(manifest["session"])
        if engine.state.stopped:
            return
        engine.apply_manifest(manifest)
        for channel in list(engine.state.channels):
            engine.backfill_channel(self.client, channel)
        engine.evaluate()
