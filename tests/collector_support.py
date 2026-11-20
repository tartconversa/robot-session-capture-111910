"""采集端测试夹具：在真实合成来源上启动采集引擎。"""
import json
import tempfile
import time
import urllib.request
from pathlib import Path

from tests.support import SourceTest


class CollectorTest(SourceTest):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.data_root, self.managed_root, self.export_root = (
            root / "data", root / "managed", root / "exports",
        )
        # 注意：父类已有 HTTP 帮助方法 self.request，此处命名为 research_request
        self.research_request = {"action_types": ["pick"], "session_ms": [0, 2000]}

    def make_runner(self, interval=0.05):
        from collector.runner import CollectorRunner
        runner = CollectorRunner(
            self.base, self.research_request, self.data_root,
            self.managed_root, self.export_root, interval=interval,
        )
        self.addCleanup(runner.shutdown)
        return runner

    def make_engine(self, session="demo-A"):
        from collector.session import SessionEngine
        from collector.storage import Storage
        storage = Storage(self.data_root / session, self.managed_root, self.export_root)
        return SessionEngine(session, self.research_request, storage)

    def wait_until(self, predicate, timeout=8.0):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = predicate()
            if last:
                return last
            time.sleep(0.05)
        raise AssertionError(f"condition not met; last={last!r}")

    def drain_stream(self):
        """真实读取一遍 chunked SSE（验证流式传输本身）。"""
        with urllib.request.urlopen(self.base + "/events", timeout=5) as resp:
            self.assertEqual(resp.headers["Transfer-Encoding"], "chunked")
            return resp.read()

    def poll(self, runner, times=2):
        for _ in range(times):
            runner._poll_once()

    def find(self, checklist, channel, epoch, seq):
        for item in checklist["items"]:
            if (item["channel"], item["epoch"], item["seq"]) == (channel, epoch, seq):
                return item
        raise KeyError(f"{channel} epoch{epoch} seq{seq}")

    def all_blobs_under(self, path: Path):
        return list(Path(path).rglob("*.bin")) if path.exists() else []

    def api_get(self, api_port, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{api_port}{path}", timeout=5) as resp:
            return json.loads(resp.read())
