"""采集端本地 HTTP API。

GET  /health                 存活与运行状态
GET  /sessions               全部会话状态清单
GET  /sessions/{id}          单个会话完整性清单（含来源缺段/冲突/未授权/已屏蔽分类）
POST /sessions/{id}/stop     停止领取（冻结通知与补取，保留已核实数据）
POST /sessions/{id}/resume   恢复领取
POST /sessions/{id}/backfill 立即按 manifest 范围补取并重新评估
GET  /sessions/{id}/export   导出目录与清单位置
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from collector.runner import CollectorRunner


class ApiHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    runner: CollectorRunner = None

    def log_message(self, *args):
        pass

    def _send(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/health":
            return self._send({"status": "ok", "kind": "teaching-collector"})
        if path == "/sessions":
            return self._send({"sessions": [e.status() for e in self.runner.engines.values()]})
        parts = [p for p in path.split("/") if p]
        if len(parts) == 2 and parts[0] == "sessions":
            return self._session(parts[1], lambda e: e.status())
        if len(parts) == 3 and parts[0] == "sessions" and parts[2] == "export":
            def export_info(e):
                root = self.runner.storage.export_root / e.session
                return {
                    "session": e.session,
                    "export_dir": str(root),
                    "checklist": str(root / "checklist.json"),
                    "items_dir": str(root / "items"),
                }
            return self._session(parts[1], export_info)
        self._send({"error": "not_found"}, 404)

    def do_POST(self):
        path = urlsplit(self.path).path
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[0] == "sessions":
            session, action = parts[1], parts[2]
            if action == "stop":
                return self._session(session, lambda e: (e.stop(), e.status())[1])
            if action == "resume":
                return self._session(session, lambda e: (e.resume(), e.status())[1])
            if action == "backfill":
                def do_backfill(e):
                    for channel in list(e.state.channels):
                        e.backfill_channel(self.runner.client, channel)
                    return e.evaluate()
                return self._session(session, do_backfill)
        self._send({"error": "not_found"}, 404)

    def _session(self, session_id, fn):
        engine = self.runner.engines.get(session_id)
        if engine is None:
            return self._send({"error": "unknown_session", "session": session_id}, 404)
        try:
            self._send(fn(engine))
        except Exception as exc:  # API 不回传任何原件字节
            self._send({"error": "processing_failed", "detail": str(exc)}, 500)


def build_server(runner: CollectorRunner, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    ApiHandler.runner = runner
    return ThreadingHTTPServer((host, port), ApiHandler)
