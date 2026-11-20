"""采集端命令行入口。

用法：
  python -m collector --request inputs/request.json run \
      --source http://127.0.0.1:8765 --data-root var/data \
      --managed-root var/managed --export-root var/exports \
      --port 8770 [--interval 1.0]
  python -m collector --request inputs/request.json once ...   # 单次采集后退出
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from collector.api import build_server
from collector.runner import CollectorRunner


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="多通道机器人示教采集端")
    parser.add_argument("--request", required=True, help="研究请求 JSON（授权动作类型与 session_ms 窗口）")
    parser.add_argument("--source", default="http://127.0.0.1:8765", help="合成示教来源地址")
    parser.add_argument("--data-root", default="var/data", help="原始原件与状态根目录")
    parser.add_argument("--managed-root", default="var/managed", help="受管理副本根目录")
    parser.add_argument("--export-root", default="var/exports", help="领取导出根目录")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8770)
    parser.add_argument("--interval", type=float, default=1.0, help="补取/评估轮询间隔秒")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="持续实时接收 + 周期补取")
    sub.add_parser("once", help="单次轮询补取评估后退出（脚本/测试场景）")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    request_path = Path(args.request)
    request_doc = json.loads(request_path.read_text(encoding="utf-8"))
    # 支持 inputs/request.json 的嵌套结构，也接受扁平 request 段
    request = request_doc.get("request", request_doc)

    runner = CollectorRunner(
        base_url=args.source,
        request=request,
        data_root=Path(args.data_root),
        managed_root=Path(args.managed_root),
        export_root=Path(args.export_root),
        interval=args.interval,
    )

    if args.command == "once":
        runner._poll_once()
        for engine in runner.engines.values():
            checklist = engine.status()
            print(json.dumps(checklist, ensure_ascii=False, indent=2))
        return 0

    runner.start()
    httpd = build_server(runner, args.host, args.port)
    print(f"采集端 API http://{args.host}:{httpd.server_port}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()
        runner.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
