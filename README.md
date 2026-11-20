# 机器人示教来源与采集端

这是本地生成的外部来源与独立接收端，仅用于提供数据、页面、协议与回执。仓库提供合成来源服务与多通道采集客户端。所有单位、厂商、研究记录和门户内容均为虚构数据，不连接真实业务系统。

## 启动

安装Docker后，在项目根目录运行 docker compose up --build，来源服务默认监听 http://127.0.0.1:8765，采集端 API 监听 http://127.0.0.1:8770。若直接在主机运行，Python 3.11+执行 pip install -r requirements.txt，再运行 python -m source.app --port 8765。浏览器任务通过Playwright 1.56.0提供的Chromium测试来源页面。

输入示例位于 inputs/request.json；业务字段、页面与协议边界见 docs/source-contract.md，采集端设计见 docs/collector-design.md。示例是项目可读输入格式，原件与来源修订均由演练服务产生。

## 采集端

采集端只做真实流式接收、按范围补取、SHA256 原件核实、锚点授权判定和会话完整性清单，不训练模型、不插值补造。启动：

```
python -m collector --request inputs/request.json run \
  --source http://127.0.0.1:8765 --data-root var/data \
  --managed-root var/managed --export-root var/exports --port 8770
```

实时接收 /events 的 chunked SSE，按 /manifest 闭区间经 /backfill 补取缺段；同序号不同摘要隔离冲突，屏蔽通知覆盖已存帧时清理 raw/managed/exports 三处字节并保留墓碑；授权边界只通过同步锚点对应到各通道序号，无法对应则保留 unmapped 缺口。状态查询、停止/恢复领取与手动补取见采集端 HTTP API（docs/collector-design.md 第 7 节）。

## 来源说明

服务页面 / 简述当前来源。JSON与XML接口描述输入契约，页面展示来源交互；POST /__control 只用于本地演练人员切换来源修订、可用性和延迟场景。状态可用 --state /path/outside/source-state.json 选择性落盘，默认每次启动恢复初始样本。端点行为和合成记录见 source/app.py。

## 检查

运行 python -m unittest discover -s tests -v 检查来源端、原件、门户状态、合成接收端以及采集端全部场景（真实流式传输、二进制原件、缺段补取、冲突隔离、屏蔽清理、锚点授权、停止领取与中断恢复）。涉及页面的数据源测试会启动真实Chromium。
