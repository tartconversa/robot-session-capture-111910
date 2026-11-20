# 示教采集端设计说明

采集端对接本地合成示教服务，负责多通道（action / camera / annotation）原件的实时接收、
按范围补取、完整性核实与授权领取。**本端不训练模型，也不对缺帧做插值或补造**——
任何无法核实的范围都如实记录为缺口。

## 1. 身份模型与归属

原件身份是四元组 `(session, channel, epoch, seq)`，落盘路径即按该四元组分层：

```
var/data/<session>/raw/<channel>/epoch<n>/seq<k>.<sha256前12位>.bin
```

- 摘要相同但身份不同（如 reconnect 后新代次从 1 重编号）分别落盘，**绝不按字节去重**；
- 同一身份收到不同摘要：标记 `conflict`，已落盘字节移入 `quarantine/`，
  不以后到者覆盖先到者，冲突项不进入受管理副本或导出；
- 实时通知允许重复：相同 (身份, 摘要) 幂等吸收。

## 2. 实时接收与补取

- `SourceClient.stream_events()` 直接消费真实 chunked SSE，逐行解析 `id/event/data`；
  传输 `id` 仅用于 SSE 协议，不作为业务序号。
- SSE 每通道只通知序号 1、3、1，缺失序号由 `backfill_channel()` 按 manifest
  闭区间 `[from,to]` 调 `/backfill` 核实；补取响应仍缺段 → `source_gap`，不编造。
- 运行器含两个守护线程：SSE 接收线程（断连退避重连）与补取/评估线程
  （周期拉 `/manifest`，捕捉结束修订、屏蔽、授权变更与重连）。
- 下载原件同时核对描述摘要与 `X-Content-SHA256` 响应头；不符即拒收。
- 503 → 通道欠采；451 → 屏蔽墓碑；409（旧代次）→ 按 `source_gap` 处理，
  旧代次序号绝不并入新代次序号空间。

## 3. 同步锚点与授权

不同设备 clock 单位不同，**设备 clock 字段只随描述存档，不参与任何跨通道换算**。
边界映射规则（`collector/anchors.py`）：

- 仅在同一通道相邻两个锚点之间做线性对应，锚点本身直接命中；
- 序号超出锚点跨度或锚点不足 → `unmapped`，保留缺口，不外推；
- 授权判定 = 来源 `grant.session_ms` 与研究请求 `session_ms` 两个闭区间取交集；
- 动作通道额外要求 `action_type` 同时在来源授权与请求类型集合内；
  相机/注记随会话，不按动作类型过滤。

## 4. 屏蔽处理

manifest 的 `redactions` 给出 `(channel, epoch, seq区间)`：

- 屏蔽先于数据到达：先立 `redacted` 墓碑，后续任何片段/补取都不得复活；
- 屏蔽覆盖已存帧：删除 raw 原始字节、managed 受管理副本、exports 导出副本三处，
  状态中仅保留无字节墓碑；同会话其他有依据的动作仍可领取；
- 任何日志、状态 JSON、临时文件都不包含原始字节（无拼接文件，`.part` 即时清理）。

## 5. 会话状态机

| overall_status      | 条件 |
| --- | --- |
| in_progress        | 无结束声明（live） |
| complete           | 有结束声明，且 manifest 各通道闭区间每个序号都已核实、无冲突/屏蔽/缺口/unmapped |
| under_collected    | 已结束但存在 source_gap、unmapped 或通道不可达 |
| conflicted         | 已结束但存在冲突隔离项 |
| partially_redacted | 已结束但部分范围被屏蔽（其余仍可领取） |
| stopped            | 人工停止领取：冻结通知与补取，保留全部已核实事实 |

manifest revision 取高：旧修订不回滚新事实；收到新的结束修订或屏蔽通知后重新评估受影响范围。

## 6. 中断恢复

状态原子写入 `state.json`（临时文件 + `os.replace`）。重启时以磁盘事实复核：

- 状态为 verified 但字节缺失（崩溃于写盘前）→ 退回 staged，等待重新补取；
- 状态为 staged 但字节已存在（崩溃于写盘后、状态更新前）→ 恢复 verified。

## 7. 目录与接口

```
var/data/<session>/state.json      会话状态（无原件字节）
var/data/<session>/raw/...         按身份归属的原始原件
var/data/<session>/quarantine/...  冲突隔离字节
var/managed/<session>/             受管理副本（只含当前 allowed 原件，授权收窄即清理）
var/exports/<session>/checklist.json  完整性清单
var/exports/<session>/items/       领取导出原件（硬链接，失败回退复制）
```

采集端 HTTP API（默认 8770）：

- `GET  /health`
- `GET  /sessions`、`GET  /sessions/{id}`（清单含 verified/staged/source_gap/conflict/redacted
  与 allowed/unauthorized/unmapped 两类计数）
- `POST /sessions/{id}/stop` / `/resume` / `/backfill`
- `GET  /sessions/{id}/export`

CLI：

```bash
python -m collector --request inputs/request.json run \
  --source http://127.0.0.1:8765 --data-root var/data \
  --managed-root var/managed --export-root var/exports --port 8770
python -m collector --request inputs/request.json once ...   # 单次采集后退出
```
