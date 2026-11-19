# 机器人示教来源协议

GET /events 是真正HTTP分块SSE，每个通知含id、event、data。传输id不是业务序号，初始每通道只通知1、3、1；缺少序号2可补取。GET /manifest 给出会话、结束声明修订、各通道epoch及闭区间from/to、同步锚点和授权范围。不同设备clock单位不可直接混用，锚点是在session_ms中的已知对应。
GET /backfill?channel=camera&epoch=1&from=2&to=2 返回原件描述；描述有session/channel/epoch/seq和SHA256。GET描述的url取得二进制原件，相机为PPM字节，动作与注记为合成二进制。每个原件均要保留会话归属，摘要相同不表示归属相同。
本地控制grant可携session_ms改变领取范围；本地控制mask屏蔽相机序号2并使下载返回451，manifest返回该范围；conflict令相同通道/代次/序号出现不同摘要；reconnect建立新设备代次并从1重编号；live取消结束声明，end重新结束；unavailable使相机补取503。不同状态的声明修订都会增长。屏蔽后原件的本地存续与清理由使用方自行管理，来源服务不实现客户端。

本地来源控制通过 POST /__control 发送JSON，例如 {"event":"reset"} 可重置样本。控制切换只影响合成来源，不能用来替业务处理方决定批准、签发或确认。来源服务与自动化客户端分别承担各自责任。
