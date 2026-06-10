package wssession

import (
	"context"
	"fmt"

	gtkitjson "github.com/gtkit/json"

	"github.com/gorilla/websocket"
)

// PushSink 是业务侧向客户端推帧的入口。
//
// 实现细节(outbox channel + writeLoop)对业务透明;
// 业务只需调 sink.Push(payload) 即可,payload 在 Push 内用 gtkitjson 序列化为文本帧。
//
// 队列满 + QueueOfferTimeout(默认 5s)仍无消费 → 返回 ErrSlowConsumer,
// 业务侧应 return 让 wssession 收敛连接(close 慢客户端)。
//
// *Session 实现本接口:Run / OnMessage 收到的 sink 即所属 Session;
// 持有 *Session(OnConnect 注入)的代码也可直接 Push(如 sessionhub 定向推送)。
type PushSink interface {
	Push(ctx context.Context, payload any) error
}

// Push 把 payload 序列化后塞进该连接的出站队列(实现 PushSink)。
//
// 并发安全,可与 Run / OnMessage 的推送并存(出帧由 writeLoop 串行写出,
// 帧序由入队时刻决定)。返回 ErrSlowConsumer / ctx.Err 时调用方应停止推送。
func (s *Session) Push(ctx context.Context, payload any) error {
	// 序列化在业务 goroutine 侧完成(可并行),writeLoop 只做纯 IO。
	data, err := gtkitjson.Marshal(payload)
	if err != nil {
		return fmt.Errorf("wssession: marshal push payload: %w", err)
	}
	return s.queue(ctx, outboundMessage{
		messageType: websocket.TextMessage,
		data:        data,
	})
}

// Kick 把该连接踢下线:下发 error(409, reason) 帧并以 close 1008 完成关闭
// 握手后收敛连接。用于单点登录顶号、管理端强制下线等场景
// (典型配合 sessionhub:遍历 Conns(userID) 逐个 Kick)。
//
// 幂等:与其它错误关闭路径共享同一幂等域,重复调用只下发首帧。
// 被踢连接的 Serve 会以预期 close 错误返回,调用方照常忽略即可。
// 客户端契约:收到 error(409) 应提示被顶下线且不自动重连。
func (s *Session) Kick(ctx context.Context, reason string) {
	s.closeWithError(ctx, CodeConflict, reason)
}
