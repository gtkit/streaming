package wssession

import (
	"context"
	"fmt"

	gtkitjson "github.com/gtkit/json"

	"github.com/gorilla/websocket"
)

// PushSink 是业务侧向客户端推帧的唯一入口。
//
// 实现细节(outbox channel + writeLoop)对业务透明;
// 业务只需调 sink.Push(payload) 即可,payload 在 Push 内用 gtkitjson 序列化为文本帧。
//
// 队列满 + QueueOfferTimeout(默认 5s)仍无消费 → 返回 ErrSlowConsumer,
// 业务侧应 return 让 wssession 收敛连接(close 慢客户端)。
type PushSink interface {
	Push(ctx context.Context, payload any) error
}

// pushSink 是 PushSink 的内部实现,持 Session 引用,Push 调 session.queue。
//
// 通过这层间接,Session.queue 是包内私有方法;业务侧只能通过 PushSink 接口推帧,
// 无法直接访问 outbox channel 或 Session 内部字段。
type pushSink struct {
	sess *Session
}

// Push 把 payload 塞进出站队列(JSON 模式)。
//
// 调用方应在 sink.Push 返回 ErrSlowConsumer / ctx.Err 时立即 return Run,
// 由 wssession 收敛连接。
func (s *pushSink) Push(ctx context.Context, payload any) error {
	// 序列化在业务 goroutine 侧完成(可并行),writeLoop 只做纯 IO。
	data, err := gtkitjson.Marshal(payload)
	if err != nil {
		return fmt.Errorf("wssession: marshal push payload: %w", err)
	}
	return s.sess.queue(ctx, outboundMessage{
		messageType: websocket.TextMessage,
		data:        data,
	})
}
