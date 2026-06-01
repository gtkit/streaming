package wssession

import (
	"context"
	"time"
)

// outboundMessage 是 outbox channel 中的一条待发送帧。
//
// 同时支持两种写出路径,通过 isJSON 区分:
//   - isJSON=true  → writeLoop 调 conn.WriteJSON(jsonPayload)
//   - isJSON=false → writeLoop 调 conn.WriteMessage(messageType, data)
//
// done 字段是可选的同步信号:writeLoop 写出该帧后会 close(done),
// 让需要"等待 flush 完成"的调用方(如 closeWithError 下发 error 帧后才能关连接)
// 同步等待。业务侧 PushSink.Push 不使用 done(异步推帧无需等)。
//
// 仅包内使用;业务通过 PushSink.Push 间接入队(只用 isJSON=true + done=nil 路径)。
type outboundMessage struct {
	messageType int
	data        []byte
	jsonPayload any
	isJSON      bool
	done        chan struct{}
}

// queue 把消息塞进 outbox channel,实现有界队列 + 反压超时。
//
// 反压三段式:
//  1. 立即非阻塞 send(channel 有空位则纳秒返回)
//  2. ctx done / channel send 都阻塞时,启 QueueOfferTimeout 定时器
//  3. 任一信号到达即返回(ctx.Err / nil / ErrSlowConsumer)
//
// 调用方:
//   - pushSink.Push(JSON 业务帧)
//   - Session.closeWithError(error 帧)
//   - Session.queueSubscribed(订阅确认帧)
func (s *Session) queue(ctx context.Context, msg outboundMessage) error {
	return s.queueWithTimeout(ctx, msg, s.options.QueueOfferTimeout)
}

func (s *Session) queueWithTimeout(ctx context.Context, msg outboundMessage, timeout time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outbox <- msg:
		return nil
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outbox <- msg:
		return nil
	case <-timer.C:
		return ErrSlowConsumer
	}
}
