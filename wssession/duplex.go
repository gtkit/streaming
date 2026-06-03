package wssession

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// turn 表示双向模式下一轮 OnMessage 的执行句柄。
type turn struct {
	cancel context.CancelFunc
	done   chan struct{} // OnMessage goroutine 退出时关闭
}

// duplexLoop 是双向模式的消息调度循环(详见 design.md D-3/D-4)。
//
// 职责:
//   - 每条入站业务帧派生一个可 cancel 的 turnCtx + goroutine 跑 OnMessage;
//   - 新消息到达时,若上一轮仍在运行则 cancel 它(打断)并上报 EventTurnInterrupted,
//     再开启新一轮——同一连接同时至多一个活跃 turn;
//   - 入站限速:超额消息丢弃 + 下发 error(429) + 上报 EventRateLimited,不关连接;
//   - 收敛:ctx 取消时 cancel 活跃 turn 并 wg.Wait 等所有 turn 退出,杜绝 goroutine 泄漏。
//
// 首帧已被 ParseRequest 当订阅/鉴权帧消费;duplexLoop 处理其后的每条消息。
// 双向模式下 Run 可选:若提供,在后台 goroutine 并行运行(用于主动推送),其异常收敛整连接。
func (s *Session) duplexLoop(ctx context.Context, cancel context.CancelFunc, req any, sink PushSink) error {
	limiter := newRateLimiter(s.options.InboundRatePerSecond, s.options.InboundRateBurst)

	var wg sync.WaitGroup
	var active *turn

	// 收敛:打断活跃 turn + 等所有 turn(含后台 Run)退出
	defer func() {
		if active != nil {
			active.cancel()
		}
		wg.Wait()
	}()

	// 双向模式下 Run 可选:后台并行跑主动推送循环,异常则收敛连接。
	if s.handlers.Run != nil {
		wg.Go(func() {
			defer func() {
				if p := recover(); p != nil {
					s.options.emit(ctx, Event{Type: EventPanic, Reason: "panic in background Run", Err: fmt.Errorf("%v", p)})
					cancel()
				}
			}()
			if err := s.handlers.Run(ctx, req, sink); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				cancel() // 后台 Run 异常 → 收敛连接
			}
		})
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame := <-s.inbox:
			if !limiter.allow() {
				s.emitRateLimited(ctx)
				continue
			}
			// 打断仍在运行的上一轮(已自然结束的不算打断)
			if active != nil {
				select {
				case <-active.done:
				default:
					active.cancel()
					s.options.emit(ctx, Event{Type: EventTurnInterrupted, Reason: "interrupted by new message"})
				}
			}
			active = s.startTurn(ctx, cancel, &wg, frame.raw, sink)
		}
	}
}

// startTurn 为一条消息派生 turnCtx 并起 goroutine 运行 OnMessage。
func (s *Session) startTurn(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, raw []byte, sink PushSink) *turn {
	turnCtx, turnCancel := context.WithCancel(ctx)
	t := &turn{cancel: turnCancel, done: make(chan struct{})}

	wg.Go(func() {
		defer close(t.done)
		defer turnCancel() // 释放 turnCtx,避免 context 泄漏
		defer func() {
			if p := recover(); p != nil {
				s.options.emit(turnCtx, Event{Type: EventPanic, Reason: "panic in OnMessage", Err: fmt.Errorf("%v", p)})
				s.closeWithError(turnCtx, CodeInternal, ReasonInternalError)
				cancel()
			}
		}()

		// 一轮 OnMessage 的返回值处置:
		//   nil 保持连接;ErrSlowConsumer→429+close;ctx 取消(被打断/超时)静默;其它→500+close。
		err := s.handlers.OnMessage(turnCtx, raw, sink)
		if err == nil ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if errors.Is(err, ErrSlowConsumer) {
			s.options.emit(turnCtx, Event{Type: EventSlowConsumer, Reason: ReasonSlowConsumer, Err: err})
			s.closeWithError(turnCtx, CodeTooManyConn, ReasonSlowConsumer)
		} else {
			s.closeWithError(turnCtx, CodeInternal, ReasonInternalError)
		}
		cancel() // 业务错误 → 收敛整连接
	})

	return t
}

// emitRateLimited 上报限速事件并向客户端下发一帧 error(429),不关闭连接。
func (s *Session) emitRateLimited(ctx context.Context) {
	s.options.emit(ctx, Event{Type: EventRateLimited, Reason: "inbound rate limited"})
	_ = s.queue(ctx, outboundMessage{
		isJSON: true,
		jsonPayload: errorFrame{
			Event:     "error",
			Code:      CodeTooManyConn,
			Reason:    "rate limited",
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		},
	})
}
