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
//     **等其 goroutine 退出后**再开启新一轮——同一连接任一时刻严格至多一个
//     OnMessage 在运行,被打断的旧轮不会在新轮启动后继续向 sink 推过期帧;
//   - 入站限速:超额消息丢弃 + 上报 EventRateLimited,同一连续限速期只下发
//     一帧 error(429) 提示,不关连接;
//   - 收敛:ctx 取消时 cancel 活跃 turn 并等待退出;失约 turn 超时后上报并收敛连接。
//
// 首帧已被 ParseRequest 当订阅/鉴权帧消费;duplexLoop 处理其后的每条消息。
// 双向模式下 Run 可选:若提供,在后台 goroutine 并行运行(用于主动推送),
// 其错误处置与单向模式一致(dispatchRunError)。
func (s *Session) duplexLoop(ctx context.Context, cancel context.CancelFunc, req any, sink PushSink) error {
	limiter := newRateLimiter(s.options.InboundRatePerSecond, s.options.InboundRateBurst)

	var wg sync.WaitGroup
	var active *turn
	// limitedNotified 标记当前连续限速期内是否已下发过 429 提示帧,
	// 任一消息重新通过限速即复位——避免限速风暴下重复出帧。
	limitedNotified := false

	// 收敛:打断活跃 turn + 等所有 turn(含后台 Run)退出。
	defer func() {
		if active != nil {
			active.cancel()
			if !s.waitTurnDone(ctx, active, "turn stuck during connection close") {
				cancel()
				return
			}
		}
		wg.Wait()
	}()

	// 双向模式下 Run 可选:后台并行跑主动推送循环。
	// 错误处置复用 dispatchRunError(事件 + error 帧 + close),与单向模式一致;
	// 返回 nil 表示推送循环自然结束,连接保持(对话由 OnMessage 继续)。
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
				_ = s.dispatchRunError(ctx, err)
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
				s.options.emit(ctx, Event{Type: EventRateLimited, Reason: "inbound rate limited"})
				if !limitedNotified {
					limitedNotified = true
					s.offerRateLimitedFrame()
				}
				continue
			}
			limitedNotified = false
			// 打断仍在运行的上一轮(已自然结束的不算打断),并等其退出:
			// 守约的 OnMessage(契约要求监听 turnCtx)毫秒级返回;失约的会经
			// inbox→readLoop 反压,最终由 PongWait 终结连接。
			if active != nil {
				select {
				case <-active.done:
				default:
					active.cancel()
					s.options.emit(ctx, Event{Type: EventTurnInterrupted, Reason: "interrupted by new message"})
					if !s.waitTurnDone(ctx, active, "turn stuck after interrupt") {
						s.closeWithError(ctx, CodeInternal, ReasonInternalError)
						cancel()
						return errTurnStuck
					}
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
		err := s.handlers.OnMessage(turnCtx, raw, sink)
		switch {
		case turnCtx.Err() != nil:
			// 被新消息打断 / 会话结束(turnCtx 已取消):视为预期,不关连接——
			// 无论业务是否如约把 ctx 取消传播为返回值,都不误杀整条连接。
		case err == nil:
			// 该轮正常结束,连接保持
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// 业务自身 ctx 取消:预期,静默
		case errors.Is(err, ErrSlowConsumer):
			s.options.emit(turnCtx, Event{Type: EventSlowConsumer, Reason: ReasonSlowConsumer, Err: err})
			s.closeWithError(turnCtx, CodeTooManyConn, ReasonSlowConsumer)
			cancel() // 慢消费者 → 收敛整连接
		default:
			s.closeWithError(turnCtx, CodeInternal, ReasonInternalError)
			cancel() // 业务错误 → 收敛整连接
		}
	})

	return t
}

func (s *Session) waitTurnDone(ctx context.Context, active *turn, reason string) bool {
	timer := time.NewTimer(s.options.TurnCloseTimeout)
	defer timer.Stop()

	select {
	case <-active.done:
		return true
	case <-timer.C:
		ev := Event{Type: EventTurnStuck, Reason: reason}
		if err := ctx.Err(); err != nil {
			ev.Err = err
		}
		s.options.emit(ctx, ev)
		return false
	}
}

// offerRateLimitedFrame 向客户端**非阻塞**下发一帧 error(429) 限速提示,不关闭连接。
//
// 用非阻塞 send:限速恰是高频刷消息时触发,若用阻塞的 queue,满了会卡住调度循环
// 最多 QueueOfferTimeout——那等于让攻击者拖慢正常处理。outbox 满则丢弃这帧提示。
// 调用频率由 duplexLoop 的 limitedNotified 控制:同一连续限速期只发一帧。
func (s *Session) offerRateLimitedFrame() {
	frame := jsonFrame(errorFrame{
		Event:     "error",
		Code:      CodeTooManyConn,
		Reason:    "rate limited",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	select {
	case s.outbox <- frame:
	default: // outbox 满 → 丢弃限速提示,绝不阻塞调度循环
	}
}
