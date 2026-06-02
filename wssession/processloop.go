package wssession

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// processLoop 是首帧编排 + Run 调度。
//
// 流程(详见 docs/wsmsg-flow.md §1 主路径):
//
//	① 启动 firstFrameTimer
//	② 等 inbox 首帧(超时 / 收到帧 / ctx done 三选一)
//	③ Handlers.ParseRequest → (key, req, err) → 失败下发 error(422) + close
//	④ tokenCap.tryAcquire(key)            → 满下发 error(429) + close
//	⑤ subscribed.Store(true) + 下发 subscribed 帧
//	⑥ Handlers.Run(ctx, req, sink) 同步调用
//	⑦ Run 返回值分发:nil → 正常关闭;ErrSlowConsumer → error(429);其他 → error(500)
//
// Run 在本 goroutine 内**同步**调用——这是有意为之:
//   - Run 自身是 blocking 业务循环(snapshot / poll / push),不是消息回调
//   - readLoop 已经独立 goroutine,不被 Run 阻塞
//   - 单 Session 内不需要并发处理多条 inbound 帧(协议约定"一连接一订阅")
func (s *Session) processLoop(ctx context.Context, cancel context.CancelFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("wssession: panic in processLoop: %v", p)
			s.options.emit(ctx, Event{Type: EventPanic, Reason: "panic in processLoop", Err: err})
			cancel()
		}
	}()

	// ① 首帧超时 timer:到时**抢占** claim,成功才下发 408 + close。
	// 与下面"首帧到达"路径用同一个 firstFrameClaimed CAS 互斥,避免竞态。
	firstFrameTimer := time.AfterFunc(s.options.FirstFrameTimeout, func() {
		if s.firstFrameClaimed.CompareAndSwap(false, true) {
			s.closeWithError(ctx, CodeFirstFrameTimeout, ReasonFirstFrameTimeout)
		}
	})

	// ② 等首帧
	var firstFrame inboundFrame
	select {
	case <-ctx.Done():
		firstFrameTimer.Stop()
		return ctx.Err()
	case firstFrame = <-s.inbox:
		firstFrameTimer.Stop()
		// 抢占 claim:抢不到说明超时回调已先动连接,直接退出。
		if !s.firstFrameClaimed.CompareAndSwap(false, true) {
			return ErrFirstFrameTimeout
		}
	}

	// ③ Handlers.ParseRequest
	key, req, parseErr := s.handlers.ParseRequest(ctx, firstFrame.raw)
	if parseErr != nil {
		s.closeWithError(ctx, CodeInvalidParam, parseErr.Error())
		return parseErr
	}

	// ④ token 维度 connCap(key 为空时跳过,让 handler 业务自己拒)
	var tokenCapKey string
	if key != "" && s.options.ConnCapEnabled {
		tokenCapKey = "token:" + key + ":" + s.path
		_, ok := tryAcquire(tokenCapKey, s.options.ConnCapKeyMax)
		if !ok {
			s.options.emit(ctx, Event{Type: EventCapRejected, Reason: ReasonTooManyTokenConn, Key: tokenCapKey})
			s.closeWithError(ctx, CodeTooManyConn, ReasonTooManyTokenConn)
			return errors.New("wssession: token connCap exceeded")
		}
		// 用 defer 在 processLoop 退出时释放,保证一一对应 tryAcquire
		defer release(tokenCapKey)
	}

	// ⑤ 状态机切到 Subscribed + 下发订阅确认帧
	s.subscribed.Store(true)
	subscribedAt := time.Now().UTC()
	if err := s.queue(ctx, outboundMessage{
		isJSON: true,
		jsonPayload: subscribedFrame{
			Event:     "subscribed",
			Timestamp: subscribedAt.Format(time.RFC3339Nano),
		},
	}); err != nil {
		// subscribed 帧入队失败(反压 / ctx done)→ 直接结束,不再尝试 Run
		return err
	}

	// ⑥ Handlers.Run — 同步 blocking 业务循环
	sink := &pushSink{sess: s}
	runErr := s.handlers.Run(ctx, req, sink)

	// ⑦ Run 返回值分发
	switch {
	case runErr == nil:
		// 正常结束:走 closeNormal 路径(由 Session.Serve 的 cleanup 处理)
		return nil
	case errors.Is(runErr, ErrSlowConsumer):
		s.options.emit(ctx, Event{Type: EventSlowConsumer, Reason: ReasonSlowConsumer, Err: runErr})
		s.closeWithError(ctx, CodeTooManyConn, ReasonSlowConsumer)
		return runErr
	case errors.Is(runErr, context.Canceled), errors.Is(runErr, context.DeadlineExceeded):
		// ctx 取消是预期路径(客户端断 / 30 分钟超时),不下发 error 帧
		return runErr
	default:
		// 未预期的业务循环错误统一按内部错误处理,客户端只收到稳定 reason。
		// 错误通过 Serve 返回值上抛给调用方,由调用方决定是否记录日志(本包不绑定日志栈)。
		s.closeWithError(ctx, CodeInternal, ReasonInternalError)
		return runErr
	}
}
