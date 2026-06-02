package wssession

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// inboundFrame 是 readLoop → processLoop 的载体。
//
// 包内私有(业务侧不直接处理 inbound,首帧由 processLoop 调 Handlers.ParseRequest)。
type inboundFrame struct {
	raw        []byte
	receivedAt time.Time
}

// readLoop 是底层唯一 reader。
//
// 职责切分(详见 docs/wsmsg-flow.md §3):
//   - 读 wsConn → 帧类型校验 → 扔进 inbox channel
//   - 维护 read deadline(配合 PongHandler)
//   - 已订阅状态(s.subscribed=true)下收到任何业务帧 → 触发 ctx cancel
//   - **不做**业务解析(JSON parse 由 processLoop 完成)
//
// 这样 readLoop 在 ParseRequest / Run 内做慢操作时仍可继续读 Pong。
func (s *Session) readLoop(ctx context.Context, cancel context.CancelFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			// panic 兜底:转成 error 经 errgroup 上抛(不再静默吞没),
			// 并 cancel 让其余 goroutine 收敛。
			err = fmt.Errorf("wssession: panic in readLoop: %v", p)
			cancel()
		}
	}()

	s.wsConn.SetReadLimit(s.options.ReadLimit)
	if err := s.wsConn.SetReadDeadline(time.Now().Add(s.options.PongWait)); err != nil {
		return err
	}
	s.wsConn.SetPongHandler(func(string) error {
		return s.wsConn.SetReadDeadline(time.Now().Add(s.options.PongWait))
	})

	for {
		msgType, raw, err := s.wsConn.ReadMessage()
		if err != nil {
			// 包括 close 帧 / 网络断 / ReadLimit 超 / deadline 超 → 都让 errgroup 收敛
			return err
		}
		// 收到任何业务帧都续 read deadline(Pong 控制帧由 PongHandler 单独处理,
		// gorilla 内部消化不返回 messageType)
		if err := s.wsConn.SetReadDeadline(time.Now().Add(s.options.PongWait)); err != nil {
			return err
		}

		// 帧类型校验:只接受 TextMessage 业务帧(BinaryMessage 直接拒)
		if msgType != websocket.TextMessage {
			s.closeWithError(ctx, CodeInvalidFrameType, ReasonBinaryFrameUnsupported)
			return ErrInvalidFrame
		}

		// 状态机检查:已订阅后收到任何业务帧 = 协议违规,服务端 close 连接。
		if s.subscribed.Load() {
			s.closeWithError(ctx, CodeInvalidParam, ReasonUnexpectedFrame)
			return ErrUnexpectedFrame
		}

		// 扔进 inbox channel,processLoop 消费
		frame := inboundFrame{raw: raw, receivedAt: time.Now().UTC()}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.inbox <- frame:
			// continue 继续读下一帧(为了在 Subscribed 阶段继续处理 Pong / close 帧)
		}
	}
}
