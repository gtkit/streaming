package wssession

import (
	"context"
	"time"

	"github.com/gorilla/websocket"
)

// writeLoop 是底层唯一 writer。
//
// 所有出帧(业务推送、订阅确认、error 帧、Ping 心跳)都从这个 goroutine 串行写出,
// 满足 gorilla/websocket 对"单连接单 writer"的要求。
//
// 退出路径:
//   - ctx.Done() → 自然结束
//   - outbox 收到的帧 WriteJSON / WriteMessage 失败 → return err 让 errgroup 收敛
//   - Ping 失败(客户端假死) → return err
func (s *Session) writeLoop(ctx context.Context, cancel context.CancelFunc) error {
	defer func() {
		if p := recover(); p != nil {
			cancel()
		}
	}()

	pingTicker := time.NewTicker(s.options.PingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-s.outbox:
			if err := s.writeOutbound(msg); err != nil {
				return err
			}

		case <-pingTicker.C:
			if err := s.wsConn.SetWriteDeadline(time.Now().Add(s.options.WriteWait)); err != nil {
				return err
			}
			if err := s.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return err
			}
		}
	}
}

func (s *Session) writeOutbound(msg outboundMessage) error {
	if msg.done != nil {
		defer close(msg.done)
	}
	if err := s.wsConn.SetWriteDeadline(time.Now().Add(s.options.WriteWait)); err != nil {
		return err
	}
	if msg.isJSON {
		return s.wsConn.WriteJSON(msg.jsonPayload)
	}
	return s.wsConn.WriteMessage(msg.messageType, msg.data)
}
