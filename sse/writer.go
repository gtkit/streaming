// Package sse 提供基于 gin 的 Server-Sent Events 写入器：解决 SSE 长连接被
// http.Server.WriteTimeout 杀死的问题，并为每帧写入设置 per-write deadline。
package sse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	gtkitjson "github.com/gtkit/json"

	"github.com/gin-gonic/gin"
)

// Writer 是基于 gin.Context 的低层 SSE 写入器，负责设置 SSE 响应头与逐帧写入。
//
// 并发安全：Writer **非并发安全**——它直接写底层 gin.ResponseWriter，
// 不做任何串行化。若需从多个 goroutine（如心跳 + 业务推送）写入同一连接，
// 请改用 Stream（它用互斥锁串行化所有写方法）。
type Writer struct {
	c *gin.Context
}

const defaultWriteTimeout = 10 * time.Second

// New 基于 gin.Context 创建一个 SSE Writer。
func New(c *gin.Context) *Writer {
	return &Writer{c: c}
}

// WriteHeaders 写入 SSE 响应头，并解除 http.Server.WriteTimeout 对本长连接的写截止。
func (w *Writer) WriteHeaders() {
	// SSE 是长连接：必须解除 http.Server.WriteTimeout 对本条连接的写截止时间，
	// 否则待支付订单、LLM 长响应会在全局 WriteTimeout 到期时被服务端 RST。
	// SetWriteDeadline(time.Time{}) 表示不设超时；在 httptest 等不支持的 writer 上
	// 会返回 http.ErrNotSupported，此时忽略即可——生产 net/http.Server 始终支持。
	rc := http.NewResponseController(w.c.Writer)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		// 非预期错误（例如连接已半关），不影响响应头继续下发；让后续 Write 自然报错。
		_ = err
	}

	w.c.Header("Content-Type", "text/event-stream; charset=utf-8")
	w.c.Header("Cache-Control", "no-store, no-cache")
	w.c.Header("Connection", "keep-alive")
	w.c.Header("X-Accel-Buffering", "no")
	w.c.Status(http.StatusOK)
}

// Event 写入一条命名 SSE 事件，payload 自动 JSON 序列化；写入带 per-write deadline。
func (w *Writer) Event(name string, payload any) error {
	select {
	case <-w.c.Request.Context().Done():
		return w.c.Request.Context().Err()
	default:
	}

	data, err := gtkitjson.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal sse payload: %w", err)
	}

	return w.withWriteDeadline(func() error {
		if _, err := fmt.Fprintf(w.c.Writer, "event: %s\ndata: %s\n\n", name, data); err != nil {
			return fmt.Errorf("write sse event: %w", err)
		}
		w.c.Writer.Flush()
		return nil
	})
}

// Comment 写入一条 SSE 注释帧。
// 注释帧不会触发前端的业务事件回调，常用于链路保活、调试标记或代理层防空闲断开。
func (w *Writer) Comment(text string) error {
	select {
	case <-w.c.Request.Context().Done():
		return w.c.Request.Context().Err()
	default:
	}

	return w.withWriteDeadline(func() error {
		lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
		for _, line := range lines {
			if _, err := fmt.Fprintf(w.c.Writer, ": %s\n", line); err != nil {
				return fmt.Errorf("write sse comment: %w", err)
			}
		}
		if _, err := fmt.Fprint(w.c.Writer, "\n"); err != nil {
			return fmt.Errorf("write sse comment tail: %w", err)
		}

		w.c.Writer.Flush()
		return nil
	})
}

// Retry 写入 SSE 的 retry 指令，提示客户端后续重连间隔（毫秒）。
// 这是 SSE 协议的一部分，浏览器/EventSource 客户端会把它作为建议重连时间使用。
func (w *Writer) Retry(milliseconds int) error {
	select {
	case <-w.c.Request.Context().Done():
		return w.c.Request.Context().Err()
	default:
	}

	return w.withWriteDeadline(func() error {
		if _, err := fmt.Fprintf(w.c.Writer, "retry: %d\n\n", milliseconds); err != nil {
			return fmt.Errorf("write sse retry: %w", err)
		}

		w.c.Writer.Flush()
		return nil
	})
}

// Context 返回绑定到本 SSE 连接的请求上下文。
func (w *Writer) Context() context.Context {
	return w.c.Request.Context()
}

func (w *Writer) withWriteDeadline(fn func() error) error {
	rc := http.NewResponseController(w.c.Writer)
	if err := rc.SetWriteDeadline(time.Now().Add(defaultWriteTimeout)); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			return fmt.Errorf("set sse write deadline: %w", err)
		}
		return fn()
	}
	defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()

	return fn()
}
