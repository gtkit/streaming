// Package sse 提供基于 gin 的 Server-Sent Events 写入器：解决 SSE 长连接被
// http.Server.WriteTimeout 杀死的问题，并为每帧写入设置 per-write deadline。
package sse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	gtkitjson "github.com/gtkit/json/v2"

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

type rawData string

// Raw 将 data 标记为 Writer.Data 或 Stream.Data 的已编码 SSE data payload。
//
// 仅在确实需要绕过 JSON 序列化的 data-only 帧中使用,例如 OpenAI 风格哨兵:
// Data(sse.Raw("[DONE]"))。裸换行仍会被拆成多条 data 行,不能注入 event 或 id 字段。
func Raw(data string) any {
	return rawData(data)
}

// New 基于 gin.Context 创建一个 SSE Writer。
func New(c *gin.Context) *Writer {
	return &Writer{c: c}
}

// WriteHeaders 写入 SSE 响应头，并解除 http.Server.WriteTimeout 对本长连接的写截止。
func (w *Writer) WriteHeaders() {
	// SSE 是长连接：必须解除 http.Server.WriteTimeout 对本条连接的写截止时间，
	// 否则待支付订单、LLM 长响应会在全局 WriteTimeout 到期时被服务端 RST。
	// SetWriteDeadline(time.Time{}) 表示不设超时；失败（httptest 的
	// http.ErrNotSupported / 连接已半关）不影响响应头继续下发，让后续 Write 自然报错。
	rc := http.NewResponseController(w.c.Writer)
	_ = rc.SetWriteDeadline(time.Time{})

	w.c.Header("Content-Type", "text/event-stream; charset=utf-8")
	w.c.Header("Cache-Control", "no-store, no-cache")
	// Connection 是连接级头部,HTTP/2(RFC 9113)禁止;仅 HTTP/1.x 设置。
	if w.c.Request.ProtoMajor == 1 {
		w.c.Header("Connection", "keep-alive")
	}
	w.c.Header("X-Accel-Buffering", "no")
	w.c.Header("X-Content-Type-Options", "nosniff")
	w.c.Status(http.StatusOK)
}

// Event 写入一条命名 SSE 事件，payload 自动 JSON 序列化；写入带 per-write deadline。
// name 含 \r / \n / NUL 时返回错误（防 SSE 帧注入），不写出任何字节。
func (w *Writer) Event(name string, payload any) error {
	return w.writeEvent("", name, payload)
}

// EventWithID 写入一条带 `id:` 字段的命名 SSE 事件，用于断线续传：
// EventSource 自动重连时会把最后收到的 id 放进 `Last-Event-ID` 头回传
// （服务端用 LastEventID 读取，自行决定从哪续推）。
// id 为空串时不输出 `id:` 行，行为等同 Event。
// id / name 含 \r / \n / NUL 时返回错误（防 SSE 帧注入），不写出任何字节。
func (w *Writer) EventWithID(id, name string, payload any) error {
	return w.writeEvent(id, name, payload)
}

// Data 写入一条 data-only 帧（仅 `data:` 行，无事件名），即 OpenAI 风格的
// 流式块格式；payload 自动 JSON 序列化，Raw(...) 原样透传——
// 终止哨兵可写作 Data(sse.Raw("[DONE]"))，输出字面 `data: [DONE]`。
// 前端经 EventSource 的 onmessage（默认事件）接收。
func (w *Writer) Data(payload any) error {
	return w.writeEvent("", "", payload)
}

// writeEvent 是 Event / EventWithID / Data 共用的帧写入:id / name 为空串的
// 字段行省略,payload JSON 序列化为 data 行。
func (w *Writer) writeEvent(id, name string, payload any) error {
	select {
	case <-w.c.Request.Context().Done():
		return w.c.Request.Context().Err()
	default:
	}

	if err := validateFieldValue("id", id); err != nil {
		return err
	}
	if err := validateFieldValue("event name", name); err != nil {
		return err
	}

	// Raw 绕过序列化原样透传(gtkitjson.Marshal 会校验 JSON 合法性,
	// 而 OpenAI 风格的 `[DONE]` 哨兵不是合法 JSON);其余 payload 正常序列化。
	var data []byte
	if raw, ok := payload.(rawData); ok {
		data = []byte(raw)
	} else {
		marshaled, err := gtkitjson.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal sse payload: %w", err)
		}
		data = marshaled
	}

	var frame strings.Builder
	if id != "" {
		frame.WriteString("id: ")
		frame.WriteString(id)
		frame.WriteByte('\n')
	}
	if name != "" {
		frame.WriteString("event: ")
		frame.WriteString(name)
		frame.WriteByte('\n')
	}
	// data 含换行时按 SSE 规范拆成多个 data: 行(客户端以 \n 重新拼接)。
	// Marshal 输出换行必转义、走快路径;只有 raw 透传可能含裸换行,
	// 拆行同时关死该路径的帧注入面。
	if !bytes.ContainsAny(data, "\r\n") {
		frame.WriteString("data: ")
		frame.Write(data)
		frame.WriteByte('\n')
	} else {
		for line := range strings.SplitSeq(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
			frame.WriteString("data: ")
			frame.WriteString(line)
			frame.WriteByte('\n')
		}
	}
	frame.WriteByte('\n')

	return w.withWriteDeadline(func() error {
		if _, err := io.WriteString(w.c.Writer, frame.String()); err != nil {
			return fmt.Errorf("write sse event: %w", err)
		}
		return w.flush()
	})
}

// validateFieldValue 拒绝含换行 / NUL 的 SSE 字段值:换行可注入伪造帧,
// NUL 是 SSE 规范明确禁止的 id 字符。非法字段是调用方编程错误,直接报错不转义。
func validateFieldValue(field, v string) error {
	if strings.ContainsAny(v, "\r\n\x00") {
		return fmt.Errorf("sse: %s must not contain newline or NUL: %q", field, v)
	}
	return nil
}

// LastEventID 返回 EventSource 自动重连时携带的 `Last-Event-ID` 请求头
// （即客户端最后收到的 EventWithID 的 id），无则返回空串。
// 服务端据此决定断线续推的起点;本包不做事件缓存,重放策略由业务实现。
func LastEventID(c *gin.Context) string {
	return c.GetHeader("Last-Event-ID")
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

		return w.flush()
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

		return w.flush()
	})
}

// Context 返回绑定到本 SSE 连接的请求上下文。
func (w *Writer) Context() context.Context {
	return w.c.Request.Context()
}

// flush 把帧冲刷到客户端并暴露错误:相比 gin 的 void Flush,
// http.ResponseController.Flush 返回 error,客户端断开能在当帧发现,
// 而非等 TCP 缓冲塞满后才从下一次 Write 报错(LLM 流场景避免对死连接白推)。
// writer 不支持 Flush(http.ErrNotSupported,如 httptest 包装层)时静默降级,
// 与 SetWriteDeadline 的降级策略一致。
func (w *Writer) flush() error {
	if err := http.NewResponseController(w.c.Writer).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return fmt.Errorf("flush sse: %w", err)
	}
	return nil
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
