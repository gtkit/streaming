package sse

import (
	"context"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Stream 是面向业务的 SSE 写入器。相比底层 Writer,Stream 额外:
//  1. 首个事件自动写入 SSE 响应头;
//  2. 跟踪响应是否已开始;
//  3. 提供统一的 ping / error 事件辅助方法。
//
// 并发安全:Stream 用互斥锁串行化所有写方法,可从不同 goroutine
// (如心跳 goroutine + 业务 goroutine)并发调用。
type Stream struct {
	writer  *Writer
	mu      sync.Mutex
	started bool
}

// NewStream 基于 gin.Context 创建一个 SSE Stream。
func NewStream(c *gin.Context) *Stream {
	return &Stream{writer: New(c)}
}

// Start 显式启动 SSE 响应(写入响应头)。
func (s *Stream) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()
}

// Event 发送一条命名 SSE 事件;响应尚未开始时自动先写 SSE 响应头。
func (s *Stream) Event(name string, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()

	return s.writer.Event(name, payload)
}

// Ping 发送一条标准保活注释帧。
func (s *Stream) Ping(at time.Time) error {
	return s.Comment("ping " + at.UTC().Format(time.RFC3339))
}

// Error 发送一条标准业务 error 事件。
func (s *Stream) Error(payload any) error {
	return s.Event("error", payload)
}

// Comment 发送一条注释帧。
func (s *Stream) Comment(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()

	return s.writer.Comment(text)
}

// Retry 告知客户端建议的重连间隔(毫秒)。
func (s *Stream) Retry(milliseconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()

	return s.writer.Retry(milliseconds)
}

// Started 返回 SSE 响应是否已开始写入。
func (s *Stream) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// Context 返回绑定到本 SSE 连接的请求上下文。
func (s *Stream) Context() context.Context {
	return s.writer.Context()
}

func (s *Stream) startLocked() {
	if s.started {
		return
	}
	s.writer.WriteHeaders()
	s.started = true
}
