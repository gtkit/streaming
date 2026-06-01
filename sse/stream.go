package sse

import (
	"context"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Stream is a business-facing SSE writer.
// Compared to the lower-level Writer, Stream additionally:
// 1. Automatically writes SSE response headers on first event;
// 2. Tracks whether the response has already started;
// 3. Provides unified ping / error event helpers.
type Stream struct {
	writer  *Writer
	mu      sync.Mutex
	started bool
}

// NewStream creates a new SSE Stream from a gin context.
func NewStream(c *gin.Context) *Stream {
	return &Stream{writer: New(c)}
}

// Start explicitly starts the SSE response.
func (s *Stream) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()
}

// Event sends a named SSE event.
// Automatically writes SSE headers if the response has not started yet.
func (s *Stream) Event(name string, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()

	return s.writer.Event(name, payload)
}

// Ping sends a standard keep-alive comment frame.
func (s *Stream) Ping(at time.Time) error {
	return s.Comment("ping " + at.UTC().Format(time.RFC3339))
}

// Error sends a standard business error event.
func (s *Stream) Error(payload any) error {
	return s.Event("error", payload)
}

// Comment sends a comment frame.
func (s *Stream) Comment(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()

	return s.writer.Comment(text)
}

// Retry tells the client the recommended reconnection time (milliseconds).
func (s *Stream) Retry(milliseconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()

	return s.writer.Retry(milliseconds)
}

// Started returns whether the SSE response has already started writing.
func (s *Stream) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// Context returns the request context bound to this SSE connection.
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
