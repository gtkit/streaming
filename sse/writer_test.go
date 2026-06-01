package sse

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestWriterWriteHeadersAndEvent(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

	writer := New(c)
	writer.WriteHeaders()
	if err := writer.Event("chunk", map[string]any{
		"session_id": "s1",
		"delta":      "hello",
	}); err != nil {
		t.Fatalf("Event() error = %v", err)
	}

	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store, no-cache" {
		t.Fatalf("cache control = %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: chunk") {
		t.Fatalf("body missing event name: %s", body)
	}
	if !strings.Contains(body, `"session_id":"s1"`) || !strings.Contains(body, `"delta":"hello"`) {
		t.Fatalf("body missing payload: %s", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("body should end with event separator, got %q", body)
	}
}

func TestWriterEventReturnsContextError(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/sse", nil)
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	c.Request = req.WithContext(ctx)

	writer := New(c)
	err := writer.Event("chunk", map[string]string{"x": "y"})
	if err == nil {
		t.Fatal("Event() error = nil, want context error")
	}
	if err != context.Canceled {
		t.Fatalf("Event() error = %v, want %v", err, context.Canceled)
	}
}

// TestWriterSurvivesServerWriteTimeout 验证 SSE 连接不会被 http.Server.WriteTimeout 截断。
// 回归场景：问题2 —— 全局 WriteTimeout 作用到每次请求，心跳 Write 不会重置它；
// WriteHeaders 内部通过 http.ResponseController.SetWriteDeadline(time.Time{}) 清零。
func TestWriterSurvivesServerWriteTimeout(t *testing.T) {
	t.Parallel()

	const writeTimeout = 200 * time.Millisecond
	const totalDuration = writeTimeout * 5 // 明显超过 WriteTimeout，足以暴露问题

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.GET("/sse", func(c *gin.Context) {
		writer := New(c)
		writer.WriteHeaders()

		ticker := time.NewTicker(writeTimeout / 2)
		defer ticker.Stop()
		deadline := time.NewTimer(totalDuration)
		defer deadline.Stop()

		for {
			select {
			case <-deadline.C:
				_ = writer.Event("done", map[string]string{"ok": "1"})
				return
			case <-ticker.C:
				if err := writer.Event("tick", map[string]string{"t": time.Now().Format(time.RFC3339Nano)}); err != nil {
					t.Errorf("Event() during long-lived SSE failed: %v", err)
					return
				}
			case <-c.Request.Context().Done():
				return
			}
		}
	})

	server := httptest.NewUnstartedServer(engine)
	server.Config.WriteTimeout = writeTimeout
	server.Start()
	defer server.Close()

	resp, err := http.Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET sse failed: %v", err)
	}
	defer resp.Body.Close()

	// 读取整个响应；若 WriteTimeout 仍生效，连接会被服务端截断，读取不到 done 事件。
	reader := bufio.NewReader(resp.Body)
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read sse body failed: %v", err)
	}

	if !strings.Contains(string(body), "event: done") {
		t.Fatalf("expected SSE stream to survive past WriteTimeout and deliver done event, got:\n%s", body)
	}
}

func TestWriterCommentAndRetry(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

	writer := New(c)
	writer.WriteHeaders()
	if err := writer.Comment("keepalive"); err != nil {
		t.Fatalf("Comment() error = %v", err)
	}
	if err := writer.Retry(3000); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}

	body := recorder.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("body missing comment frame: %s", body)
	}
	if !strings.Contains(body, "retry: 3000") {
		t.Fatalf("body missing retry frame: %s", body)
	}
}

func TestWriterOperationsSetPerWriteDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		write    func(*Writer) error
		wantBody string
	}{
		{
			name: "event",
			write: func(writer *Writer) error {
				return writer.Event("chunk", map[string]string{"ok": "1"})
			},
			wantBody: "event: chunk",
		},
		{
			name: "comment",
			write: func(writer *Writer) error {
				return writer.Comment("keepalive")
			},
			wantBody: ": keepalive",
		},
		{
			name: "retry",
			write: func(writer *Writer) error {
				return writer.Retry(3000)
			},
			wantBody: "retry: 3000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gin.SetMode(gin.TestMode)
			recorder := newDeadlineResponseWriter()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Writer = recorder
			c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

			if err := tt.write(New(c)); err != nil {
				t.Fatalf("write error = %v", err)
			}

			if got := recorder.body.String(); !strings.Contains(got, tt.wantBody) {
				t.Fatalf("body missing %q: %s", tt.wantBody, got)
			}
			if len(recorder.deadlines) != 2 {
				t.Fatalf("deadline calls = %d, want 2", len(recorder.deadlines))
			}
			if recorder.deadlines[0].IsZero() {
				t.Fatal("first deadline is zero, want per-write timeout")
			}
			if time.Until(recorder.deadlines[0]) < defaultWriteTimeout/2 {
				t.Fatalf("first deadline = %s, want roughly %s in future", recorder.deadlines[0], defaultWriteTimeout)
			}
			if !recorder.deadlines[1].IsZero() {
				t.Fatalf("second deadline = %s, want cleared deadline", recorder.deadlines[1])
			}
		})
	}
}

type deadlineResponseWriter struct {
	headers    http.Header
	body       strings.Builder
	statusCode int
	size       int
	written    bool
	closeCh    chan bool
	deadlines  []time.Time
}

func newDeadlineResponseWriter() *deadlineResponseWriter {
	return &deadlineResponseWriter{
		headers:    make(http.Header),
		statusCode: http.StatusOK,
		closeCh:    make(chan bool),
	}
}

func (w *deadlineResponseWriter) Header() http.Header { return w.headers }

func (w *deadlineResponseWriter) Write(data []byte) (int, error) {
	w.WriteHeaderNow()
	n, err := w.body.WriteString(string(data))
	w.size += n
	return n, err
}

func (w *deadlineResponseWriter) WriteHeader(code int) {
	if !w.written {
		w.statusCode = code
	}
}

func (w *deadlineResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, http.ErrNotSupported
}

func (w *deadlineResponseWriter) Flush() { w.WriteHeaderNow() }

func (w *deadlineResponseWriter) CloseNotify() <-chan bool { return w.closeCh }

func (w *deadlineResponseWriter) Status() int { return w.statusCode }

func (w *deadlineResponseWriter) Size() int { return w.size }

func (w *deadlineResponseWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

func (w *deadlineResponseWriter) Written() bool { return w.written }

func (w *deadlineResponseWriter) WriteHeaderNow() {
	if !w.written {
		w.written = true
	}
}

func (w *deadlineResponseWriter) Pusher() http.Pusher { return nil }

func (w *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}
