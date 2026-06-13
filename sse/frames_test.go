package sse

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// newTestWriter 构造 Writer + recorder 测试对。
func newTestWriter(t *testing.T) (*Writer, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)
	return New(c), recorder
}

// ============================================================================
// EventWithID:断线续传 id 字段
// ============================================================================

func TestWriterEventWithID(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.EventWithID("42", "chunk", map[string]string{"delta": "hi"}); err != nil {
		t.Fatalf("EventWithID() error = %v", err)
	}

	body := rec.Body.String()
	if !strings.HasPrefix(body, "id: 42\nevent: chunk\ndata: ") {
		t.Fatalf("frame = %q, want id/event/data 三行序", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("frame should end with separator, got %q", body)
	}
}

func TestWriterEventWithIDEmptyIDEqualsEvent(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.EventWithID("", "chunk", map[string]string{"x": "y"}); err != nil {
		t.Fatalf("EventWithID() error = %v", err)
	}
	if body := rec.Body.String(); strings.Contains(body, "id:") {
		t.Fatalf("empty id should omit id line, got %q", body)
	}
}

func TestLastEventID(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)
	c.Request.Header.Set("Last-Event-ID", "42")

	if got := LastEventID(c); got != "42" {
		t.Fatalf("LastEventID() = %q, want 42", got)
	}

	c.Request.Header.Del("Last-Event-ID")
	if got := LastEventID(c); got != "" {
		t.Fatalf("LastEventID() = %q, want empty", got)
	}
}

// ============================================================================
// Data:data-only 帧(OpenAI 风格)
// ============================================================================

func TestWriterData(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Data(map[string]string{"delta": "hi"}); err != nil {
		t.Fatalf("Data() error = %v", err)
	}

	body := rec.Body.String()
	if !strings.HasPrefix(body, "data: ") {
		t.Fatalf("frame = %q, want data-only", body)
	}
	if strings.Contains(body, "event:") || strings.Contains(body, "id:") {
		t.Fatalf("data-only frame must not contain event/id line, got %q", body)
	}
}

func TestWriterDataRawSentinel(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Data(Raw("[DONE]")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if body := rec.Body.String(); body != "data: [DONE]\n\n" {
		t.Fatalf("frame = %q, want literal data: [DONE]", body)
	}
}

func TestWriterDataRawMultilineSplitsDataLines(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	// raw 透传含裸换行:按 SSE 规范拆成多个 data: 行,换行无法逃出 data 字段
	if err := w.Data(Raw("line1\nline2")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if body := rec.Body.String(); body != "data: line1\ndata: line2\n\n" {
		t.Fatalf("frame = %q, want multiline data lines", body)
	}
}

// ============================================================================
// 字段注入防护
// ============================================================================

func TestWriterRejectsFieldInjection(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		call func(w *Writer) error
	}{
		{"event name 换行", func(w *Writer) error { return w.Event("x\ndata: evil", nil) }},
		{"event name 回车", func(w *Writer) error { return w.Event("x\rdata: evil", nil) }},
		{"id 换行", func(w *Writer) error { return w.EventWithID("1\n2", "n", nil) }},
		{"id NUL", func(w *Writer) error { return w.EventWithID("1\x002", "n", nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w, rec := newTestWriter(t)
			if err := tc.call(w); err == nil {
				t.Fatal("expected injection rejection error, got nil")
			}
			if rec.Body.Len() != 0 {
				t.Fatalf("rejected frame must not write bytes, got %q", rec.Body.String())
			}
		})
	}
}

// ============================================================================
// 响应头合规:h2 无 Connection,nosniff 恒有
// ============================================================================

func TestWriteHeadersProtocolAware(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)

	// HTTP/1.1:有 Connection
	rec1 := httptest.NewRecorder()
	c1, _ := gin.CreateTestContext(rec1)
	c1.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)
	New(c1).WriteHeaders()
	if got := rec1.Header().Get("Connection"); got != "keep-alive" {
		t.Fatalf("HTTP/1 Connection = %q, want keep-alive", got)
	}

	// HTTP/2:无 Connection(连接级头部,RFC 9113 禁止)
	rec2 := httptest.NewRecorder()
	c2, _ := gin.CreateTestContext(rec2)
	req2 := httptest.NewRequest(http.MethodGet, "/sse", nil)
	req2.Proto, req2.ProtoMajor, req2.ProtoMinor = "HTTP/2.0", 2, 0
	c2.Request = req2
	New(c2).WriteHeaders()
	if got := rec2.Header().Get("Connection"); got != "" {
		t.Fatalf("HTTP/2 Connection = %q, want empty", got)
	}

	for _, rec := range []*httptest.ResponseRecorder{rec1, rec2} {
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
		}
	}
}

// ============================================================================
// Stream 封装:EventWithID / Data 自动写头
// ============================================================================

func TestStreamEventWithIDAndDataAutoStart(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

	s := NewStream(c)
	if err := s.EventWithID("7", "chunk", map[string]int{"n": 1}); err != nil {
		t.Fatalf("EventWithID() error = %v", err)
	}
	if err := s.Data(Raw("[DONE]")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}

	if !s.Started() {
		t.Fatal("stream should be started after first frame")
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "id: 7\nevent: chunk\n") || !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("body = %q", body)
	}
}
