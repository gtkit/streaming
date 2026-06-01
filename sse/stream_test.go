package sse

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestStreamEventAutoStarts(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

	stream := NewStream(c)
	if stream.Started() {
		t.Fatal("Started() = true before first event, want false")
	}

	if err := stream.Event("status", map[string]any{"status": "pending"}); err != nil {
		t.Fatalf("Event() error = %v", err)
	}

	if !stream.Started() {
		t.Fatal("Started() = false after first event, want true")
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: status") {
		t.Fatalf("body missing event name: %s", body)
	}
	if !strings.Contains(body, `"status":"pending"`) {
		t.Fatalf("body missing payload: %s", body)
	}
}

func TestStreamPing(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

	stream := NewStream(c)
	ts := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	if err := stream.Ping(ts); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	body := recorder.Body.String()
	if strings.Contains(body, "event: ping") {
		t.Fatalf("body should not contain ping event: %s", body)
	}
	if !strings.Contains(body, ": ping 2026-03-30T10:00:00Z") {
		t.Fatalf("body missing ping comment: %s", body)
	}
}

func TestStreamErrorAutoStarts(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

	stream := NewStream(c)
	if err := stream.Error(map[string]string{"error": "boom"}); err != nil {
		t.Fatalf("Error() error = %v", err)
	}

	if !stream.Started() {
		t.Fatal("Started() = false after Error(), want true")
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: error") {
		t.Fatalf("body missing error event: %s", body)
	}
	if !strings.Contains(body, `"error":"boom"`) {
		t.Fatalf("body missing error payload: %s", body)
	}
}

func TestStreamCommentAndRetryAutoStart(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)

	stream := NewStream(c)
	if err := stream.Comment("keepalive"); err != nil {
		t.Fatalf("Comment() error = %v", err)
	}
	if err := stream.Retry(1500); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}

	if !stream.Started() {
		t.Fatal("Started() = false after Comment/Retry, want true")
	}
	body := recorder.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("body missing comment frame: %s", body)
	}
	if !strings.Contains(body, "retry: 1500") {
		t.Fatalf("body missing retry frame: %s", body)
	}
}
