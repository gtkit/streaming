package sse

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// newTestStream 构造一个挂在 httptest recorder 上的 Stream。
func newTestStream(recorder *httptest.ResponseRecorder) *Stream {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/sse", nil)
	return NewStream(c)
}

// TestStreamConcurrentWrite 验证 Stream 用锁串行化后可从多个 goroutine 并发写入。
// 必须在 -race 下运行才有意义。
func TestStreamConcurrentWrite(t *testing.T) {
	t.Parallel()
	recorder := httptest.NewRecorder()
	stream := newTestStream(recorder)

	const writers = 8
	const perWriter = 50
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := range writers {
		go func(w int) {
			defer wg.Done()
			for range perWriter {
				switch w % 3 {
				case 0:
					_ = stream.Event("tick", map[string]int{"w": w})
				case 1:
					_ = stream.Comment("keepalive")
				default:
					_ = stream.Ping(time.Unix(0, 0))
				}
			}
		}(w)
	}
	wg.Wait()

	if !stream.Started() {
		t.Fatal("Started() = false after concurrent writes, want true")
	}
}

// BenchmarkStreamEvent 测 SSE 命名事件写入热路径(序列化 + 格式化 + flush)。
func BenchmarkStreamEvent(b *testing.B) {
	recorder := httptest.NewRecorder()
	stream := newTestStream(recorder)
	payload := map[string]any{"status": "pending", "order": 12345}

	b.ReportAllocs()
	for b.Loop() {
		_ = stream.Event("status", payload)
		recorder.Body.Reset()
	}
}
