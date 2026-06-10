package wssession

import (
	"testing"
	"time"
)

// TestPushMarshalErrorReturned:不可序列化的 payload 应让 Push 立即返回 error,且不入队任何帧。
func TestPushMarshalErrorReturned(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox:  make(chan outboundMessage, 1),
		options: Options{QueueOfferTimeout: time.Second},
	}
	sink := &pushSink{sess: s}

	// channel 无法被 JSON 序列化
	err := sink.Push(t.Context(), map[string]any{"bad": make(chan int)})
	if err == nil {
		t.Fatal("Push 不可序列化 payload 应返回 error")
	}

	select {
	case <-s.outbox:
		t.Fatal("序列化失败不应入队任何帧")
	default:
	}
}

// TestPushSerializesToBytes:正常 payload 被序列化为文本帧字节后入队。
func TestPushSerializesToBytes(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox:  make(chan outboundMessage, 1),
		options: Options{QueueOfferTimeout: time.Second},
	}
	sink := &pushSink{sess: s}

	if err := sink.Push(t.Context(), map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("Push error = %v", err)
	}
	msg := <-s.outbox
	if len(msg.data) == 0 {
		t.Fatal("入队帧 data 为空")
	}
	if string(msg.data) != `{"status":"ok"}` {
		t.Fatalf("data = %s, want {\"status\":\"ok\"}", msg.data)
	}
}
