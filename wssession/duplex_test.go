package wssession

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	gtkitjson "github.com/gtkit/json"
)

// duplexHandlers 构造双向模式 Handlers:首帧当订阅(任意内容),其后每条走 onMsg。
func duplexHandlers(onMsg func(ctx context.Context, raw []byte, sink PushSink) error) Handlers {
	return Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			return "tok", nil, nil
		},
		OnMessage: onMsg,
	}
}

// waitForEvent 等待指定类型事件,超时失败。
func waitForEvent(t *testing.T, events <-chan Event, want EventType) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Type == want {
				return
			}
		case <-deadline:
			t.Fatalf("3s 内未收到事件 %v", want)
		}
	}
}

func TestDuplexMultiTurn(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		var m map[string]any
		_ = gtkitjson.Unmarshal(raw, &m)
		return sink.Push(ctx, map[string]any{"echo": m["text"]})
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	if sub := readJSONFrame(t, conn, 2*time.Second); sub["event"] != "subscribed" {
		t.Fatalf("first frame = %v, want subscribed", sub["event"])
	}

	// 同一连接多轮:每条触发一次 OnMessage,连接保持
	for _, want := range []string{"a", "b", "c"} {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":"`+want+`"}`))
		got := readJSONFrame(t, conn, 2*time.Second)
		if got["echo"] != want {
			t.Fatalf("echo = %v, want %s", got["echo"], want)
		}
	}
}

func TestDuplexInterrupt(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	started := make(chan struct{}, 4)
	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		var m map[string]any
		_ = gtkitjson.Unmarshal(raw, &m)
		if m["slow"] == true {
			started <- struct{}{}
			<-ctx.Done() // 阻塞直到被打断
			return ctx.Err()
		}
		return sink.Push(ctx, map[string]any{"echo": m["text"]})
	})
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 慢消息:开启一轮并阻塞
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"slow":true}`))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("慢轮未启动")
	}

	// 新消息打断上一轮
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":"x"}`))
	waitForEvent(t, events, EventTurnInterrupted)

	if got := readJSONFrame(t, conn, 2*time.Second); got["echo"] != "x" {
		t.Fatalf("echo = %v, want x", got["echo"])
	}
}

func TestDuplexTurnCancelledOnClose(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var cancelled atomic.Bool
	started := make(chan struct{}, 1)
	h := duplexHandlers(func(ctx context.Context, _ []byte, _ PushSink) error {
		started <- struct{}{}
		<-ctx.Done() // 阻塞直到连接收敛
		cancelled.Store(true)
		return ctx.Err()
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))
	<-started

	_ = conn.Close() // 客户端断开 → 会话 ctx 取消 → turn ctx 取消

	deadline := time.After(3 * time.Second)
	for !cancelled.Load() {
		select {
		case <-deadline:
			t.Fatal("客户端 close 后 turn ctx 未被取消(可能泄漏)")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestDuplexOnMessageError(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := duplexHandlers(func(context.Context, []byte, PushSink) error {
		return errors.New("business failure")
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))

	msg := readJSONFrame(t, conn, 2*time.Second)
	if msg["event"] != "error" {
		t.Fatalf("event = %v, want error", msg["event"])
	}
	if code, _ := msg["code"].(float64); int(code) != CodeInternal {
		t.Fatalf("code = %v, want %d", msg["code"], CodeInternal)
	}
}

func TestDuplexRateLimit(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 32)
	h := duplexHandlers(func(ctx context.Context, _ []byte, sink PushSink) error {
		return sink.Push(ctx, map[string]any{"ok": 1})
	})
	opts := Options{
		InboundRatePerSecond: 1,
		InboundRateBurst:     1,
		OnEvent:              func(_ context.Context, ev Event) { events <- ev },
	}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 快速连发,超出 1/s burst 1
	for range 5 {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))
	}
	waitForEvent(t, events, EventRateLimited)
}
