package wssession

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

// Session 代表一个被 wssession 接管的 WebSocket 连接。
//
// 字段对外只读暴露(通过方法),内部状态由 readLoop / processLoop / writeLoop 协作维护。
type Session struct {
	wsConn   *websocket.Conn
	options  Options
	handlers Handlers

	// inbox 是 readLoop → processLoop 的有界 channel(默认容量 4)。
	inbox chan inboundFrame

	// outbox 是业务 → writeLoop 的有界 channel(默认容量 128),
	// 通过 PushSink.Push 间接写入,不暴露给业务直接 send。
	outbox chan outboundMessage

	// path 用于 connCap key 拼接,在 Serve 入口从 r.URL.Path 提取。
	path string

	// subscribed 标记是否已完成首帧 + ParseRequest + tokenCap 三步;
	// 切到 true 后 readLoop 检测到任何业务帧立即拒。
	subscribed atomic.Bool

	closeOnce sync.Once
}

const errorFrameQueueOfferTimeout = 500 * time.Millisecond

// Serve 完成一个 wsmsg 连接的完整托管流程。
//
// 流程(详见 docs/wsmsg-flow.md §1):
//
//	① IP 维度 connCap(Upgrade 之前;失败 HTTP 429,不 Upgrade)
//	② HTTP Upgrade(Origin 校验在 Upgrader.CheckOrigin 内完成)
//	③ context.WithTimeout(parent, MaxSessionDuration)
//	④ OnConnect hook(可选)
//	⑤ 启动 readLoop / processLoop / writeLoop(errgroup)
//	⑥ 等所有 goroutine 收敛 + 释放资源
//
// 返回值:
//   - nil          : 正常关闭(Run 自然 return / 客户端 close / ctx 超时)
//   - non-nil err  : 任一 goroutine 异常 / IP cap 满 / Upgrade 失败 / OnConnect err
func Serve(parent context.Context, w http.ResponseWriter, r *http.Request, options Options, handlers Handlers) error {
	if err := handlers.validate(); err != nil {
		return err
	}
	if err := options.Validate(); err != nil {
		return err
	}
	opts := normalizeOptions(options)

	path := r.URL.Path

	// ① IP 维度 connCap(Upgrade 之前)
	var ipCapKey string
	if opts.ConnCapEnabled {
		ipCapKey = "ip:" + clientIP(r) + ":" + path
		if _, ok := tryAcquire(ipCapKey, opts.ConnCapIPMax); !ok {
			// 普通 HTTP 响应,不进入 WS 协议层
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":429,"msg":"` + ReasonTooManyIPConn + `","data":null}`))
			return errors.New("wssession: ip connCap exceeded")
		}
		defer release(ipCapKey)
	}

	// ② HTTP Upgrade(Origin 校验在 CheckOrigin 内)
	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     newOriginChecker(opts.AllowedOrigins),
	}
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// gorilla 已写 HTTP 4xx,直接 return
		return err
	}

	// ③ context.WithTimeout(MaxSessionDuration)
	ctx, cancel := context.WithTimeout(parent, opts.MaxSessionDuration)
	defer cancel()

	sess := &Session{
		wsConn:   wsConn,
		options:  opts,
		handlers: handlers,
		inbox:    make(chan inboundFrame, opts.InboundBufferSize),
		outbox:   make(chan outboundMessage, opts.OutboundBufferSize),
		path:     path,
	}
	defer func() { _ = sess.Close() }()

	// 当 ctx 取消时立刻 close 底层连接,把 readLoop 从 ReadMessage 阻塞中踹醒
	go func() {
		<-ctx.Done()
		_ = sess.Close()
	}()

	// ④ OnConnect hook(可选)
	if handlers.OnConnect != nil {
		if err := handlers.OnConnect(ctx, sess); err != nil {
			return err
		}
	}

	// ⑤ 启动 3 个 goroutine(errgroup)
	group, runCtx := errgroup.WithContext(ctx)
	groupCancel := func() {
		// errgroup 自身收敛 ctx,这里提供给各 loop 的 panic recovery 调用
		cancel()
	}
	group.Go(func() error { return sess.readLoop(runCtx, groupCancel) })
	group.Go(func() error { return sess.processLoop(runCtx, groupCancel) })
	group.Go(func() error { return sess.writeLoop(runCtx, groupCancel) })

	// ⑥ 等所有 goroutine 收敛
	waitErr := group.Wait()

	// 过滤预期 close 错误
	if waitErr != nil && !IsExpectedClose(waitErr) {
		return waitErr
	}
	return nil
}

// Close 幂等关闭底层 WS 连接。
//
// 在 Serve defer 中 + ctx.Done 监听 goroutine 内均会调用,sync.Once 保证只关一次。
func (s *Session) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		if s.wsConn != nil {
			closeErr = s.wsConn.Close()
		}
	})
	return closeErr
}

// closeWithError 下发一帧 error JSON,**同步**等 writeLoop flush 完后再 close 底层连接。
//
// 行为约定:
//   - 入队带 done 信号的 error 帧
//   - **同步**等 done 关闭(writeLoop 已写出帧)或 1s 兜底超时
//   - 主动 close 底层 conn,踹醒 readLoop 立刻退出
//
// 同步等待是关键:若立即 close,writeLoop 会因 wsConn 关闭而 WriteJSON 失败,
// error 帧丢失,客户端只看到 abnormal closure 而无错误码/原因。
//
// 调用方应在调用本方法后 return,让所在的 loop 退出 → errgroup 收敛 → defer Close。
func (s *Session) closeWithError(ctx context.Context, code int, reason string) {
	frame := errorFrame{
		Event:     "error",
		Code:      code,
		Reason:    truncateErrorReason(reason),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	done := make(chan struct{})
	if err := s.queueWithTimeout(ctx, outboundMessage{
		isJSON:      true,
		jsonPayload: frame,
		done:        done,
	}, errorFrameQueueOfferTimeout); err != nil {
		// 入队失败(ctx done / slow consumer)→ 直接关
		_ = s.Close()
		return
	}
	// 同步等 writeLoop flush 完 error 帧(1s 兜底,防 writeLoop 因 ctx done 已退出)
	select {
	case <-done:
	case <-time.After(time.Second):
	}
	_ = s.Close()
}

func truncateErrorReason(reason string) string {
	if len(reason) <= maxErrorReasonLen {
		return reason
	}
	return reason[:maxErrorReasonLen]
}

// IsExpectedClose 用于识别浏览器主动断开 / 正常 EOF / errgroup 内部 cancel 触发的 close,
// 这类"错误"不应作为服务端异常上报。
func IsExpectedClose(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if websocket.IsCloseError(err,
		websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseNoStatusReceived,
		websocket.CloseAbnormalClosure,
	) {
		return true
	}
	return false
}

// clientIP 从 http.Request 提取客户端 IP(优先 X-Forwarded-For 首段,fallback RemoteAddr)。
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// 取第一段(逗号分隔列表)
		for i := range len(xff) {
			if xff[i] == ',' {
				return trimSpace(xff[:i])
			}
		}
		return trimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// trimSpace 是 strings.TrimSpace 的轻量替代,避免 import strings 仅为此一处。
func trimSpace(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
