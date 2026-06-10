package wssession

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
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

	// tokenCapKey 在 processLoop 内 tryAcquire 成功后写入,由 Serve 在连接
	// 真正结束(group.Wait 之后)释放——cap 占用与连接生命周期对齐,
	// 不随 processLoop 提前退出而提前释放。
	// 并发安全:processLoop 写、Serve 在 group.Wait 返回后读,Wait 构成 happens-before。
	tokenCapKey string

	// subscribed 标记是否已完成首帧 + ParseRequest + tokenCap 三步;
	// 切到 true 后 readLoop 检测到任何业务帧立即拒。
	subscribed atomic.Bool

	// firstFrameClaimed 裁决"首帧到达"与"首帧超时"的竞态:二者各自 CAS,
	// 只有抢到的一方能对连接采取动作,避免首帧已收到却被误判超时关闭。
	firstFrameClaimed atomic.Bool

	// errFrameOnce 保证并发错误关闭时只下发首个 error 帧。
	errFrameOnce sync.Once

	closeOnce sync.Once
}

const errorFrameQueueOfferTimeout = 500 * time.Millisecond

// closeFrameWriteTimeout 是 ctx 收敛路径 best-effort 发送 close 控制帧的写超时;
// 客户端假死时最多延迟这么久再裸关,不阻碍连接关闭。
const closeFrameWriteTimeout = time.Second

// wsWriteBufferPool 供所有连接共享写缓冲(gorilla 仅在写帧瞬间占用),
// 避免每连接常驻 4KB 写缓冲。
var wsWriteBufferPool = &sync.Pool{}

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
		ipCapKey = "ip:" + clientIP(r, opts.TrustedProxyCount) + ":" + path
		if _, ok := tryAcquire(ipCapKey, opts.ConnCapIPMax); !ok {
			opts.emit(parent, Event{Type: EventCapRejected, Reason: ReasonTooManyIPConn, Key: ipCapKey})
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
		WriteBufferPool: wsWriteBufferPool,
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

	// token cap 在 processLoop 内 acquire,但占用必须覆盖整条连接的生命周期,
	// 故释放挂在 Serve 退出(group.Wait 之后),与上面 ipCapKey 的时序一致。
	defer func() {
		if sess.tokenCapKey != "" {
			release(sess.tokenCapKey)
		}
	}()

	// 当 ctx 取消时(MaxSessionDuration 到期 / 上游取消)收敛连接,把 readLoop
	// 从 ReadMessage 阻塞中踹醒。close 握手兜底(1001)在 Close 内统一完成,
	// 避免多个收敛者(本 watcher / closeNormal / closeWithError)竞争出帧。
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
	if waitErr == nil {
		return nil
	}

	// 1006 异常断开:上报事件,但不作为错误返回(避免网络抖动变成误报)
	if isAbnormalClose(waitErr) {
		opts.emit(ctx, Event{Type: EventAbnormalClose, Reason: "abnormal closure", Err: waitErr})
		return nil
	}

	// 过滤其余预期 close 错误
	if !IsExpectedClose(waitErr) {
		return waitErr
	}
	return nil
}

// Close 幂等关闭底层 WS 连接,关闭前 best-effort 补发 close 握手帧。
//
// 在 Serve defer、ctx.Done 监听 goroutine、closeNormal / closeWithError 内
// 均会调用,sync.Once 保证只关一次。
//
// 兜底握手:若此前已通过 outbox 写出过 close 帧(Run 正常结束的 1000 /
// 错误关闭的 1008/1011),gorilla 对重复 close 帧返回 ErrCloseSent、不上写,
// 客户端只看到先到的那帧;若尚未发过(会话超时 / 上游取消 / flush 失败的
// 服务端单方面终止),则以 1001 GoingAway 完成握手,提示客户端走重连分支,
// 避免裸关 TCP 让客户端只看到 1006。
func (s *Session) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		if s.wsConn != nil {
			_ = s.wsConn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, ""),
				time.Now().Add(closeFrameWriteTimeout))
			closeErr = s.wsConn.Close()
		}
	})
	return closeErr
}

// closeWithError 依次下发一帧 error JSON 与一帧 close 控制帧,**同步**等
// writeLoop flush 完后再 close 底层连接,完成 WebSocket 关闭握手。
//
// 行为约定:
//   - 入队 error 帧,再入队带 done 信号的 close 控制帧(channel FIFO 保证先后)
//   - **同步**等 done 关闭(writeLoop 已写出两帧)或 1s 兜底超时
//   - 主动 close 底层 conn,踹醒 readLoop 立刻退出
//
// 同步等待是关键:若立即 close,writeLoop 会因 wsConn 关闭而 WriteMessage 失败,
// error 帧丢失,客户端只看到 abnormal closure 而无错误码/原因。
//
// 调用方应在调用本方法后 return,让所在的 loop 退出 → errgroup 收敛 → defer Close。
func (s *Session) closeWithError(ctx context.Context, code int, reason string) {
	// 幂等:并发触发只下发首个 error 帧,避免客户端收到矛盾的错误码。
	s.errFrameOnce.Do(func() {
		frame := errorFrame{
			Event:     "error",
			Code:      code,
			Reason:    truncateErrorReason(reason),
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		}
		if err := s.queueWithTimeout(ctx, jsonFrame(frame), errorFrameQueueOfferTimeout); err != nil {
			// 入队失败(ctx done / slow consumer)→ 不等 flush,落到下面统一 Close
			return
		}
		// close 握手帧排在 error 帧之后写出,done 挂在最后一帧上,单次等待覆盖两帧。
		s.flushCloseFrame(ctx, wsCloseCode(code), errorFrameQueueOfferTimeout)
	})
	_ = s.Close()
}

// closeNormal 发送 close(1000) 握手帧并等 flush 后关闭连接。
//
// 用于单向模式 Run 正常返回后的主动收敛:channel FIFO 保证 Run 已 Push 的
// 尾部业务帧先于 close 帧写出。conn 关闭后 readLoop 解阻塞返回预期 close
// 错误,errgroup 随之收敛,连接不再悬挂等客户端断开或 MaxSessionDuration。
func (s *Session) closeNormal(ctx context.Context) {
	s.flushCloseFrame(ctx, websocket.CloseNormalClosure, s.options.QueueOfferTimeout)
	_ = s.Close()
}

// flushCloseFrame 入队一帧 close 控制帧并同步等 writeLoop 写出;
// ctx 取消(连接已在收敛,close 握手由 Serve 的 ctx watcher best-effort 兜底)
// 或入队失败时直接放弃,由调用方裸关。1s 兜底防 writeLoop 恰在入队后退出。
func (s *Session) flushCloseFrame(ctx context.Context, wsCode int, offerTimeout time.Duration) {
	done := make(chan struct{})
	msg := outboundMessage{
		messageType: websocket.CloseMessage,
		data:        websocket.FormatCloseMessage(wsCode, ""),
		done:        done,
	}
	if err := s.queueWithTimeout(ctx, msg, offerTimeout); err != nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	case <-time.After(time.Second):
	}
}

// wsCloseCode 把下发给客户端的业务错误码映射为 WebSocket close code:
// 500 → 1011(internal error),其余(408/415/422/429)→ 1008(policy violation)。
func wsCloseCode(code int) int {
	if code == CodeInternal {
		return websocket.CloseInternalServerErr
	}
	return websocket.ClosePolicyViolation
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
	) {
		return true
	}
	return false
}

// isAbnormalClose 识别 1006(CloseAbnormalClosure):无正常 close 握手的断开。
//
// 1006 不再归为预期 close(见 IsExpectedClose):它可能是客户端网络抖动,
// 也可能掩盖服务端写超时等真实问题,故单独识别并通过 OnEvent 上报,
// 但不作为 Serve 错误返回(避免把常见网络抖动变成调用方的错误误报)。
func isAbnormalClose(err error) bool {
	return websocket.IsCloseError(err, websocket.CloseAbnormalClosure)
}

// clientIP 提取用于 IP 维度 connCap 的客户端 IP。
//
// trustedProxyCount<=0 时只用传输层 RemoteAddr,忽略客户端可伪造的
// X-Forwarded-For;>0 时从 X-Forwarded-For 列表由右向左取第 trustedProxyCount
// 跳(可信代理把上游地址追加在右侧),越界回退到列表最左端或 RemoteAddr。
func clientIP(r *http.Request, trustedProxyCount int) string {
	remote := remoteHost(r)
	if trustedProxyCount <= 0 {
		return remote
	}

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return remote
	}

	parts := strings.Split(xff, ",")
	idx := max(len(parts)-trustedProxyCount, 0)
	if ip := strings.TrimSpace(parts[idx]); ip != "" {
		return ip
	}
	return remote
}

// remoteHost 返回 RemoteAddr 的 host 部分(无端口);解析失败时原样返回。
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
