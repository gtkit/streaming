# streaming

`github.com/gtkit/streaming` 提供与业务无关的实时/流式传输底座，含两个独立子包，覆盖同一场景
（服务端向客户端持续推送状态）的两种传输方式：

| 子包 | import 路径 | 传输 | 用途 |
|---|---|---|---|
| sse | `github.com/gtkit/streaming/sse` | Server-Sent Events | 单向流式推送；核心解决 SSE 长连接被 `http.Server.WriteTimeout` 杀死的问题 + 每帧写超时 |
| wssession | `github.com/gtkit/streaming/wssession` | WebSocket | 生产级 WS 会话生命周期：心跳、反压、连接 cap、Origin 白名单、首帧超时、panic 恢复 |

两个子包均**业务无关**：业务逻辑通过回调 / 接口注入，包本身不含任何领域语义；也**不绑定日志栈**，
错误通过返回值上抛，由调用方决定如何记录。

## 安装

```bash
go get github.com/gtkit/streaming
```

要求 Go 1.26+。两个子包依赖隔离：只 `import` `streaming/sse` 时，`wssession` 依赖的
`gorilla/websocket` 不会进入你的 `go.mod` / 二进制（反之亦然）。

---

## streaming/sse

基于 gin 的 SSE 写入器。相对 gin 原生 `c.SSEvent` / `gin-contrib/sse` 的增量在于**长连接写超时管理**：

- `WriteHeaders` 解除 `http.Server.WriteTimeout`（否则长响应会在全局超时到期时被 RST）
- 每帧 `Event` / `Comment` / `Retry` 写入带 per-write deadline，防慢客户端阻塞 goroutine

### 引用

```go
import "github.com/gtkit/streaming/sse"
```

### 完整示例：流式推送订单状态（快照 + 轮询 + 心跳）

```go
package main

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/streaming/sse"
)

func main() {
	r := gin.New()
	r.GET("/orders/:id/stream", streamOrderStatus)
	_ = r.Run(":8080")
}

func streamOrderStatus(c *gin.Context) {
	// NewStream：首次写事件时自动下发 SSE 响应头并解除 http.Server.WriteTimeout。
	// （需手动控制写头时机可改用 sse.New(c) + w.WriteHeaders()。）
	stream := sse.NewStream(c)

	// 1) 立即推一帧快照
	if err := stream.Event("snapshot", gin.H{"status": "pending"}); err != nil {
		return // 客户端已断开，停止推送
	}

	// 2) 周期轮询 + 心跳，直到命中或客户端断开
	poll := time.NewTicker(3 * time.Second)
	defer poll.Stop()
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return // 客户端断开 / 请求取消
		case <-heartbeat.C:
			if err := stream.Ping(time.Now()); err != nil {
				return // 写注释心跳失败 = 连接已坏
			}
		case <-poll.C:
			done, payload := pollOrder(c.Param("id"))
			if err := stream.Event("update", payload); err != nil {
				return
			}
			if done {
				return // 命中后 return，SSE 连接随 handler 结束而关闭
			}
		}
	}
}

// pollOrder 是业务查询占位（真实实现查 DB / 缓存）。
func pollOrder(id string) (done bool, payload any) {
	return true, gin.H{"status": "paid"}
}
```

### API 速览

| 调用 | 说明 |
|---|---|
| `sse.New(c) *Writer` | 低层写入器，需手动 `WriteHeaders()` |
| `sse.NewStream(c) *Stream` | 业务层封装，首次写自动写头 |
| `(*Stream).Event(name, payload) error` | 发命名事件（payload 自动 JSON 序列化） |
| `(*Stream).Comment(text) error` / `Ping(at)` | 注释帧 / 标准心跳 |
| `(*Stream).Retry(ms) error` | 下发客户端建议重连间隔 |
| `(*Stream).Context() context.Context` | 请求上下文（监听断开） |

---

## streaming/wssession

通用 WebSocket 桥接层。业务通过 `Handlers{ParseRequest, Run, OnConnect}` 函数式注入，
通过 `PushSink` 推帧；`Options` 控制心跳、超时、缓冲、连接 cap、Origin 白名单。

### 引用

```go
import "github.com/gtkit/streaming/wssession"
```

### 完整示例：首帧订阅 + 循环推送

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/streaming/wssession"
)

func main() {
	r := gin.New()
	r.GET("/orders/query/wsmsg", handleWSMsg)
	_ = r.Run(":8080")
}

// 客户端连接后发的首帧（文本帧）：{"token":"abc123"}
type subscribeReq struct {
	Token string `json:"token"`
}

func handleWSMsg(c *gin.Context) {
	err := wssession.Serve(
		c.Request.Context(), // parent context
		c.Writer,            // http.ResponseWriter
		c.Request,           // *http.Request（用于 Upgrade / Origin 校验）
		wssession.Options{
			AllowedOrigins:     []string{"https://example.com"}, // 空切片 = same-origin
			MaxSessionDuration: 30 * time.Minute,
			PingInterval:       25 * time.Second,
			FirstFrameTimeout:  10 * time.Second,
			ConnCapEnabled:     true,
			ConnCapIPMax:       50, // 单 IP+path 并发上限
			ConnCapKeyMax:      5,  // 单 key+path 并发上限（key 来自 ParseRequest）
			TrustedProxyCount:  1,  // 部署在 1 层可信反代后；0（默认）则忽略 X-Forwarded-For，IP 取自 RemoteAddr
		},
		wssession.Handlers{
			// ParseRequest：解析首帧，返回 (限流key, 业务请求对象, err)。
			// 必须快（只做解析 + 字段校验，不查 DB / 不发网络）。
			ParseRequest: func(ctx context.Context, raw []byte) (key string, req any, err error) {
				var r subscribeReq
				if err := json.Unmarshal(raw, &r); err != nil {
					return "", nil, err // → 下发 error(422) 帧并 close
				}
				if r.Token == "" {
					return "", nil, errors.New("token required")
				}
				// 返回的 key 用于 token 维度连接 cap；返回的 req 原样传给 Run
				return r.Token, r, nil
			},
			// Run：业务推送循环，blocking 调用。通过 sink.Push 推帧；return 即结束连接。
			Run: func(ctx context.Context, req any, sink wssession.PushSink) error {
				r := req.(subscribeReq)
				poll := time.NewTicker(3 * time.Second)
				defer poll.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err() // 客户端断开 / 30min 超时（预期 close）
					case <-poll.C:
						done, payload := pollOrder(r.Token)
						if err := sink.Push(ctx, payload); err != nil {
							return err // ErrSlowConsumer → 下发 error(429) + close
						}
						if done {
							return nil // 正常结束 → normal closure
						}
					}
				}
			},
			// OnConnect 可选：Upgrade 成功后、进 Run 前调一次（连接级 setup / 审计）。
			// OnConnect: func(ctx context.Context, sess *wssession.Session) error { return nil },
		},
	)

	// Serve 已过滤客户端正常断开等预期 close（见 wssession.IsExpectedClose）；
	// 返回 non-nil 即真异常，由调用方决定记录方式（本包不打日志）。
	if err != nil {
		// 例如：logger.Warn("wsmsg serve failed", zap.Error(err))
		_ = err
	}
}

func pollOrder(token string) (done bool, payload any) {
	return true, gin.H{"code": 200, "status": "paid"}
}
```

### 帧协议（对外 JSON schema）

| 时机 | 帧 |
|---|---|
| ParseRequest + 连接 cap 通过后 | `{"event":"subscribed","timestamp":"..."}` |
| 业务推送（`sink.Push` 的 payload） | 由业务 payload 决定（原样 JSON 序列化） |
| 各类错误 / 超时 | `{"event":"error","code":<码>,"reason":"...","timestamp":"..."}` |

错误码：`408` 首帧超时、`415` 非文本帧、`422` 解析失败、`429` 连接超限/慢消费、`500` 内部错误
（常量见 `wssession/errors.go`：`CodeFirstFrameTimeout` / `CodeInvalidParam` / `CodeTooManyConn` 等）。

### 客户端 IP 与可信代理

IP 维度连接 cap 使用的客户端 IP 默认取自传输层 `RemoteAddr`，**忽略客户端可伪造的 `X-Forwarded-For`**。
部署在反向代理（Nginx / 网关）后时，把 `Options.TrustedProxyCount` 设为可信代理跳数，
`wssession` 会从 `X-Forwarded-For` 列表**由右向左**取第 N 跳作为客户端 IP。未配置（0）时，
所有请求按真实 `RemoteAddr` 计入 cap，伪造 XFF 无法绕过上限。

> loop goroutine 内发生 panic 会被恢复并转为 error 经 `Serve` 返回值上抛（不会让进程崩溃，也不会被静默吞没）。

### 关键约束

- `ParseRequest` / `Run` 必填，`OnConnect` 可选；缺必填字段时 `Serve` 返回 `ErrHandlersIncomplete`。
- `Run` 是 blocking 调用，跑在独立 processLoop；**不要**在 `Run` 内 spawn goroutine 后立即 return
  （否则会被当作业务已结束）。需异步处理就在 `Run` 内自己用 errgroup 编排后再 return。
- `sink.Push` 返回 `ErrSlowConsumer`（出站队列满 + 超时）时，业务应 `return` 让 `wssession` 收敛连接。
