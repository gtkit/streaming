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

## 适合什么项目

这是一个**通用、与业务无关的实时推送底座**，适合用 Go + gin 写后端、需要"服务端持续向客户端推状态"的场景：

- **订单 / 支付状态流**：下单后前端订阅，服务端轮询到"已支付 / 已发货"实时推送（本包的原始场景）
- **任务 / 作业进度**：长任务（导出、转码、训练）的进度条与阶段事件推送
- **LLM / AI 流式响应**：逐 token / 逐段下发（SSE 最常见用法）
- **轻量通知 / 看板刷新**：告警、行情、在线状态等单向下行流
- **需要生产级连接治理**：要心跳保活、慢消费者反压、单 IP/token 并发上限、Origin 白名单、会话时长封顶、panic 不崩进程的 WebSocket 长连接

### 不适合 / 过度的场景

- **强双向、复杂房间路由的实时应用**（多人协作、游戏、聊天室广播）：`wssession` 刻意约定"一连接一订阅、客户端首帧后不再发业务帧"，不内置房间 / 广播 / 多路复用，强行套用会别扭——这类需求应选 `melody`、`centrifugo` 或自建 hub。
- **请求-响应式 API**：用普通 HTTP / gRPC 即可，长连接是负担。
- **超大规模扇出广播**（一条消息推百万连接）：本包按"每连接一个独立业务循环"建模，不是为单源海量扇出优化的 pub/sub。

### sse 还是 wssession？

| 你的需求 | 选 |
|---|---|
| 只需服务端→客户端**单向**推送，客户端不发消息 | **sse**（更简单，浏览器原生 `EventSource` + 自动重连） |
| 客户端要先发一帧**订阅参数**（token / 过滤条件），之后服务端单向推 | **wssession**（首帧订阅模型，带连接 cap / 反压治理） |
| 需要浏览器之外的客户端、二进制、或更强的连接控制 | **wssession** |
| 跑在只认 HTTP/1.1 文本流的代理 / 网关后，要尽量少踩坑 | **sse** |

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

	// 2) 周期轮询 + 心跳，直到命中 / 出错 / 客户端断开
	poll := time.NewTicker(3 * time.Second)
	defer poll.Stop()
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return // 客户端断开 / 请求取消
		case <-heartbeat.C:
			// 心跳：注释帧，前端事件回调不触发，仅防代理空闲断开
			if err := stream.Ping(time.Now()); err != nil {
				return // 写心跳失败 = 连接已坏
			}
		case <-poll.C:
			done, payload, err := pollOrder(c.Param("id"))
			if err != nil {
				// 业务出错：发一个 error 事件再结束（事件名自定，注意见下）
				_ = stream.Event("fail", gin.H{"reason": err.Error()})
				return
			}
			if err := stream.Event("update", payload); err != nil {
				return // 客户端断开
			}
			if done {
				// 终态：明确告知客户端可关闭，再 return
				_ = stream.Event("done", gin.H{"status": "paid"})
				return
			}
		}
	}
}

// pollOrder 是业务查询占位（真实实现查 DB / 缓存）。
func pollOrder(id string) (done bool, payload any, err error) {
	return true, gin.H{"status": "pending"}, nil
}
```

这个 demo 用到 4 个**业务自定义**事件名 + 1 个注释帧：

| 名字 | 入口 | 含义 |
|---|---|---|
| `snapshot` | `Event("snapshot", …)` | 首帧当前态 |
| `update` | `Event("update", …)` | 每次状态变化 |
| `done` | `Event("done", …)` | 终态，告知客户端可主动关闭 |
| `fail` | `Event("fail", …)` | 业务出错（**故意不叫 `error`**，见下方注意） |
| —（ping） | `Ping(at)` | 注释帧保活，**不是事件** |

> 名字全由你定，`sse` 包不预设任何事件名。这里业务错误用 `fail` 而非 `error`：浏览器原生 `EventSource` 自带一个连接级 `error` 事件（断线/重连触发），服务端再发 `event: error` 会和它混在同一个回调里难以区分。包提供的 `stream.Error(payload)` 快捷方法发的就是 `event: error`，用原生 `EventSource` 时建议改用自定义名（如 `fail`）。

### API 速览

| 调用 | 说明 |
|---|---|
| `sse.New(c) *Writer` | 低层写入器，需手动 `WriteHeaders()` |
| `sse.NewStream(c) *Stream` | 业务层封装，首次写自动写头 |
| `(*Stream).Event(name, payload) error` | 发命名事件（payload 自动 JSON 序列化） |
| `(*Stream).Comment(text) error` / `Ping(at)` | 注释帧 / 标准心跳 |
| `(*Stream).Retry(ms) error` | 下发客户端建议重连间隔 |
| `(*Stream).Context() context.Context` | 请求上下文（监听断开） |

> **并发安全**：`Stream` 用互斥锁串行化所有写方法，可从不同 goroutine（如心跳 goroutine + 业务 goroutine）并发调用；底层 `Writer` **非并发安全**，多 goroutine 写同一连接请用 `Stream`。

### POST + 请求体发起 SSE（如 LLM 流式响应）

sse 包**不绑定 HTTP 方法**——GET 只是浏览器原生 `EventSource` 的限制（只能 GET、无请求体、无自定义 header）。需要 **POST + body**（如 LLM 对话，大 prompt 放不进 query、也不想进 access log）时，服务端照常用 sse，只把路由换成 `r.POST` 并自己从请求体 bind 参数；客户端改用 `fetch` + `ReadableStream`（或 `@microsoft/fetch-event-source`）。

服务端：

```go
package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/streaming/sse"
)

type chatRequest struct {
	Prompt string `json:"prompt"`
}

func main() {
	r := gin.New()
	r.POST("/chat/stream", streamChat)
	_ = r.Run(":8080")
}

func streamChat(c *gin.Context) {
	// 参数走请求体（不是 query）
	var req chatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	stream := sse.NewStream(c) // 首个事件自动写 SSE 响应头
	for _, chunk := range fakeLLMStream(req.Prompt) {
		if err := stream.Event("token", gin.H{"text": chunk}); err != nil {
			return // 客户端断开，停止推送
		}
	}
	_ = stream.Event("done", gin.H{"finish": "stop"})
}

// fakeLLMStream 占位：真实实现逐 token 读 LLM 流。
func fakeLLMStream(prompt string) []string {
	return []string{"Hello", ", ", "world"}
}
```

浏览器客户端（`fetch` 流式读取 + 手动解析 SSE 帧）：

```js
const resp = await fetch("/chat/stream", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ prompt: "讲个笑话" }),
});

const reader = resp.body.getReader();
const decoder = new TextDecoder();
let buf = "";
while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });

  const frames = buf.split("\n\n"); // SSE 帧以空行分隔
  buf = frames.pop();               // 末尾残帧留到下次
  for (const frame of frames) {
    let event = "message", data = "";
    for (const line of frame.split("\n")) {
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) data = line.slice(5).trim();
    }
    if (data) console.log(event, JSON.parse(data));
  }
}
```

> 用 `fetch` 自己读流会失去浏览器 `EventSource` 自带的断线重连；需要自动重连就用 `@microsoft/fetch-event-source` 这类库，它支持 POST/body/header 且保留重连。

### 前端对接（EventSource）

GET 方式用浏览器原生 `EventSource`，步骤：

1. **建立连接**：`new EventSource(url)`——仅 GET，自动携带同源 cookie，**断线自动重连**（无需手写）。
2. **按事件名监听**：`addEventListener(<name>, …)`，`<name>` 与服务端 `Event(name, …)` 一一对应；没有 `event:` 行的帧走默认 `onmessage`。
3. **收到终态主动关闭**：收到 `done` / `fail` 后调 `es.close()`，否则浏览器会继续自动重连。
4. **连接级错误**走 `onerror`（网络断开 / 服务端关闭）；这与服务端业务事件无关。
5. **心跳无需处理**：服务端 `Ping` 是注释帧，浏览器自动忽略，前端不会收到事件。

```js
// 1) 建立连接：仅 GET、自动带同源 cookie、断线自动重连
const es = new EventSource("/orders/123/stream");

// 2) 按服务端事件名监听
es.addEventListener("snapshot", e => render(JSON.parse(e.data)));
es.addEventListener("update",   e => render(JSON.parse(e.data)));
es.addEventListener("done", e => {           // 3) 终态：渲染后主动关闭，阻止重连
  render(JSON.parse(e.data));
  es.close();
});
es.addEventListener("fail", e => {           // 业务错误（服务端自定义事件名）
  console.error("业务出错", JSON.parse(e.data));
  es.close();
});

// 4) 连接级错误：EventSource 会自动重连，不想重连就 es.close()
es.onerror = () => {
  if (es.readyState === EventSource.CLOSED) console.warn("连接已关闭");
};
```

> **断点续传**：本包的 `Event` 不写 SSE 的 `id:` 字段，因此浏览器重连不会自动带 `Last-Event-ID`。需要续传时，把游标（如最后处理的版本号）放进事件 `data` 里，前端重连时将游标作为 query 参数重新发起请求。

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
			// OnEvent 可选：接入自己的日志/metrics（本包不绑定日志栈）
			OnEvent: func(ctx context.Context, ev wssession.Event) {
				// 例：logger.Warn("ws event", zap.Stringer("type", ev.Type), zap.Error(ev.Err))
				_ = ev
			},
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

### 首帧鉴权

`wssession` 不内置身份鉴权——它是业务无关桥接层。**HTTP 凭据**（`Authorization` header / cookie）应在调 `Serve` 之前的 gin 中间件里验；**首帧里携带的业务 token** 则按下面的分工落地：

- `ParseRequest` 必须**快**（在关键路径上）：只做格式校验 + 提取 token（顺便当 token 维度 connCap 的 key），**不查 DB / 不验签**。
- 需要查库 / 调鉴权服务的**重验证放 `Run` 开头**：失败时先推一帧明确的错误码，再结束连接。

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/streaming/wssession"
)

func main() {
	r := gin.New()
	r.GET("/orders/stream/authed", handleAuthedWS)
	_ = r.Run(":8080")
}

// 首帧（文本帧）：{"token":"<jwt / session token>"}
type authFrame struct {
	Token string `json:"token"`
}

// authedReq 鉴权通过后传给推送循环。
type authedReq struct {
	token  string
	userID string
}

func handleAuthedWS(c *gin.Context) {
	_ = wssession.Serve(
		c.Request.Context(), c.Writer, c.Request,
		wssession.Options{
			FirstFrameTimeout: 10 * time.Second, // 连上不发鉴权帧 → 10s 后 408 + close
			ConnCapEnabled:    true,
			ConnCapIPMax:      50,
			ConnCapKeyMax:     5, // 以 token 为 key：限制单用户并发连接数
		},
		wssession.Handlers{
			// ParseRequest 必须快：只解析 + 提取 token，不查 DB / 不验签。
			ParseRequest: func(_ context.Context, raw []byte) (key string, req any, err error) {
				var f authFrame
				if err := json.Unmarshal(raw, &f); err != nil {
					return "", nil, fmt.Errorf("bad auth frame: %w", err) // → error(422) + close
				}
				if f.Token == "" {
					return "", nil, errors.New("token required")
				}
				return f.Token, authedReq{token: f.Token}, nil // token 同时作为 cap key
			},
			// Run 跑在独立 goroutine：可查 DB / 调鉴权服务做重验证。
			Run: func(ctx context.Context, req any, sink wssession.PushSink) error {
				r := req.(authedReq)

				// —— 鉴权落点：重验证放这里 ——
				userID, err := verifyToken(ctx, r.token)
				if err != nil {
					// 先推一帧明确的 401，再正常结束（见下方说明）
					_ = sink.Push(ctx, gin.H{"event": "error", "code": 401, "reason": "unauthorized"})
					return nil
				}
				r.userID = userID

				// —— 鉴权通过，开始推送 ——
				poll := time.NewTicker(3 * time.Second)
				defer poll.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-poll.C:
						done, payload := loadUserUpdates(r.userID)
						if err := sink.Push(ctx, payload); err != nil {
							return err // ErrSlowConsumer → error(429) + close
						}
						if done {
							return nil
						}
					}
				}
			},
		},
	)
}

// verifyToken 鉴权占位：真实实现验 JWT 签名 / 查 session 存储 / 调鉴权服务。
func verifyToken(_ context.Context, token string) (userID string, err error) {
	if token == "valid-token" {
		return "user-42", nil
	}
	return "", errors.New("invalid or expired token")
}

func loadUserUpdates(userID string) (done bool, payload any) {
	return true, gin.H{"user": userID, "status": "ok"}
}
```

> **为什么鉴权失败要先 `sink.Push` 再 `return nil`**：框架默认把 `Run` 返回的非 sentinel error 统一映射为 `error(500, "internal error")`，客户端拿不到"鉴权失败"的语义。先主动推一帧 `{"code":401,...}` 再 `return nil`（正常结束），客户端就能收到精确的失败原因。若服务端也想记录这次失败，在 `verifyToken` 出错处自行打日志 / metrics 即可。
>
> **配套保护**：`FirstFrameTimeout`（默认 10s）兜底"连上不发鉴权帧"的连接；`ConnCapKeyMax` 以 token 为 key 限制单用户并发连接数；token 走首帧 body 而非 URL query，不会泄漏进 access log。
>
> **握手前鉴权**：若用 cookie / `Authorization` 等 HTTP 凭据，应在调 `Serve` 之前的中间件里验，鉴权失败直接返回 401、根本不 Upgrade；把结果塞进 `c.Request.Context()`，`ParseRequest` / `Run` 都能取到。

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

### 可观测性（OnEvent + 连接数快照）

本包不绑定日志栈，但通过两个口子把运行状态暴露给调用方自行接日志 / metrics：

**`Options.OnEvent`** —— 可选回调，在以下事件发生时被调用（`nil` 则跳过；回调内 panic 会被桥接层 recover）：

| `Event.Type` | 含义 | 关键字段 |
|---|---|---|
| `EventPanic` | 某 loop goroutine 发生 panic | `Err` |
| `EventSlowConsumer` | 出站队列满超时，客户端消费跟不上 | `Err` |
| `EventCapRejected` | 连接被 IP / token cap 拒绝 | `Key`（cap key） |
| `EventAbnormalClose` | 1006 异常断开（无正常 close 握手） | `Err` |

> `OnEvent` 必须**快且非阻塞**（同步参与连接收敛路径），与 `ParseRequest` 同约定。

**`wssession.ConnCapSnapshot() map[string]int64`** —— 返回当前所有活跃 cap key 及其连接数的独立副本快照，供 metrics 拉取 / 运维查询（key 形态 `ip:<ip>:<path>` / `token:<key>:<path>`，归零的 key 不出现）：

```go
for key, n := range wssession.ConnCapSnapshot() {
	metrics.Gauge("ws_active_conns", float64(n), "key", key)
}
```

> 注：1006 异常断开会通过 `OnEvent` 上报 `EventAbnormalClose`，但**不**作为 `Serve` 的错误返回——避免把常见的客户端网络抖动变成调用方的错误误报。

### 关键约束

- `ParseRequest` / `Run` 必填，`OnConnect` 可选；缺必填字段时 `Serve` 返回 `ErrHandlersIncomplete`。
- `Run` 是 blocking 调用，跑在独立 processLoop；**不要**在 `Run` 内 spawn goroutine 后立即 return
  （否则会被当作业务已结束）。需异步处理就在 `Run` 内自己用 errgroup 编排后再 return。
- `sink.Push` 返回 `ErrSlowConsumer`（出站队列满 + 超时）时，业务应 `return` 让 `wssession` 收敛连接。

### 前端对接（WebSocket）

按 `wssession` 的协议约定对接，步骤：

1. **建立连接**：`new WebSocket("wss://…/path")`，生产用 `wss`。浏览器原生 `WebSocket` 不能自定义 header——鉴权 token 走下一步的首帧（或 cookie）。
2. **连上后立即发首帧订阅**（`onopen` 里），文本 JSON，字段与服务端 `ParseRequest` 解析的一致。
3. **解析下行帧**（`onmessage`）：先 `JSON.parse`，按 `event` 分支——`subscribed`（订阅确认）/ `error`（带 `code` + `reason`）/ 其余为业务推送 payload。
4. **首帧后不要再发业务帧**：协议约定"一连接一订阅"，订阅后再发任何帧会被服务端拒（`error` 422 + close）。
5. **心跳无需处理**：服务端定期发 WebSocket Ping 控制帧，浏览器自动回 Pong；前端不用写心跳代码。长时间无数据时连接靠服务端 Ping 保活（`PongWait` 默认 70s）。
6. **关闭与重连**：`onclose` 里区分正常关闭（code 1000）与异常，异常用指数退避重连，**重连后需重新发首帧订阅**。

```js
function connect() {
  const ws = new WebSocket("wss://example.com/orders/query/wsmsg");

  ws.onopen = () => {
    // 2) 首帧订阅（字段对应服务端 ParseRequest）
    ws.send(JSON.stringify({ action: "subscribe", token: "abc123" }));
  };

  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    switch (msg.event) {
      case "subscribed":
        console.log("订阅成功", msg.timestamp);
        break;
      case "error":
        // code: 408 首帧超时 / 415 非文本帧 / 422 解析失败 / 429 超限 / 500 内部错误
        console.error(`服务端错误 ${msg.code}: ${msg.reason}`);
        break;
      default:
        render(msg); // 业务推送 payload
    }
  };

  // 6) 异常关闭 → 指数退避重连（重连后 onopen 会重新发首帧）
  ws.onclose = (e) => {
    if (e.code === 1000) return; // 正常关闭，不重连
    setTimeout(connect, backoff());
  };
  ws.onerror = () => ws.close();
  return ws;
}
```

> **不要**在订阅成功后用 `ws.send` 继续发业务请求帧——本包按"客户端首帧后只收不发"建模，多发会触发服务端 `error(422) + close`。需要双向交互、房间路由的场景见上文「适合什么项目 → 不适合的场景」。
