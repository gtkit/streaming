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
| `(*Stream).EventWithID(id, name, payload) error` | 发带 `id:` 的命名事件（断线续传锚点） |
| `(*Stream).Data(payload) error` | data-only 帧（OpenAI 风格，无事件名） |
| `(*Stream).Comment(text) error` / `Ping(at)` | 注释帧 / 标准心跳 |
| `(*Stream).Retry(ms) error` | 下发客户端建议重连间隔 |
| `(*Stream).Context() context.Context` | 请求上下文（监听断开） |
| `sse.LastEventID(c) string` | 读取重连请求的 `Last-Event-ID` 头 |

> **并发安全**：`Stream` 用互斥锁串行化所有写方法，可从不同 goroutine（如心跳 goroutine + 业务 goroutine）并发调用；底层 `Writer` **非并发安全**，多 goroutine 写同一连接请用 `Stream`。
>
> **写入硬化**：事件名 / id 含换行或 NUL 时返回错误（防 SSE 帧注入）；每帧 flush 失败（客户端已断开）当帧报错，不会对死连接持续推送。
>
> **显式启动**：`WriteHeaders` / `Stream.Start` 只设置响应头并解除长连接写截止；客户端要立即感知连接建立时，写一条注释或业务帧（如 `stream.Comment("open")`），该帧会 flush 到客户端。

### 断线续传（id + Last-Event-ID）

`EventSource` 断线后会**自动重连**并把最后收到的事件 id 放进 `Last-Event-ID` 请求头。给事件标上 id、重连时从断点续推：

```go
func handleNotifications(c *gin.Context) {
	stream := sse.NewStream(c)
	since := sse.LastEventID(c) // 重连时为客户端最后收到的 id；首连为空串
	for _, ev := range loadEventsSince(since) {
		if err := stream.EventWithID(ev.ID, "notice", ev); err != nil {
			return
		}
	}
	// ……继续实时推送，每条都带 id
}
```

> 本包只传递断点（`id:` 写入 + `Last-Event-ID` 读取），事件缓存与重放策略由业务实现。

### data-only 帧（OpenAI 风格）

做 LLM 代理 / OpenAI 兼容接口时，流式块是纯 `data:` 行（无事件名）、以字面 `data: [DONE]` 结尾：

```go
for chunk := range llmStream(ctx) {
	if err := stream.Data(chunk); err != nil { // data: {"delta":"..."}
		return
	}
}
_ = stream.Data(sse.Raw("[DONE]")) // data: [DONE]（Raw 原样透传）
```

`Data` 的普通 payload 会用 `github.com/gtkit/json/v2` 序列化；只有 `sse.Raw(...)` 会原样写入 data 行。

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
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	gtkitjson "github.com/gtkit/json/v2"
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

// handleWSMsg 只负责接线：把 Options 与 Handlers 交给 Serve，再处理返回值。
func handleWSMsg(c *gin.Context) {
	err := wssession.Serve(c.Request.Context(), c.Writer, c.Request, wsOptions(), wsHandlers())

	// Serve 已过滤客户端正常断开等预期 close（见 wssession.IsExpectedClose）；
	// 返回 non-nil 即真异常，由调用方决定记录方式（本包不打日志）。
	if err != nil {
		// 例如：logger.Warn("wsmsg serve failed", zap.Error(err))
		_ = err
	}
}

// wsOptions 集中配置：心跳 / 超时 / 连接 cap / Origin / 事件回调。
func wsOptions() wssession.Options {
	return wssession.Options{
		AllowedOrigins:         []string{"https://example.com"}, // 空切片 = same-origin
		MaxSessionDuration:     30 * time.Minute,
		PingInterval:           25 * time.Second,
		FirstFrameTimeout:      10 * time.Second,
		ConnCapEnabled:         true,
		ConnCapIPMax:           50,      // 单 IP+path 并发上限
		ConnCapKeyMax:          5,       // 单 key+path 并发上限（key 来自 ParseRequest）
		TrustedProxyCount:      1,       // 部署在 1 层可信反代后；0（默认）则忽略 X-Forwarded-For，IP 取自 RemoteAddr
		MaxOutboundFrameBytes:  1 << 20, // 单条业务出站帧最大 1 MiB；0 表示不限
		OnEvent:                onWSEvent,
	}
}

// wsHandlers 注入业务逻辑。Handlers 是函数式注入，可直接传具名函数，无需匿名闭包。
func wsHandlers() wssession.Handlers {
	return wssession.Handlers{
		ParseRequest: parseSubscribe,
		Run:          runPush,
		// OnConnect 可选：Upgrade 成功后、进 Run 前调一次（连接级 setup / 审计）。
		// OnConnect: onConnect,
	}
}

// parseSubscribe 解析首帧，返回 (限流 key, 业务请求对象, err)。
// 必须快：只做解析 + 字段校验，不查 DB / 不发网络。
func parseSubscribe(_ context.Context, raw []byte) (key string, req any, err error) {
	var r subscribeReq
	if err := gtkitjson.Unmarshal(raw, &r); err != nil {
		return "", nil, err // → 下发 error(422) 帧并 close
	}
	if r.Token == "" {
		return "", nil, errors.New("token required")
	}
	// 返回的 key 用于 token 维度连接 cap；返回的 req 原样传给 Run
	return r.Token, r, nil
}

// runPush 业务推送循环，blocking 调用。通过 sink.Push 推帧；return 即结束连接。
func runPush(ctx context.Context, req any, sink wssession.PushSink) error {
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
				return err // ErrSlowConsumer / ErrOutboundFrameTooLarge 等
			}
			if done {
				return nil // 正常结束 → normal closure
			}
		}
	}
}

// onWSEvent 接入自己的日志 / metrics（本包不绑定日志栈）。
func onWSEvent(_ context.Context, ev wssession.Event) {
	// 例：logger.Warn("ws event", zap.Stringer("type", ev.Type), zap.Error(ev.Err))
	_ = ev
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
	"errors"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	gtkitjson "github.com/gtkit/json/v2"
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
				if err := gtkitjson.Unmarshal(raw, &f); err != nil {
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
							return err // ErrSlowConsumer / ErrOutboundFrameTooLarge 等
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

### 双向多轮对话（LLM 流式）

默认 wssession 是"订阅后只收不发"的单向模型。要在**单个连接里多轮双向对话**（用户反复发消息、AI 流式回复、可被新消息打断），提供 `Handlers.OnMessage` 即进入**双向模式**：首帧仍由 `ParseRequest` 处理（会话初始化 / 鉴权），其后每条客户端消息触发一轮 `OnMessage`，在独立 goroutine 运行；**新消息到达会 cancel 上一轮的 `turnCtx`（打断正在进行的生成），并等上一轮 goroutine 退出后才启动新一轮**——同一连接任一时刻严格至多一个 `OnMessage` 在运行，被打断的旧轮不会在新轮启动后继续推过期帧。

> 双向模式下 `OnMessage` **必须监听 `turnCtx` 并及时返回**（把它传给 LLM 流式调用），否则打断会阻塞后续消息的调度、连接关闭时会等待其退出——与 `ParseRequest`"必须快"同性质。

入站限速（`InboundRatePerSecond`）触发时，被丢弃的每条消息都会上报 `EventRateLimited` 事件，但**同一连续限速期内只向客户端下发一帧 `error(429)` 提示**（限速恢复后再次超限会重新提示一帧）。双向模式下后台 `Run` 的错误处置与单向模式一致：`ErrSlowConsumer` → `error(429)` + close，其它错误 → `error(500)` + close。

若旧 `OnMessage` 被打断后没有在 `TurnCloseTimeout`（默认 5s）内退出，`wssession` 会上报 `EventTurnStuck` 并收敛连接，避免该连接继续处理新消息；这不能强杀业务 goroutine，所以 `OnMessage` 仍必须监听 `ctx`。

服务端：

```go
package main

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/streaming/wssession"
)

func main() {
	r := gin.New()
	r.GET("/chat", handleChat)
	_ = r.Run(":8080")
}

func handleChat(c *gin.Context) {
	_ = wssession.Serve(c.Request.Context(), c.Writer, c.Request,
		wssession.Options{
			FirstFrameTimeout:    10 * time.Second,
			InboundRatePerSecond: 2, // 单连接每秒最多 2 条用户消息（防刷）
			InboundRateBurst:     3,
		},
		wssession.Handlers{
			// 首帧：会话初始化 / 鉴权（不是对话消息）
			ParseRequest: func(_ context.Context, raw []byte) (key string, req any, err error) {
				return "session-1", nil, nil // 占位：解析鉴权 / 会话 ID
			},
			// 每条用户消息触发一轮；新消息会打断上一轮（turnCtx 被 cancel）
			OnMessage: func(ctx context.Context, raw []byte, sink wssession.PushSink) error {
				prompt := string(raw)
				for token := range llmStream(ctx, prompt) { // 把 ctx 传给 LLM，支持打断
					if err := sink.Push(ctx, gin.H{"event": "token", "text": token}); err != nil {
						return err // ErrSlowConsumer / ctx 取消
					}
				}
				return sink.Push(ctx, gin.H{"event": "done"})
			},
		},
	)
}

// llmStream 占位：真实实现调 LLM 流式 API，并在 ctx 取消时停止生成。
func llmStream(ctx context.Context, prompt string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for _, tok := range []string{"Hello", ", ", "world"} {
			select {
			case <-ctx.Done():
				return // 被新消息打断 / 连接断开
			case ch <- tok:
			}
		}
	}()
	return ch
}
```

浏览器客户端（多轮收发）：

```js
const ws = new WebSocket("wss://example.com/chat");
ws.onopen = () => ws.send(JSON.stringify({ token: "abc" })); // 首帧：会话初始化

let current = "";
ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  switch (msg.event) {
    case "subscribed": sendPrompt("你好"); break; // 订阅确认后开始第一轮
    case "token":      current += msg.text; break; // 累积流式 token
    case "done":       console.log("本轮完成:", current); current = ""; break;
    case "error":      console.error(msg.code, msg.reason); break; // 含 429 限速
  }
};

// 每条用户消息触发一轮；途中再发会打断上一轮生成
function sendPrompt(text) { ws.send(text); }
```

> **与单向模式的关系**：`OnMessage` 为 nil 时行为完全不变（订阅后再发帧仍被拒）。双向模式下 `Run` 可选——若同时提供，作为后台主动推送循环与 `OnMessage` 并存。超过 `InboundRatePerSecond` 的消息会被丢弃并下发 `error(429)`（不关连接），同时通过 `OnEvent` 上报 `EventRateLimited`；打断会上报 `EventTurnInterrupted`。

### 帧协议（对外 JSON schema）

| 时机 | 帧 |
|---|---|
| ParseRequest + 连接 cap 通过后 | `{"event":"subscribed","timestamp":"..."}` |
| 业务推送（`sink.Push` 的 payload） | 由业务 payload 决定（原样 JSON 序列化） |
| 各类错误 / 超时 | `{"event":"error","code":<码>,"reason":"...","timestamp":"..."}` |

错误码：`408` 首帧超时、`409` 被顶下线（`Session.Kick`，客户端**不应**自动重连）、`415` 非文本帧、
`422` 解析失败、`429` 连接超限/慢消费、`500` 内部错误
（常量见 `wssession/errors.go`：`CodeFirstFrameTimeout` / `CodeConflict` / `CodeInvalidParam` / `CodeTooManyConn` 等）。

**关闭语义（WebSocket close 握手）**：服务端主动关闭一律先完成 close 握手再断开——
`Run` 正常结束（返回 nil）→ flush 完在途帧后发 close `1000`；错误关闭 → 先发上表的 `error` JSON 帧，
再发 close 帧（`408/409/415/422/429` → `1008`，`500` → `1011`）；会话超时 / 上游取消（服务端单方面终止，
客户端应重连）→ best-effort 发 close `1001`。客户端 `onclose` 收到 `1006` 即代表真实网络异常（非服务端主动关闭）。

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
| `EventRateLimited` | 双向模式入站消息超过速率限制 | `Reason` |
| `EventTurnInterrupted` | 双向模式上一轮被新消息打断 | `Reason` |
| `EventTurnStuck` | 双向模式上一轮取消后未及时退出 | `Reason` / `Err` |

> `OnEvent` 必须**快且非阻塞**（同步参与连接收敛路径），与 `ParseRequest` 同约定。

**`wssession.ConnCapSnapshot() map[string]int64`** —— 返回当前所有活跃 cap key 及其连接数的独立副本快照，供 metrics 拉取 / 运维查询（key 形态 `ip:<ip>:<path>` / `token:<key>:<path>`，归零的 key 不出现）：

```go
for key, n := range wssession.ConnCapSnapshot() {
	metrics.Gauge("ws_active_conns", float64(n), "key", key)
}
```

> 注：1006 异常断开会通过 `OnEvent` 上报 `EventAbnormalClose`，但**不**作为 `Serve` 的错误返回——避免把常见的客户端网络抖动变成调用方的错误误报。

### 多端会话管理（sessionhub）

同一用户可能有多个并发连接（多设备 / 多标签页）。可选子包 `wssession/sessionhub` 提供按 userID 管理活跃连接的轻量注册表：**枚举元数据**（`List` / `Count` / `Users` / `Total`）、**定向推送 / 踢下线**（`RegisterConn` 登记连接句柄 + `Conns` 枚举操作）。它与核心包零 import 依赖（`*wssession.Session` 结构性满足 `sessionhub.Conn` 接口），集成靠 `Serve` 是阻塞调用——`register → defer release → Serve`：

```go
package main

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/streaming/wssession"
	"github.com/gtkit/streaming/wssession/sessionhub"
)

var hub = sessionhub.New()

func main() {
	r := gin.New()
	r.GET("/ws", handleWS)
	// 运维 / 在线状态：列出某用户当前所有活跃连接
	r.GET("/users/:id/conns", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"count": hub.Count(c.Param("id")),
			"conns": hub.List(c.Param("id")),
		})
	})
	_ = r.Run(":8080")
}

func handleWS(c *gin.Context) {
	// userID 来自首帧 token：用闭包把 release 从 ParseRequest 回传到 handler 的 defer
	var sess *wssession.Session
	var release func()
	defer func() {
		if release != nil {
			release()
		}
	}()

	_ = wssession.Serve(c.Request.Context(), c.Writer, c.Request,
		wssession.Options{},
		wssession.Handlers{
			// OnConnect 拿到 *Session 作为连接句柄（Push / Kick）
			OnConnect: func(_ context.Context, s *wssession.Session) error {
				sess = s
				return nil
			},
			ParseRequest: func(ctx context.Context, raw []byte) (key string, req any, err error) {
				userID := parseUserID(raw)
				// 单点登录：踢掉同 userID 的旧连接（被踢端收到 error 409 + close 1008，不应自动重连）
				for _, old := range hub.Conns(userID) {
					old.Kick(ctx, "logged in elsewhere")
				}
				_, release = hub.RegisterConn(userID, sess) // 登记；Serve 返回（连接结束）时 defer 注销
				return userID, userID, nil
			},
			Run: func(ctx context.Context, _ any, _ wssession.PushSink) error {
				<-ctx.Done()
				return nil
			},
		},
	)
}

func parseUserID(raw []byte) string { return "user-1" } // 占位：解析首帧
```

向某用户的所有在线端**定向推送**（任意服务端代码处）：

```go
for _, conn := range hub.Conns("user-1") {
	_ = conn.Push(ctx, gin.H{"event": "notice", "text": "您有新的订单"})
}
```

> **多端策略由业务决定**：允许多端就不踢（只 `RegisterConn` 不 `Kick`）；限制端数就先 `Conns` 检查再决定。`Conns` 返回快照，必须在循环里调 `Kick`（它同步等出帧 flush）——不要在持有自己锁的临界区内调用。
> 只需要枚举不需要操作时，继续用 `Register`（无句柄，`Conns` 不含这类条目）。

> **握手前鉴权场景更简单**：若 userID 在调 `Serve` 前已知（中间件鉴权放进 ctx），直接 `_, release := hub.Register(userID); defer release()` 再 `Serve(...)` 即可，无需闭包。
>
> **SSE 侧**：`sse` 没有连接生命周期 hook，多端识别需业务在 handler 里自行 `hub.Register` + `defer release()`（Stream 的请求 handler 本身也是阻塞的，模式相同）。
>
> **出帧序列化**：`PushSink.Push` 的 payload 在**业务 goroutine 侧**用 `gtkitjson` 序列化（可并行），`writeLoop` 只做纯 IO——大 payload 不阻塞出帧管道。因此 `Push` 现在会在 payload **无法序列化时立即返回错误**（如含 channel 字段）。

### 关键约束

- `ParseRequest` / `Run` 必填，`OnConnect` 可选；缺必填字段时 `Serve` 返回 `ErrHandlersIncomplete`。
- `Run` 是 blocking 调用，跑在独立 processLoop；**不要**在 `Run` 内 spawn goroutine 后立即 return
  （否则会被当作业务已结束）。需异步处理就在 `Run` 内自己用 errgroup 编排后再 return。
- 单向模式下 `Run` 返回 nil 即视为业务结束：`wssession` 会 flush 完在途帧、下发 close(1000) 并主动关闭连接。
- **Handlers 闭包不要捕获可变状态**：同一个 `Handlers` 值复用于多次 `Serve`（如提升为包级变量）时，
  闭包捕获的可变状态会被该路由**所有连接（所有用户）共享**——既是数据竞争也是用户间串台。
  连接级状态经 `ParseRequest` 返回的 `req` 传递，或像本文示例一样在每次请求内现场构造 `Handlers`。
- `Options.OnEvent` 回调会被多个 goroutine 并发调用，实现必须并发安全且快速非阻塞。
- `sink.Push` 返回 `ErrSlowConsumer`（出站队列满 + 超时）时，业务应 `return` 让 `wssession` 收敛连接。
- `sink.Push` 返回 `ErrOutboundFrameTooLarge` 时，该业务帧没有入队；生产环境建议按协议设置 `MaxOutboundFrameBytes`。
- 双向模式的 `OnMessage` 必须监听 `ctx`；否则 `TurnCloseTimeout` 到期后会触发 `EventTurnStuck` 并关闭连接。

### 优雅停机

**陷阱**：`http.Server.Shutdown` 对被 hijack 的连接（WebSocket 正是）**既不关闭也不等待**，且 hijack 之后
`r.Context()` 不会因 Shutdown 取消——若按上文示例把 `c.Request.Context()` 直接当 parent 传给 `Serve`，
停机时所有 WS 会话会一直挂到 `MaxSessionDuration`（默认 30 分钟）。

**正确接法**：用进程级 shutdown ctx 作为 `Serve` 的 parent。停机时 cancel，所有会话经既有收敛路径
向客户端发 close `1001`（GoingAway，客户端据此重连到新实例）并释放：

```go
shutdownCtx, shutdown := context.WithCancel(context.Background())

r.GET("/ws", func(c *gin.Context) {
	// parent 同时尊重停机信号与单请求生命周期
	ctx, cancel := context.WithCancel(shutdownCtx)
	defer cancel()
	context.AfterFunc(c.Request.Context(), cancel) // 客户端断开也收敛

	_ = wssession.Serve(ctx, c.Writer, c.Request, opts, handlers)
})

// 停机流程：先 shutdown() 收敛 WS 会话，再 srv.Shutdown(ctx) 处理普通 HTTP 请求
```

### 前端对接（WebSocket）

#### 单向订阅模式

按 `wssession` 的协议约定对接，步骤：

1. **建立连接**：`new WebSocket("wss://…/path")`，生产用 `wss`。浏览器原生 `WebSocket` 不能自定义 header——鉴权 token 走下一步的首帧（或 cookie）。
2. **连上后立即发首帧订阅**（`onopen` 里），文本 JSON，字段与服务端 `ParseRequest` 解析的一致。
3. **解析下行帧**（`onmessage`）：先 `JSON.parse`，按 `event` 分支——`subscribed`（订阅确认）/ `error`（带 `code` + `reason`）/ 其余为业务推送 payload。
4. **首帧后不要再发业务帧**：单向模式（`Handlers.OnMessage == nil`）协议约定"一连接一订阅"，订阅后再发任何帧会被服务端拒（`error` 422 + close）。
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

> **单向模式约束**：`Handlers.OnMessage == nil` 时，订阅成功后不要用 `ws.send` 继续发业务请求帧；多发会触发服务端 `error(422) + close`。

#### 双向消息模式

`Handlers.OnMessage != nil` 时，首帧仍是订阅 / 鉴权帧；收到 `subscribed` 后可以继续发送业务消息。每条业务消息触发一轮 `OnMessage`，新消息会打断上一轮。客户端应把 `error(429)` 当作限速提示；收到 `error(500)` 或 close 后按业务策略重连并重新发送首帧。
