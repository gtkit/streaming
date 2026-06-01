# SSE Package

`internal/pkg/sse` 提供项目内部统一的 SSE 输出工具。

它的目标不是做一个“覆盖所有 SSE 场景的超大框架”，而是解决当前项目里最常见的几个问题：

1. 统一写 `text/event-stream` 响应头
2. 统一写 `event/data` 帧
3. 统一处理“首次输出时才真正开始响应”
4. 统一暴露 `ping / error / retry / comment` 这些常见 SSE 辅助能力
5. 让不同业务模块共享一套稳定的 SSE 写法

当前项目里已经有两个真实使用场景：

- `internal/module/llm/transport/http`
- `internal/module/order/transport/http`

对应接入文档：

- [LLM STREAM_CHAT_SSE](../../module/llm/STREAM_CHAT_SSE.md)
- [ORDER STATUS STREAM](../../module/order/ORDER_STATUS_STREAM.md)

对应最小示例：

- [examples/llm_sse_client/main.go](../../../examples/llm_sse_client/main.go)
- [examples/order_status_sse_client/main.go](../../../examples/order_status_sse_client/main.go)

## 1. 和 `gin.Context.SSEvent()` 相比，这个包更好吗

结论先说：

- **不是绝对更好**
- 但对当前这个项目来说，**更合适**

### 1.1 `c.SSEvent()` 的优点

Gin 自带的 `c.SSEvent()` 很直接：

- 学习成本低
- 适合临时写一个简单 SSE 接口
- 对“快速发一条事件”很方便

如果你的需求只是：

- 手写几行
- 发几个事件
- 没有复用需求

那直接用 `c.SSEvent()` 完全可以。

### 1.2 当前这个包比 `c.SSEvent()` 更适合的原因

当前项目的问题不是“能不能发 SSE”，而是：

- 多个模块都在发 SSE
- 需要统一响应头
- 需要统一自动起流逻辑
- 需要统一 `Started()` 判断
- 需要统一 `ping / error / retry / comment`
- 需要把 SSE 作为一个可复用内部组件，而不是每个模块手写一套

而 `gin.Context.SSEvent()` 只解决了“写一条 SSE event”这个最小问题，并没有提供：

1. 自动起流  
   你仍然要自己决定什么时候写 Header。

2. started 状态  
   像 `llm` 这种场景，需要知道“响应是否已经开始”，以决定还能不能回普通 JSON。

3. 标准业务辅助方法  
   比如 `Ping()`、`Error()`、`Retry()`、`Comment()`。

4. 项目内统一抽象  
   每个业务模块还是会写出各自不同风格的 SSE 代码。

所以对这个项目来说，当前 `pkg/sse` 的价值在于：

- 不是简单包装 Gin
- 而是提供一层“项目内部统一 SSE 语义”

## 2. 包结构

当前主要有两层：

### 2.1 `Writer`

文件：

- [writer.go](./writer.go)

职责：

- 最底层 SSE 帧写入
- 写响应头
- 写 `event/data`
- 写 `comment`
- 写 `retry`

适合：

- 你需要完全自己控制输出顺序
- 你明确知道什么时候开始响应

### 2.2 `Stream`

文件：

- [stream.go](./stream.go)

职责：

- 在 `Writer` 之上增加一层业务友好的能力
- 首次输出时自动写 Header
- 提供 `Started()` 状态
- 提供 `Ping()` / `Error()` / `Comment()` / `Retry()` 这些标准辅助方法

适合：

- 大多数业务 SSE 场景
- 比如订单状态流、LLM 流式输出

当前建议：

- **业务代码优先使用 `Stream`**
- 只有极少数需要完全手控底层帧顺序的场景，才直接用 `Writer`

## 3. 快速开始

### 3.1 最小示例

```go
package demo

import (
    "net/http"
    "time"

    ssepkg "workai_status_api/internal/pkg/sse"

    "github.com/gin-gonic/gin"
)

func StreamDemo(c *gin.Context) {
    stream := ssepkg.NewStream(c)

    // 首次 Event 会自动写 text/event-stream 头
    if err := stream.Event("status", gin.H{
        "status": "pending",
    }); err != nil {
        return
    }

    if err := stream.Ping(time.Now()); err != nil {
        return
    }

    if err := stream.Event("status", gin.H{
        "status": "done",
        "final":  true,
    }); err != nil {
        return
    }
}
```

## 4. API 说明

### 4.1 `NewStream(c)`

创建一个业务层友好的 SSE 输出器。

```go
stream := sse.NewStream(c)
```

### 4.2 `stream.Event(name, payload)`

发送具名事件。

```go
_ = stream.Event("status", gin.H{
    "status": "pending",
})
```

输出类似：

```text
event: status
data: {"status":"pending"}

```

### 4.3 `stream.Ping(at)`

发送标准保活注释帧。

```go
_ = stream.Ping(time.Now())
```

输出类似：

```text
: ping 2026-03-30T10:00:00Z

```

### 4.4 `stream.Error(payload)`

发送标准业务错误事件。

```go
_ = stream.Error(gin.H{
    "error": "order not found",
})
```

输出类似：

```text
event: error
data: {"error":"order not found"}

```

注意：

- 这里是**服务端业务事件**
- 不是浏览器 `EventSource.onerror` 那个网络层错误回调

### 4.5 `stream.Comment(text)`

发送一条 SSE 注释帧。

```go
_ = stream.Comment("keepalive")
```

输出类似：

```text
: keepalive

```

用途：

- 保活
- 调试
- 某些代理环境下防止长时间空闲断开

### 4.6 `stream.Retry(milliseconds)`

发送 SSE 的 `retry` 指令，提示客户端后续重连间隔。

```go
_ = stream.Retry(3000)
```

输出类似：

```text
retry: 3000

```

用途：

- 给浏览器原生 `EventSource` 提供重连节奏建议

### 4.7 `stream.Started()`

判断当前响应是否已经开始输出。

这个方法在“业务失败时还想回普通 JSON”的场景里很有用。

例如：

```go
if err != nil {
    if !stream.Started() {
        // 还能回普通 JSON
        c.JSON(http.StatusBadRequest, ...)
        return
    }
    // 已经开始 SSE 输出，只能继续发 SSE error
    _ = stream.Error(gin.H{"error": "internal error"})
    return
}
```

## 5. 当前项目里的两个真实模式

### 5.1 LLM 流式输出

文件：

- [stream_responder.go](../../module/llm/transport/http/stream_responder.go)

特点：

- 先发 `session`
- 中间连续发 `chunk`
- 结束时发 `done`
- 出错时发 `error`

适合：

- 文本 token/delta 持续输出

### 5.2 订单状态流

文件：

- [handler.go](../../module/order/transport/http/handler.go)

特点：

- 先发一条 `status(snapshot)`
- 后续发 `status(push/poll)`
- 定期发注释心跳
- 异常时发 `error`
- 进入终态后主动结束流

适合：

- 单向状态推送
- 客户端“等一个结果”

## 6. 推荐实践

### 6.1 业务层统一使用 `Stream`

推荐：

```go
stream := sse.NewStream(c)
_ = stream.Event("status", payload)
```

不推荐每个模块都自己手写：

- `WriteHeaders`
- `started` 标志
- 心跳格式
- `error` 格式

### 6.2 事件名保持稳定

SSE 的事件名本质上就是客户端协议的一部分。

一旦对外开放，尽量不要频繁改：

- `status`
- `ping`
- `error`
- `chunk`
- `done`

### 6.3 `error` 事件和网络错误分开处理

必须区分两种错误：

1. 服务端发出的 `event: error`
2. 浏览器/客户端自己的连接错误回调

这两者不是一个层级。

### 6.4 终态后主动结束流

如果业务天然有终态：

- 订单 `delivered/closed/...`
- LLM `done`

建议服务端在终态后结束流，客户端也主动关闭连接。

### 6.5 不要把双向交互硬塞进 SSE

SSE 只适合：

- 服务端 -> 客户端

如果你的业务需要：

- 客户端持续发消息
- 双向实时交互
- 房间/广播/多人会话

那应该考虑 WebSocket，而不是继续堆 SSE。

## 7. 测试

当前公共层测试：

- [writer_test.go](./writer_test.go)
- [stream_test.go](./stream_test.go)

覆盖点包括：

- 写头
- 普通事件
- context cancel
- 自动起流
- ping 注释心跳
- error
- comment
- retry

## 8. 结论

如果你只是临时写一个简单 SSE 接口：

- `gin.Context.SSEvent()` 足够

如果你要在这个项目里持续维护多个 SSE 业务流：

- 当前 `internal/pkg/sse` 更合适

因为它已经把：

- 起流
- 事件输出
- 错误输出
- 保活
- started 判断
- 重连建议

这些项目内的公共语义统一起来了。
