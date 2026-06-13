# Changelog

本项目遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)。

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [1.2.0] - 2026-06-13

### Added

- `sse` 新增 `Raw`，用于 `Data(sse.Raw("[DONE]"))` 这类 data-only 帧原样输出场景，不再依赖 JSON 包的 `RawMessage`
- `wssession` 新增 `Options.MaxOutboundFrameBytes` 与 `ErrOutboundFrameTooLarge`，调用方可限制单条业务出站帧序列化后的最大字节数，超限帧不会进入出站队列
- `wssession` 新增 `Options.TurnCloseTimeout` 与 `EventTurnStuck`，双向模式下旧 `OnMessage` 被取消后未及时退出时会上报事件并收敛连接

### Changed

- JSON 依赖迁移到 `github.com/gtkit/json/v2`，README 示例同步移除 `encoding/json`
- README 明确区分 WebSocket 单向订阅模式与双向消息模式的客户端发帧约束，并补充 SSE 显式启动需要写入并 flush 首帧的说明

## [1.1.0] - 2026-06-10

### Added

- `wssession` 新增双向模式：`Handlers.OnMessage` 非 nil 时，单连接支持多轮双向消息（如多轮 LLM 对话）——每条客户端消息触发一轮，**新消息打断上一轮**（cancel 旧轮 turn context 并等其退出后才启动新一轮，任一时刻严格至多一个活跃轮次）；`Run` 在双向模式下变为可选（后台主动推送，错误处置与单向模式一致）。`OnMessage` 为 nil 时单向行为完全不变
- `wssession` 新增 `Options.InboundRatePerSecond` / `InboundRateBurst`：双向模式下单连接入站消息速率限制（令牌桶，标准库实现），超限丢弃、上报事件并下发 `error(429)` 提示（同一连续限速期内仅提示一帧），不关连接
- `wssession` 新增 `EventType` 值 `EventRateLimited`（入站超速）与 `EventTurnInterrupted`（轮次被打断），经 `OnEvent` 上报
- `wssession` 新增连接级操作入口：`Session.Push`（`Session` 直接实现 `PushSink`，持有 `*Session` 即可向该连接推帧）与 `Session.Kick`（下发 `error(409, reason)` + close 1008 踢下线，幂等；客户端收到 409 应提示被顶下线且不自动重连），新增错误码常量 `CodeConflict = 409`
- 新增可选子包 `wssession/sessionhub`：按 userID 管理活跃连接的轻量注册表——`Register` / `List` / `Count` / `Users` / `Total` 枚举元数据，`Conn` 接口（`*wssession.Session` 结构性满足，零 import 解耦）+ `RegisterConn` + `Conns` 支持定向推送与单点登录踢旧连接
- README 新增"优雅停机"说明：`http.Server.Shutdown` 不关闭、不等待被 hijack 的 WebSocket 连接，应以进程级 shutdown ctx 作为 `Serve` 的 parent，停机时取消并经 close 1001 收敛
- `sse` 新增断线续传支持：`Writer.EventWithID` / `Stream.EventWithID` 输出带 `id:` 的事件，包级 `LastEventID(c)` 读取 EventSource 重连携带的 `Last-Event-ID` 头，业务可据此从断点续推
- `sse` 新增 `Writer.Data` / `Stream.Data`：data-only 帧（OpenAI 风格流式块），`RawMessage` 原样透传以支持字面 `data: [DONE]` 终止哨兵；data 含换行时按 SSE 规范自动拆分多 `data:` 行

### Changed

- `wssession` 出帧 JSON 序列化前移到 `Push` 端（业务 goroutine 并行序列化），`writeLoop` 改为纯 IO；统一用 `gtkitjson`，移除对 gorilla `WriteJSON`（内部 `encoding/json`）的依赖。`Push` 现在会在 payload 无法序列化时立即返回错误（签名不变，兼容增强）
- `wssession` 服务端主动关闭现在会完成 WebSocket close 握手：错误关闭在 `error` JSON 帧后追加 close 帧（业务码 `408/409/415/422/429` → close `1008`，`500` → close `1011`），会话超时 / 上游取消（服务端单方面终止，客户端应重连）best-effort 发 close `1001`。客户端不再恒收 `1006`，`1006` 重新成为真实网络异常的信号
- `wssession` 文档明确两条公开契约：`Options.OnEvent` 回调会被多个 goroutine 并发调用、实现必须并发安全；`Handlers` 函数闭包捕获的可变状态会被同一路由所有连接共享，连接级状态应经 `ParseRequest` 返回的 `req` 传递
- `wssession` 升级握手接入共享写缓冲池（`WriteBufferPool`），降低高连接数场景下的每连接常驻内存

### Fixed

- 修复 `sse` 对已断开客户端的延迟发现：每帧写出后改经 `http.ResponseController.Flush()` 冲刷并检查错误，客户端断开当帧报错，不再等到 TCP 缓冲塞满才暴露（LLM 流不会对死连接白推数据）
- 修复 `sse` 事件名可注入伪造帧的隐患：事件名 / id 含换行或 NUL 时返回错误且不写出任何字节
- 修复 `sse` 在 HTTP/2 下发送被禁止的 `Connection: keep-alive` 连接级头部的问题（仅 HTTP/1.x 设置），并补充 `X-Content-Type-Options: nosniff` 防护头
- 修复单向模式 `Run` 返回 nil 后连接悬挂的问题：现在 `wssession` 会 flush 完在途帧、下发 close(1000) 并主动收敛，连接不再空挂到 `MaxSessionDuration`（默认 30 分钟）才释放
- 修复 token 维度连接 cap 提前释放的问题：占用窗口现在与连接真实生命周期对齐，连接存活期间同 token 的新连接不会超额放行
- 修复连接关闭路径在 writeLoop 已退出时固定等满 1 秒兜底的延迟：退出时兑现滞留帧的完成信号，关闭立即返回

## [1.0.0] - 2026-06-02

### Added

- 首个版本：从业务项目下沉的通用实时/流式传输底座
  - `streaming/sse`：基于 gin 的 SSE 写入器，含长连接写超时管理（解除全局 `WriteTimeout` + 每帧 deadline）、`Event` / `Comment` / `Retry` 帧、`Stream` 自动写头封装
  - `streaming/wssession`：通用 WebSocket 桥接层（`Serve` / `Handlers` / `Options` / `PushSink`），含心跳、反压、IP/key 双维度连接 cap、Origin 白名单、首帧超时、最大会话时长、panic 恢复
- `wssession` 新增 `Options.TrustedProxyCount`：配置可信反向代理跳数，用于安全地从 `X-Forwarded-For` 解析客户端 IP（部署在 Nginx / 网关后时设置）
- `wssession` 新增 `Options.OnEvent` 可选回调与 `Event` / `EventType`：上报 panic、慢消费者、连接 cap 拒绝、1006 异常断开事件，供调用方接入自己的日志 / metrics
- `wssession` 新增 `ConnCapSnapshot() map[string]int64`：返回当前所有活跃连接 cap key 及连接数的快照，供 metrics / 运维查询
- `sse` 明确并发契约：`Stream` 并发安全（互斥锁串行化），`Writer` 非并发安全（多 goroutine 写请用 `Stream`）

### Changed

- `wssession` 移除对 `github.com/gtkit/logger` 的直接依赖：内部不再打日志，错误统一通过 `Serve` 返回值上抛，由调用方决定记录方式（符合"库不绑定日志栈"原则）
- `wssession` 连接计数表改为计数归零即删除条目，占用与当前活跃连接数成正比，消除长期运行的内存无界增长
- `wssession` loop goroutine 的 panic 现在会转为 error 经 `Serve` 返回值上抛，不再被静默吞没；`Serve` 在发生 panic 时返回非 nil error
- `wssession` 1006 异常断开（`CloseAbnormalClosure`）不再被 `IsExpectedClose` 无条件视为预期：改为通过 `OnEvent` 上报 `EventAbnormalClose`，但仍不作为 `Serve` 错误返回（避免把客户端网络抖动变成误报）。若此前依赖 `IsExpectedClose(1006) == true` 的调用方需复核

### Security

- `wssession` 默认不再信任客户端可伪造的 `X-Forwarded-For`：客户端 IP 默认取自 `RemoteAddr`，仅在显式配置 `TrustedProxyCount > 0` 时才按可信跳数从 XFF 由右向左解析。修复了伪造 XFF 绕过 IP 维度连接 cap 并放大计数表 key 膨胀的隐患
