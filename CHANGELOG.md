# Changelog

本项目遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)。

## [Unreleased]

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
