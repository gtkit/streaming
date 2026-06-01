# Changelog

本项目遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)。

## [Unreleased]

### Added

- 首个版本：从业务项目下沉的通用实时/流式传输底座
  - `streaming/sse`：基于 gin 的 SSE 写入器，含长连接写超时管理（解除全局 `WriteTimeout` + 每帧 deadline）、`Event` / `Comment` / `Retry` 帧、`Stream` 自动写头封装
  - `streaming/wssession`：通用 WebSocket 桥接层（`Serve` / `Handlers` / `Options` / `PushSink`），含心跳、反压、IP/key 双维度连接 cap、Origin 白名单、首帧超时、最大会话时长、panic 恢复

### Changed

- `wssession` 移除对 `github.com/gtkit/logger` 的直接依赖：内部不再打日志，错误统一通过 `Serve` 返回值上抛，由调用方决定记录方式（符合"库不绑定日志栈"原则）
