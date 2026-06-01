package wssession

import "errors"

// Sentinel errors — 桥接层和业务层共同识别的错误标记。
var (
	// ErrSlowConsumer outbox 入队 5s 超时,客户端消费速度跟不上推送速度。
	// 业务侧 Run 收到此 error 时应 return,wssession 收敛并下发 error(429) 帧 + close。
	ErrSlowConsumer = errors.New("wssession: slow consumer (outbox queue full)")

	// ErrFirstFrameTimeout Upgrade 后 Options.FirstFrameTimeout 内无任何 inbound 帧。
	// 由 processLoop 在 timer 触发时主动 close。
	ErrFirstFrameTimeout = errors.New("wssession: first frame timeout")

	// ErrInvalidFrame inbound 帧类型非 TextMessage 或大小超 ReadLimit。
	ErrInvalidFrame = errors.New("wssession: invalid frame")

	// ErrUnexpectedFrame 已订阅状态下收到的额外业务帧(协议约定:首帧后不应再发)。
	ErrUnexpectedFrame = errors.New("wssession: unexpected frame after subscribed")

	// ErrHandlersIncomplete Handlers 缺必填字段(ParseRequest / Run 为 nil)。
	ErrHandlersIncomplete = errors.New("wssession: handlers.ParseRequest and Run are required")
)

// 错误码常量 — 与 docs/wsmsg-flow.md §5 错误码映射表对齐。
const (
	CodeFirstFrameTimeout = 408
	CodeInvalidFrameType  = 415
	CodeInvalidParam      = 422
	CodeTooManyConn       = 429
	CodeInternal          = 500
)

const (
	maxErrorReasonLen = 256

	ReasonFirstFrameTimeout      = "first frame timeout"
	ReasonBinaryFrameUnsupported = "binary frame not supported"
	ReasonUnexpectedFrame        = "unexpected frame after subscribed"
	ReasonTooManyIPConn          = "too many concurrent connections from this ip"
	ReasonTooManyTokenConn       = "too many concurrent connections for this token"
	ReasonSlowConsumer           = "slow consumer"
	ReasonInternalError          = "internal error"
)

// errorFrame 是服务端下发给客户端的错误事件载体(对外 JSON schema 契约)。
//
// JSON 形态:{"event":"error","code":422,"reason":"...","timestamp":"..."}.
type errorFrame struct {
	Event     string `json:"event"`
	Code      int    `json:"code"`
	Reason    string `json:"reason"`
	Timestamp string `json:"timestamp"`
}

// subscribedFrame 是服务端在 ParseRequest + tokenCap 通过后立即下发的订阅确认帧。
//
// JSON 形态:{"event":"subscribed","timestamp":"..."}.
type subscribedFrame struct {
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}
