// Package wssession — 通用 WebSocket 桥接层(协议无关 / 业务无关)。
//
// 文件分布:
//   - options.go      配置与默认值
//   - errors.go       sentinel error + error/subscribed 帧 schema
//   - handlers.go     业务注入函数式 hooks(OnConnect / ParseRequest / Run)
//   - pushsink.go     PushSink 接口:业务 → outbox 唯一入口
//   - session.go      Session struct + Serve(lifecycle 编排) + close 路径
//   - readloop.go     readLoop:WS → inbox
//   - processloop.go  processLoop:inbox → ParseRequest → connCap → Run
//   - writeloop.go    writeLoop:outbox → WS(含 Ping 心跳)
//   - outbound.go     outboundMessage + queue 反压
//   - connlimit.go    IP/key 维度连接 cap(分片 mutex 计数表,归零删除 key)
//   - origin.go       Origin 白名单
//
// 完整流程文档见 docs/wsmsg-flow.md。
package wssession

import (
	"context"
	"errors"
	"time"
)

// EventType 标识 wssession 在连接生命周期内上报的事件类别。
type EventType int

const (
	// EventPanic 某 loop goroutine 发生 panic(已被 recover 并转为 error)。
	EventPanic EventType = iota + 1
	// EventSlowConsumer 出站队列满 + QueueOfferTimeout 超时,客户端消费跟不上。
	EventSlowConsumer
	// EventCapRejected 连接被 IP 或 token 维度连接 cap 拒绝。
	EventCapRejected
	// EventAbnormalClose 连接以 1006(无正常 close 握手)异常断开。
	EventAbnormalClose
)

// String 返回事件类型的可读名,便于日志。
func (t EventType) String() string {
	switch t {
	case EventPanic:
		return "panic"
	case EventSlowConsumer:
		return "slow_consumer"
	case EventCapRejected:
		return "cap_rejected"
	case EventAbnormalClose:
		return "abnormal_close"
	default:
		return "unknown"
	}
}

// Event 是 wssession 通过 Options.OnEvent 上报给调用方的生命周期事件。
//
// 字段语义:
//   - Type   事件类别
//   - Reason 人类可读原因文案
//   - Err    关联错误(可能为 nil)
//   - Key    cap 相关事件的 cap key(如 "ip:...:path" / "token:...:path"),其它事件为空
type Event struct {
	Type   EventType
	Reason string
	Err    error
	Key    string
}

// Options 控制 wssession Session 的所有可调行为。
//
// 所有 Duration / 数值字段在 normalizeOptions() 内回退默认值;
// AllowedOrigins 空切片走 same-origin 校验,非空则严格白名单。
type Options struct {
	// AllowedOrigins WebSocket 握手期 Origin 白名单(空切片 = same-origin)。
	AllowedOrigins []string

	// FirstFrameTimeout Upgrade 后无任何 inbound 帧的最大等待。
	// 超时下发 error(408) 帧 + close。默认 10s。
	FirstFrameTimeout time.Duration

	// MaxSessionDuration 单 Session 绝对存活上限(防 fd 长期占用)。默认 30 min。
	MaxSessionDuration time.Duration

	// ReadLimit 单 inbound 帧最大字节数;超出 gorilla 返回 ErrReadLimit。默认 4096。
	ReadLimit int64

	// PingInterval 服务端 Ping 周期。默认 25s。
	PingInterval time.Duration

	// PongWait 无 Pong 后判定连接死亡的最大时长。默认 70s。
	PongWait time.Duration

	// WriteWait 单帧写超时。默认 10s。
	WriteWait time.Duration

	// OutboundBufferSize outbox channel 容量。默认 128。
	OutboundBufferSize int

	// QueueOfferTimeout 业务 sink.Push 入队超时;超时返回 ErrSlowConsumer。默认 5s。
	QueueOfferTimeout time.Duration

	// InboundBufferSize inbox channel 容量;本场景首帧 1 条即够,默认 4 留余量。
	InboundBufferSize int

	// ConnCapEnabled 连接 cap 总开关(false 时两层 cap 透传)。
	ConnCapEnabled bool

	// ConnCapIPMax 单 client_ip + path 同时活跃连接数上限。
	// ConnCapEnabled=true 时必须 > 0。默认 50。
	ConnCapIPMax int

	// ConnCapKeyMax 单 token + path 同时活跃连接数上限(ParseRequest 返回的 key)。
	// ConnCapEnabled=true 时必须 > 0。默认 5。
	ConnCapKeyMax int

	// TrustedProxyCount 信任的反向代理跳数,决定客户端 IP 的取值来源。
	//
	//   - 0(默认):忽略 X-Forwarded-For,客户端 IP 取自 RemoteAddr。
	//     X-Forwarded-For 客户端可任意伪造,默认信任会导致 IP 维度 connCap
	//     被绕过,并放大连接计数表的 key 膨胀,故默认不信任。
	//   - n>0:从 X-Forwarded-For 列表**由右向左**数第 n 跳取客户端 IP
	//     (可信代理把上游地址追加在列表右侧);n 超过列表长度时回退到列表
	//     最左端,列表为空时回退 RemoteAddr。
	//
	// 部署在 Nginx / 网关等反向代理后时,应设为可信代理的跳数。
	TrustedProxyCount int

	// OnEvent 可选的生命周期事件回调,用于接入调用方自己的日志 / metrics。
	//
	// 上报时机:panic / 慢消费者 / 连接 cap 拒绝 / 1006 异常断开(见 EventType)。
	// nil 时桥接层跳过上报。本包不绑定日志栈,事件记录方式由调用方决定。
	//
	// 回调必须**快且非阻塞**(同步调用,会短暂参与连接收敛路径);回调内的 panic
	// 会被桥接层 recover,不影响连接生命周期。
	OnEvent func(ctx context.Context, ev Event)
}

// emit 安全触发 OnEvent:nil 跳过,并 recover 用户回调内的 panic
// (回调 panic 不应影响连接收敛,也不再触发 EventPanic 递归)。
func (o Options) emit(ctx context.Context, ev Event) {
	if o.OnEvent == nil {
		return
	}
	defer func() { _ = recover() }()
	o.OnEvent(ctx, ev)
}

// Validate 校验运行所需的关键参数。
//
// 仅在 Options.ConnCapEnabled=true 时校验两个 cap;其余字段空值由 normalizeOptions 兜底默认。
func (o Options) Validate() error {
	if o.ConnCapEnabled {
		if o.ConnCapIPMax <= 0 {
			return errors.New("wssession: ConnCapIPMax must be > 0 when ConnCapEnabled")
		}
		if o.ConnCapKeyMax <= 0 {
			return errors.New("wssession: ConnCapKeyMax must be > 0 when ConnCapEnabled")
		}
	}
	return nil
}

// normalizeOptions 为调用方未显式配置的字段填充生产可用的默认值。
func normalizeOptions(o Options) Options {
	if o.FirstFrameTimeout <= 0 {
		o.FirstFrameTimeout = 10 * time.Second
	}
	if o.MaxSessionDuration <= 0 {
		o.MaxSessionDuration = 30 * time.Minute
	}
	if o.ReadLimit <= 0 {
		o.ReadLimit = 4096
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 25 * time.Second
	}
	if o.PongWait <= 0 {
		o.PongWait = 70 * time.Second
	}
	if o.WriteWait <= 0 {
		o.WriteWait = 10 * time.Second
	}
	if o.OutboundBufferSize <= 0 {
		o.OutboundBufferSize = 128
	}
	if o.QueueOfferTimeout <= 0 {
		o.QueueOfferTimeout = 5 * time.Second
	}
	if o.InboundBufferSize <= 0 {
		o.InboundBufferSize = 4
	}
	return o
}
