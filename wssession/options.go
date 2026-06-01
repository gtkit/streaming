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
//   - connlimit.go    IP/key 维度连接 cap(包级 sync.Map,与 middleware/conncap 不共享)
//   - origin.go       Origin 白名单
//
// 完整流程文档见 docs/wsmsg-flow.md。
package wssession

import (
	"errors"
	"time"
)

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
