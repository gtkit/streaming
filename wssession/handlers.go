package wssession

import "context"

// Handlers 由业务侧注入,wssession 通过这些函数完成"协议无关 + 业务无关"。
//
// 设计原则:
//   - 函数式注入而非 interface,业务侧写匿名函数即可,不需要新建 type(详见 design.md D-2)
//   - ParseRequest / Run 必填;OnConnect 可选(nil 时桥接层 skip)
//   - ParseRequest 必须**快**(只做 JSON 解析 + 字段校验,不调 DB / 网络);
//     若需要重操作放到 Run 内,因为 Run 跑在独立的 processLoop 串行段不阻塞 readLoop
type Handlers struct {
	// OnConnect 可选,Upgrade 成功 + 进 lifecycle goroutine 之前调一次。
	//
	// 适用场景:连接级 setup(连接 ID 注册 / 准入审计日志 / 自定义心跳计数器);
	// 当前订单业务不使用此 hook。
	//
	// 返回 error 时 Session 立即 close,不下发任何业务帧。
	OnConnect func(ctx context.Context, sess *Session) error

	// ParseRequest 解析客户端首帧,返回:
	//   - key:用于 token 维度 connCap 计数的 key(空字符串 → 跳过 tokenCap,继续 Run)
	//   - req:业务请求对象,会原样传给 Run(无类型约束,业务自定义)
	//   - err:解析失败 → 桥接层下发 error 帧(code=422)+ close
	//
	// 必填。
	ParseRequest func(ctx context.Context, raw []byte) (key string, req any, err error)

	// Run 业务推送循环。
	//   - 内部通过 sink.Push(payload) 推帧(payload 由 wssession 用 JSON 序列化)
	//   - return nil → 自然结束,wssession 下发 normal closure
	//   - return ErrSlowConsumer → 桥接层下发 error(429) + close
	//   - return 其他 err → 桥接层下发 error(500 / 422 视 sentinel)+ close
	//
	// Run 是 blocking 调用,跑在 processLoop 内;不要在 Run 内 spawn goroutine 后立即 return,
	// 否则桥接层会以为业务已结束。如需异步处理,在 Run 内自己用 errgroup 编排再 return。
	//
	// 必填。
	Run func(ctx context.Context, req any, sink PushSink) error
}

// validate 检查 Handlers 关键字段非 nil。
func (h Handlers) validate() error {
	if h.ParseRequest == nil || h.Run == nil {
		return ErrHandlersIncomplete
	}
	return nil
}
