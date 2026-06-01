package wssession

import (
	"sync"
	"sync/atomic"
)

// connCounters 是 wssession 包级独立的连接计数器注册表。
//
// 与 internal/middleware/conncap.go 的全局 sync.Map **不共享**——
// 保证 wsmsg 路径与 SSE 路径的 connCap 完全隔离(见 design.md D-3)。
//
// key 形态:
//   - IP 维度:"ip:" + client_ip + ":" + path
//   - token 维度:"token:" + key + ":" + path(key 由 Handlers.ParseRequest 返回)
var connCounters sync.Map // map[string]*atomic.Int64

// tryAcquire 原子自增 key 对应的活跃连接计数。
//
// 采用"先增后判,超限回滚"模式避免 TOCTOU:
//
//	n := cnt.Add(1)
//	if n > max { cnt.Add(-1); return false }
//
// 返回的 current 是回滚前的瞬时值(被拒时 = max+1,放行时 = 实际占用数 ≤ max);仅用于日志。
func tryAcquire(key string, maxConcurrent int) (current int64, ok bool) {
	v, _ := connCounters.LoadOrStore(key, new(atomic.Int64))
	cnt, _ := v.(*atomic.Int64) // LoadOrStore 100% 返回 *atomic.Int64
	n := cnt.Add(1)
	if n > int64(maxConcurrent) {
		cnt.Add(-1)
		return n, false
	}
	return n, true
}

// release 原子自减 key 对应的活跃连接计数。
//
// 应该一一对应 tryAcquire 的**成功**路径;tryAcquire 失败已自己回滚,不应再调 release。
func release(key string) {
	if v, ok := connCounters.Load(key); ok {
		if cnt, typeOk := v.(*atomic.Int64); typeOk {
			cnt.Add(-1)
		}
	}
}
