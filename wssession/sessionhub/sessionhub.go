// Package sessionhub 提供一个可选、轻量的连接注册表,用于**识别 / 枚举**同一 userID
// 的多个并发连接(多设备 / 多标签页)。
//
// 它独立于 wssession 核心包:不持有 *wssession.Session、不改核心包行为。
// 集成模式利用 wssession.Serve 是阻塞调用——业务在 handler 里注册并 defer 注销:
//
//	connID, release := hub.Register(userID)
//	defer release()
//	_ = wssession.Serve(ctx, w, r, opts, handlers) // 阻塞至连接结束 → defer 注销
//
// 若 userID 来自首帧(由 Handlers.ParseRequest 解析),用闭包把 release 从 ParseRequest
// 回传到 handler 的 defer 即可。
//
// 本包只做识别 / 枚举,不含踢出 / 定向推送 / 单点登录。
package sessionhub

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Entry 是一个活跃连接的元数据快照。
type Entry struct {
	ConnID      string
	UserID      string
	ConnectedAt time.Time
}

// Registry 是按 userID 索引的活跃连接注册表,并发安全。
//
// 零值不可用,请用 New 创建。
type Registry struct {
	mu     sync.RWMutex
	seq    atomic.Uint64
	byUser map[string]map[string]Entry // userID → connID → Entry
	now    func() time.Time            // 便于测试注入;默认 time.Now
}

// New 创建一个空 Registry。
func New() *Registry {
	return &Registry{
		byUser: make(map[string]map[string]Entry),
		now:    time.Now,
	}
}

// Register 登记一个属于 userID 的活跃连接,返回唯一 connID 与注销函数 release。
//
// 调用方应在连接结束时调用 release(通常 `defer release()`)。release 幂等,
// 多次调用只注销一次。
func (r *Registry) Register(userID string) (connID string, release func()) {
	connID = strconv.FormatUint(r.seq.Add(1), 36)
	entry := Entry{ConnID: connID, UserID: userID, ConnectedAt: r.now()}

	r.mu.Lock()
	conns := r.byUser[userID]
	if conns == nil {
		conns = make(map[string]Entry)
		r.byUser[userID] = conns
	}
	conns[connID] = entry
	r.mu.Unlock()

	var once sync.Once
	release = func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			conns := r.byUser[userID]
			delete(conns, connID)
			if len(conns) == 0 {
				delete(r.byUser, userID)
			}
		})
	}
	return connID, release
}

// List 返回 userID 当前所有活跃连接的元数据(独立副本);无活跃连接时返回 nil。
func (r *Registry) List(userID string) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conns := r.byUser[userID]
	if len(conns) == 0 {
		return nil
	}
	out := make([]Entry, 0, len(conns))
	for _, e := range conns {
		out = append(out, e)
	}
	return out
}

// Count 返回 userID 当前活跃连接数。
func (r *Registry) Count(userID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byUser[userID])
}

// Users 返回当前有活跃连接的所有 userID(顺序不确定)。
func (r *Registry) Users() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.byUser))
	for uid := range r.byUser {
		out = append(out, uid)
	}
	return out
}

// Total 返回全局活跃连接总数。
func (r *Registry) Total() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := 0
	for _, conns := range r.byUser {
		total += len(conns)
	}
	return total
}
