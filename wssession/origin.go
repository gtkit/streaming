package wssession

import (
	"net/http"
	"strings"
)

// newOriginChecker 返回一个 gorilla/websocket Upgrader.CheckOrigin 函数。
//
// 行为约定:
//   - allowedOrigins 为空切片(或 nil)→ same-origin only(Origin header 必须与请求 Host 匹配,
//     或 Origin 为空也放行——用于非浏览器客户端)
//   - allowedOrigins 含 "*" → 放行所有 Origin(包括非浏览器 / 跨域脚本;仅用于开发)
//   - 其它情况 → 严格白名单(大小写不敏感比对)
func newOriginChecker(allowedOrigins []string) func(r *http.Request) bool {
	// 预处理白名单:小写化 + 去重,避免每个请求重复处理。
	allowList := make(map[string]struct{}, len(allowedOrigins))
	wildcard := false
	for _, o := range allowedOrigins {
		o = strings.ToLower(strings.TrimSpace(o))
		if o == "" {
			continue
		}
		if o == "*" {
			wildcard = true
			break
		}
		allowList[o] = struct{}{}
	}

	return func(r *http.Request) bool {
		origin := strings.ToLower(strings.TrimSpace(r.Header.Get("Origin")))

		// 空 Origin = 非浏览器客户端(curl / 服务端到服务端);always pass
		if origin == "" {
			return true
		}

		// wildcard 放行(开发环境用)
		if wildcard {
			return true
		}

		// 显式白名单:Origin 在列表中
		if _, ok := allowList[origin]; ok {
			return true
		}

		// 空白名单:走 same-origin(Origin 主机部分与请求 Host 匹配)
		if len(allowList) == 0 && !wildcard {
			return sameOrigin(origin, r.Host)
		}

		return false
	}
}

// sameOrigin 比较 Origin URL 的 host 部分与请求 Host header 是否一致。
// 例:Origin=https://example.com:443,Host=example.com:443 → true.
func sameOrigin(origin, host string) bool {
	// Origin 形如 scheme://host[:port],提取 host 部分
	if i := strings.Index(origin, "://"); i >= 0 {
		origin = origin[i+3:]
	}
	// 去掉路径
	if i := strings.IndexByte(origin, '/'); i >= 0 {
		origin = origin[:i]
	}
	return strings.EqualFold(origin, host)
}
