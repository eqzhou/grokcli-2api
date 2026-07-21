package server

import (
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type loginAttempt struct {
	Failures     int
	WindowStart  time.Time
	BlockedUntil time.Time
}

type loginAttemptLimiter struct {
	mu          sync.Mutex
	attempts    map[string]loginAttempt
	maxFailures int
	window      time.Duration
	block       time.Duration
	maxEntries  int
	now         func() time.Time
}

func newLoginAttemptLimiter(maxFailures int, window, block time.Duration) *loginAttemptLimiter {
	return &loginAttemptLimiter{
		attempts: map[string]loginAttempt{}, maxFailures: maxFailures,
		window: window, block: block, maxEntries: 4096, now: time.Now,
	}
}

func (l *loginAttemptLimiter) Allow(key string) bool {
	key = strings.TrimSpace(key)
	if l == nil || key == "" {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	entry, ok := l.attempts[key]
	if !ok {
		return true
	}
	if !entry.BlockedUntil.IsZero() && now.Before(entry.BlockedUntil) {
		return false
	}
	if now.Sub(entry.WindowStart) >= l.window || (!entry.BlockedUntil.IsZero() && !now.Before(entry.BlockedUntil)) {
		l.attempts = loginAttemptsWithout(l.attempts, key)
	}
	return true
}

func (l *loginAttemptLimiter) RecordFailure(key string) {
	key = strings.TrimSpace(key)
	if l == nil || key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	base := pruneLoginAttempts(l.attempts, now, l.window)
	entry := base[key]
	if entry.WindowStart.IsZero() || now.Sub(entry.WindowStart) >= l.window {
		entry = loginAttempt{WindowStart: now}
	}
	entry.Failures++
	if entry.Failures >= l.maxFailures {
		entry.BlockedUntil = now.Add(l.block)
	}
	if _, exists := base[key]; !exists && len(base) >= l.maxEntries {
		base = loginAttemptsWithout(base, oldestLoginAttemptKey(base))
	}
	next := loginAttemptsCopy(base)
	next[key] = entry
	l.attempts = next
}

func (l *loginAttemptLimiter) Reset(key string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts = loginAttemptsWithout(l.attempts, strings.TrimSpace(key))
}

func loginAttemptsCopy(src map[string]loginAttempt) map[string]loginAttempt {
	dst := make(map[string]loginAttempt, len(src)+1)
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func loginAttemptsWithout(src map[string]loginAttempt, drop string) map[string]loginAttempt {
	dst := make(map[string]loginAttempt, len(src))
	for key, value := range src {
		if key != drop {
			dst[key] = value
		}
	}
	return dst
}

func pruneLoginAttempts(src map[string]loginAttempt, now time.Time, window time.Duration) map[string]loginAttempt {
	dst := make(map[string]loginAttempt, len(src))
	for key, entry := range src {
		activeBlock := !entry.BlockedUntil.IsZero() && now.Before(entry.BlockedUntil)
		activeWindow := !entry.WindowStart.IsZero() && now.Sub(entry.WindowStart) < window
		if activeBlock || activeWindow {
			dst[key] = entry
		}
	}
	return dst
}

func oldestLoginAttemptKey(src map[string]loginAttempt) string {
	oldestKey := ""
	var oldest time.Time
	for key, entry := range src {
		if oldestKey == "" || entry.WindowStart.Before(oldest) {
			oldestKey = key
			oldest = entry.WindowStart
		}
	}
	return oldestKey
}

func loginClientKey(r *http.Request) string {
	if r == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		host = strings.Trim(strings.TrimSpace(r.RemoteAddr), "[]")
	}
	peerIP := net.ParseIP(host)
	if peerIP != nil && trustedProxyIP(peerIP) {
		forwardedChain := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
		for i := len(forwardedChain) - 1; i >= 0; i-- {
			if forwarded := net.ParseIP(strings.TrimSpace(forwardedChain[i])); forwarded != nil && !trustedProxyIP(forwarded) {
				return forwarded.String()
			}
		}
	}
	if peerIP != nil {
		return peerIP.String()
	}
	return host
}

func trustedProxyIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, raw := range strings.Split(os.Getenv("GROK2API_TRUSTED_PROXY_CIDRS"), ",") {
		_, network, err := net.ParseCIDR(strings.TrimSpace(raw))
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}

var (
	adminLoginAttempts = newLoginAttemptLimiter(5, 5*time.Minute, 15*time.Minute)
	adminAuthReadSlots = make(chan struct{}, 32)
	adminLoginSlots    = make(chan struct{}, 4)
)
