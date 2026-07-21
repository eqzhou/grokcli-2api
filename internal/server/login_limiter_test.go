package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLoginClientKeyIgnoresUserAgentAndUntrustedForwarding(t *testing.T) {
	t.Setenv("GROK2API_TRUSTED_PROXY_CIDRS", "")
	req := httptest.NewRequest(http.MethodPost, "/admin/api/login", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.Header.Set("User-Agent", "attacker-controlled")
	req.Header.Set("X-Forwarded-For", "198.51.100.5")
	first := loginClientKey(req)
	req.Header.Set("User-Agent", "rotated")
	if second := loginClientKey(req); second != first || second != "203.0.113.10" {
		t.Fatalf("keys first=%q second=%q", first, second)
	}
}

func TestLoginClientKeyUsesForwardedIPOnlyForTrustedProxy(t *testing.T) {
	t.Setenv("GROK2API_TRUSTED_PROXY_CIDRS", "127.0.0.1/32")
	req := httptest.NewRequest(http.MethodPost, "/admin/api/login", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("X-Forwarded-For", "192.0.2.99, 198.51.100.5, 127.0.0.1")
	if got := loginClientKey(req); got != "198.51.100.5" {
		t.Fatalf("client key=%q", got)
	}
}

func TestLoginAttemptLimiterBlocksAndResets(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiter := newLoginAttemptLimiter(3, time.Minute, 5*time.Minute)
	limiter.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if !limiter.Allow("203.0.113.10") {
			t.Fatalf("attempt %d blocked too early", i+1)
		}
		limiter.RecordFailure("203.0.113.10")
	}
	if limiter.Allow("203.0.113.10") {
		t.Fatal("repeated failures should be blocked")
	}
	if !limiter.Allow("203.0.113.11") {
		t.Fatal("one client must not block another")
	}

	limiter.Reset("203.0.113.10")
	if !limiter.Allow("203.0.113.10") {
		t.Fatal("successful authentication should reset failures")
	}
}

func TestLoginAttemptLimiterExpiresBlock(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiter := newLoginAttemptLimiter(1, time.Minute, 5*time.Minute)
	limiter.now = func() time.Time { return now }
	limiter.RecordFailure("203.0.113.10")
	if limiter.Allow("203.0.113.10") {
		t.Fatal("client should be blocked")
	}
	now = now.Add(6 * time.Minute)
	if !limiter.Allow("203.0.113.10") {
		t.Fatal("expired block should be released")
	}
}

func TestLoginAttemptLimiterCapsStoredSources(t *testing.T) {
	limiter := newLoginAttemptLimiter(5, time.Hour, time.Hour)
	limiter.maxEntries = 2
	limiter.RecordFailure("one")
	limiter.RecordFailure("two")
	limiter.RecordFailure("three")
	if len(limiter.attempts) > 2 {
		t.Fatalf("attempt map grew beyond cap: %d", len(limiter.attempts))
	}
}
