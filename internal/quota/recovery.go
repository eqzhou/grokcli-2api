package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hm2899/grokcli-2api/internal/store/postgres"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

type RecoveryOutcome string

const (
	RecoveryQuotaHealthy      RecoveryOutcome = "healthy"
	RecoveryQuotaExhausted    RecoveryOutcome = "exhausted"
	RecoveryQuotaInconclusive RecoveryOutcome = "inconclusive"
)

type RecoveryObservation struct {
	Outcome  RecoveryOutcome
	Snapshot map[string]any
}

func classifyRecoveryQuota(snap map[string]any, headers http.Header) RecoveryOutcome {
	if !truthyMap(snap, "ok") {
		return RecoveryQuotaInconclusive
	}
	if truthyMap(snap, "exhausted") || truthyMap(snap, "auto_disabled") {
		return RecoveryQuotaExhausted
	}
	limit := headerInt64(headers, "x-ratelimit-limit-tokens", "X-RateLimit-Limit-Tokens")
	remaining := headerInt64(headers, "x-ratelimit-remaining-tokens", "X-RateLimit-Remaining-Tokens")
	if limit != nil && remaining != nil && *limit > 0 && *remaining <= 0 {
		return RecoveryQuotaExhausted
	}
	return RecoveryQuotaHealthy
}

// ObserveRecovery performs a current, side-effect-free billing observation.
// The caller invokes it only after a strict model success and passes the model
// response headers so free-token capacity does not require a second chat probe.
func (s *Service) ObserveRecovery(ctx context.Context, auth postgres.AccountAuth, rateHeaders http.Header) RecoveryObservation {
	out := map[string]any{
		"ok":         false,
		"account_id": auth.ID,
		"email":      auth.Email,
		"fetched_at": time.Now().Unix(),
		"source":     "billing_recovery",
	}
	if s == nil || strings.TrimSpace(auth.Token) == "" {
		out["error"] = "missing account token"
		return RecoveryObservation{Outcome: RecoveryQuotaInconclusive, Snapshot: out}
	}
	gc := &grok.Client{BaseURL: s.Upstream}
	headers := gc.Headers(auth.Token, "grok-4.5")
	headers["Accept"] = "application/json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Upstream+"/billing", nil)
	if err != nil {
		out["error"] = err.Error()
		return RecoveryObservation{Outcome: RecoveryQuotaInconclusive, Snapshot: out}
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := s.client().Do(req)
	if err != nil {
		out["error"] = err.Error()
		return RecoveryObservation{Outcome: RecoveryQuotaInconclusive, Snapshot: out}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	out["status_code"] = resp.StatusCode
	if resp.StatusCode >= http.StatusBadRequest {
		out["error"] = fmt.Sprintf("billing HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
		return RecoveryObservation{Outcome: RecoveryQuotaInconclusive, Snapshot: out}
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		out["error"] = "parse billing: " + err.Error()
		return RecoveryObservation{Outcome: RecoveryQuotaInconclusive, Snapshot: out}
	}
	for key, value := range normalizeBilling(raw) {
		if key == "raw" {
			continue
		}
		out[key] = value
	}
	if limit := headerInt64(rateHeaders, "x-ratelimit-limit-tokens", "X-RateLimit-Limit-Tokens"); limit != nil {
		out["tokens_limit"] = *limit
	}
	if remaining := headerInt64(rateHeaders, "x-ratelimit-remaining-tokens", "X-RateLimit-Remaining-Tokens"); remaining != nil {
		out["tokens_remaining"] = *remaining
	}
	outcome := classifyRecoveryQuota(out, rateHeaders)
	if outcome == RecoveryQuotaExhausted {
		out["exhausted"] = true
		if stringFromAny(out["exhaust_reason"]) == "" {
			out["exhaust_reason"] = "remaining token quota is zero"
		}
	}
	return RecoveryObservation{Outcome: outcome, Snapshot: out}
}
