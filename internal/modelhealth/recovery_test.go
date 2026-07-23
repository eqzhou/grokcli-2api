package modelhealth

import (
	"context"
	"testing"
	"time"
)

func TestIsStrictProbeReply(t *testing.T) {
	tests := []struct {
		name string
		text string
		want bool
	}{
		{name: "exact", text: "OK", want: true},
		{name: "surrounding whitespace", text: " \nOK\r\n", want: true},
		{name: "lowercase", text: "ok", want: false},
		{name: "extra prose", text: "OK!", want: false},
		{name: "empty", text: "", want: false},
		{name: "done marker", text: "[DONE]", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStrictProbeReply(tt.text); got != tt.want {
				t.Fatalf("isStrictProbeReply(%q) = %v, want %v", tt.text, got, tt.want)
			}
		})
	}
}

func TestRecoveryBackoff(t *testing.T) {
	tests := []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: 15 * time.Minute},
		{failures: 1, want: 30 * time.Minute},
		{failures: 2, want: time.Hour},
		{failures: 3, want: 3 * time.Hour},
		{failures: 4, want: 6 * time.Hour},
		{failures: 5, want: 12 * time.Hour},
		{failures: 6, want: 12 * time.Hour},
		{failures: 7, want: 12 * time.Hour},
		{failures: 100, want: 12 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.want.String(), func(t *testing.T) {
			if got := recoveryBackoff(tt.failures); got != tt.want {
				t.Fatalf("recoveryBackoff(%d) = %s, want %s", tt.failures, got, tt.want)
			}
		})
	}
}

func TestEvaluateRecoveryModelFailureDoesNotQueryQuota(t *testing.T) {
	quotaCalls := 0
	got := evaluateRecovery(context.Background(), probeStreamResult{
		Outcome:   probeFailure,
		Text:      "",
		Completed: false,
		Terminal:  "response.failed",
	}, func(context.Context) probeOutcome {
		quotaCalls++
		return probeSuccess
	})

	if got != probeFailure {
		t.Fatalf("evaluateRecovery(model failure) = %q, want %q", got, probeFailure)
	}
	if quotaCalls != 0 {
		t.Fatalf("quota verifier called %d times after model failure, want 0", quotaCalls)
	}
}

func TestEvaluateRecoveryModelInconclusiveDoesNotQueryQuota(t *testing.T) {
	quotaCalls := 0
	got := evaluateRecovery(context.Background(), probeStreamResult{Outcome: probeInconclusive}, func(context.Context) probeOutcome {
		quotaCalls++
		return probeSuccess
	})
	if got != probeInconclusive || quotaCalls != 0 {
		t.Fatalf("inconclusive model got=%q quotaCalls=%d", got, quotaCalls)
	}
}

func TestEvaluateRecoveryQuotaTriState(t *testing.T) {
	modelOK := probeStreamResult{
		Outcome:   probeSuccess,
		Text:      "OK",
		Completed: true,
		Terminal:  "completed",
	}
	tests := []struct {
		name  string
		quota probeOutcome
		want  probeOutcome
	}{
		{name: "healthy quota recovers", quota: probeSuccess, want: probeSuccess},
		{name: "exhausted quota rejects", quota: probeFailure, want: probeFailure},
		{name: "unknown quota defers", quota: probeInconclusive, want: probeInconclusive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			got := evaluateRecovery(context.Background(), modelOK, func(context.Context) probeOutcome {
				calls++
				return tt.quota
			})
			if got != tt.want {
				t.Fatalf("evaluateRecovery(quota=%q) = %q, want %q", tt.quota, got, tt.want)
			}
			if calls != 1 {
				t.Fatalf("quota verifier called %d times, want 1", calls)
			}
		})
	}
}

func TestEvaluateRecoveryRejectsNonExactModelOutputBeforeQuota(t *testing.T) {
	quotaCalls := 0
	got := evaluateRecovery(context.Background(), probeStreamResult{
		Outcome:   probeSuccess,
		Text:      "OK, account is healthy",
		Completed: true,
		Terminal:  "completed",
	}, func(context.Context) probeOutcome {
		quotaCalls++
		return probeSuccess
	})

	if got != probeFailure {
		t.Fatalf("evaluateRecovery(non-exact output) = %q, want %q", got, probeFailure)
	}
	if quotaCalls != 0 {
		t.Fatalf("quota verifier called %d times after non-exact output, want 0", quotaCalls)
	}
}
