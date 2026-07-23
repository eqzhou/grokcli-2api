package quota

import (
	"net/http"
	"testing"
)

func TestClassifyRecoveryQuota(t *testing.T) {
	tests := []struct {
		name    string
		snap    map[string]any
		headers http.Header
		want    RecoveryOutcome
	}{
		{name: "invalid observation", snap: map[string]any{"ok": false}, want: RecoveryQuotaInconclusive},
		{name: "explicit exhausted", snap: map[string]any{"ok": true, "exhausted": true}, want: RecoveryQuotaExhausted},
		{name: "free billing and strong model", snap: map[string]any{"ok": true, "unlimited_or_free": true, "monthly_limit": 0.0}, want: RecoveryQuotaHealthy},
		{name: "paid remaining", snap: map[string]any{"ok": true, "monthly_limit": 20.0, "remaining": 10.0}, want: RecoveryQuotaHealthy},
		{
			name: "explicit token zero wins",
			snap: map[string]any{"ok": true, "unlimited_or_free": true},
			headers: http.Header{
				"X-Ratelimit-Limit-Tokens":     []string{"1000"},
				"X-Ratelimit-Remaining-Tokens": []string{"0"},
			},
			want: RecoveryQuotaExhausted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyRecoveryQuota(tt.snap, tt.headers); got != tt.want {
				t.Fatalf("classifyRecoveryQuota() = %q, want %q", got, tt.want)
			}
		})
	}
}
