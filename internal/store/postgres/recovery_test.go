package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestValidateRecoveryApplyInputRequiresStrongCurrentEvidence(t *testing.T) {
	valid := RecoveryApplyInput{
		AccountID:              "account-1",
		Model:                  "grok-4.5",
		ExpectedAccountVersion: 4,
		ExpectedPoolVersion:    7,
		Outcome:                RecoveryOutcomeSuccess,
		Probe: map[string]any{
			"outcome":         "success",
			"probe_status":    "ok",
			"has_output_text": true,
			"terminal_event":  "completed",
			"output_text":     "OK",
		},
		Quota: map[string]any{"ok": true, "exhausted": false},
	}
	if err := validateRecoveryApplyInput(valid); err != nil {
		t.Fatalf("valid recovery rejected: %v", err)
	}

	cases := []struct {
		name string
		edit func(*RecoveryApplyInput)
	}{
		{"missing model", func(in *RecoveryApplyInput) { in.Model = "" }},
		{"missing output", func(in *RecoveryApplyInput) { in.Probe["has_output_text"] = false }},
		{"non exact output", func(in *RecoveryApplyInput) { in.Probe["output_text"] = "OK!" }},
		{"failed terminal", func(in *RecoveryApplyInput) { in.Probe["terminal_event"] = "failed" }},
		{"inconclusive probe", func(in *RecoveryApplyInput) { in.Probe["probe_status"] = "inconclusive" }},
		{"quota failed", func(in *RecoveryApplyInput) { in.Quota["ok"] = false }},
		{"quota exhausted", func(in *RecoveryApplyInput) { in.Quota["exhausted"] = true }},
		{"missing version", func(in *RecoveryApplyInput) { in.ExpectedPoolVersion = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := valid
			in.Probe = copyRecoveryTestMap(valid.Probe)
			in.Quota = copyRecoveryTestMap(valid.Quota)
			tc.edit(&in)
			if err := validateRecoveryApplyInput(in); err == nil {
				t.Fatal("invalid recovery was accepted")
			}
		})
	}
}

func TestValidateRecoveryApplyInputAllowsNonSuccessWithoutRecoveryEvidence(t *testing.T) {
	for _, outcome := range []RecoveryOutcome{RecoveryOutcomeFailure, RecoveryOutcomeInconclusive} {
		in := RecoveryApplyInput{
			AccountID: "account-1", ExpectedAccountVersion: 2, ExpectedPoolVersion: 3,
			Outcome: outcome, Probe: map[string]any{"probe_status": string(outcome)},
		}
		if err := validateRecoveryApplyInput(in); err != nil {
			t.Fatalf("outcome %s rejected: %v", outcome, err)
		}
	}
}

func TestValidateRecoveryApplyInputRejectsMissingProbeForMiss(t *testing.T) {
	in := RecoveryApplyInput{
		AccountID: "account-1", ExpectedAccountVersion: 2, ExpectedPoolVersion: 3,
		Outcome: RecoveryOutcomeFailure,
	}
	if err := validateRecoveryApplyInput(in); err == nil {
		t.Fatal("missing probe observation was accepted")
	}
}

func TestRecoveryBackoffIsBounded(t *testing.T) {
	want := []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour}
	for i, expected := range want {
		if got := recoveryBackoff(i); got != expected {
			t.Fatalf("failure count %d: got %s want %s", i, got, expected)
		}
	}
}

func TestQuotaCooldownCodesAreExplicit(t *testing.T) {
	for _, code := range []string{"free-usage", "free_usage", "subscription:free-usage-exhausted", "billing", "billing_quota", "quota", "quota_exhausted"} {
		if !isRecoveryQuotaCooldownCode(code) {
			t.Fatalf("expected quota cooldown code %q", code)
		}
	}
	for _, code := range []string{"manual", "rate_limit", "auth", "server_error", "not-quota-but-contains-word"} {
		if isRecoveryQuotaCooldownCode(code) {
			t.Fatalf("non-quota cooldown code %q accepted", code)
		}
	}
}

func TestApplyRecoveryDecisionCASAndScopedClearingIntegration(t *testing.T) {
	conn := testConnector(t)
	ctx := context.Background()
	accountID := fmt.Sprintf("recovery-it-%d", time.Now().UnixNano())
	_, err := conn.Pool.Exec(ctx, `
		INSERT INTO accounts (id, email, payload)
		VALUES ($1, 'recovery@example.invalid', '{"key":"test-token"}'::jsonb)`, accountID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.Pool.Exec(context.Background(), `DELETE FROM account_pool WHERE account_id = $1`, accountID)
		_, _ = conn.Pool.Exec(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
	})
	_, err = conn.Pool.Exec(ctx, `
		INSERT INTO account_pool (
			account_id, enabled, disabled_for_quota, pool_status, cooldown_until,
			cooldown_code, cooldown_count, blocked_models, admin_locked
		) VALUES (
			$1, false, true, 'quota_disabled', now() + interval '1 hour',
			'free-usage', 2, '{"grok-4.5":true,"grok-3":true}'::jsonb, false
		)
		ON CONFLICT (account_id) DO UPDATE SET
			enabled=false, disabled_for_quota=true, pool_status='quota_disabled',
			cooldown_until=now() + interval '1 hour', cooldown_code='free-usage',
			cooldown_count=2, blocked_models='{"grok-4.5":true,"grok-3":true}'::jsonb,
			admin_locked=false`, accountID)
	if err != nil {
		t.Fatal(err)
	}
	var accountVersion, poolVersion int64
	if err := conn.Pool.QueryRow(ctx, `
		SELECT a.row_version, ap.row_version
		FROM accounts a JOIN account_pool ap ON ap.account_id=a.id
		WHERE a.id=$1`, accountID).Scan(&accountVersion, &poolVersion); err != nil {
		t.Fatal(err)
	}

	result, err := conn.ApplyRecoveryDecision(ctx, RecoveryApplyInput{
		AccountID: accountID, Model: "grok-4.5",
		ExpectedAccountVersion: accountVersion, ExpectedPoolVersion: poolVersion,
		Outcome: RecoveryOutcomeSuccess,
		Probe: map[string]any{
			"outcome": "success", "probe_status": "ok",
			"has_output_text": true, "terminal_event": "completed", "output_text": "OK",
		},
		Quota: map[string]any{"ok": true, "exhausted": false, "source": "billing_recovery"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied || !result.Recovered || result.PoolStatus != "model_blocked" {
		t.Fatalf("unexpected result: %#v", result)
	}
	var enabled, quotaDisabled bool
	var cooldownUntil *time.Time
	var cooldownCode *string
	var blockedBytes []byte
	if err := conn.Pool.QueryRow(ctx, `
		SELECT enabled, disabled_for_quota, cooldown_until, cooldown_code, blocked_models
		FROM account_pool WHERE account_id=$1`, accountID).Scan(
		&enabled, &quotaDisabled, &cooldownUntil, &cooldownCode, &blockedBytes,
	); err != nil {
		t.Fatal(err)
	}
	blocked := decodeMap(blockedBytes)
	if !enabled || quotaDisabled || cooldownUntil != nil || cooldownCode != nil {
		t.Fatalf("state not recovered: enabled=%v quota=%v until=%v code=%v", enabled, quotaDisabled, cooldownUntil, cooldownCode)
	}
	if blocked["grok-4.5"] != nil || blocked["grok-3"] == nil {
		t.Fatalf("model blocks were not scoped: %#v", blocked)
	}

	stale, err := conn.ApplyRecoveryDecision(ctx, RecoveryApplyInput{
		AccountID: accountID, Model: "grok-4.5",
		ExpectedAccountVersion: accountVersion, ExpectedPoolVersion: poolVersion,
		Outcome: RecoveryOutcomeSuccess,
		Probe: map[string]any{
			"outcome": "success", "probe_status": "ok",
			"has_output_text": true, "terminal_event": "completed", "output_text": "OK",
		},
		Quota: map[string]any{"ok": true, "exhausted": false},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stale.Applied || stale.Status != "stale_or_protected" {
		t.Fatalf("stale CAS unexpectedly applied: %#v", stale)
	}
}

func TestRecoverableDisableNeverClearsAdminLockIntegration(t *testing.T) {
	conn := testConnector(t)
	ctx := context.Background()
	accountID := fmt.Sprintf("recovery-lock-it-%d", time.Now().UnixNano())
	if _, err := conn.Pool.Exec(ctx, `
		INSERT INTO accounts (id, email, payload)
		VALUES ($1, 'locked@example.invalid', '{"key":"test-token"}'::jsonb)`, accountID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.Pool.Exec(context.Background(), `DELETE FROM account_pool WHERE account_id = $1`, accountID)
		_, _ = conn.Pool.Exec(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
	})
	if _, err := conn.SetAccountAdminLocked(ctx, accountID, true, "investigation"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.DisableAccountRecoverable(ctx, accountID, "HTTP 401", "model_health"); err != nil {
		t.Fatal(err)
	}
	var locked bool
	var reason *string
	if err := conn.Pool.QueryRow(ctx, `SELECT admin_locked, disabled_reason FROM account_pool WHERE account_id=$1`, accountID).Scan(&locked, &reason); err != nil {
		t.Fatal(err)
	}
	if !locked || reason == nil || *reason != "investigation" {
		t.Fatalf("automated disable changed admin lock: locked=%v reason=%v", locked, reason)
	}
}

func TestManualLiveClearsAdminLockIntegration(t *testing.T) {
	conn := testConnector(t)
	ctx := context.Background()
	accountID := fmt.Sprintf("recovery-live-it-%d", time.Now().UnixNano())
	if _, err := conn.Pool.Exec(ctx, `
		INSERT INTO accounts (id, email, payload)
		VALUES ($1, 'live@example.invalid', '{"key":"test-token"}'::jsonb)`, accountID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.Pool.Exec(context.Background(), `DELETE FROM account_pool WHERE account_id = $1`, accountID)
		_, _ = conn.Pool.Exec(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
	})
	if _, err := conn.SetAccountAdminLocked(ctx, accountID, true, "manual"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.SetAccountPoolStatus(ctx, accountID, "live", "manual recover", "", nil); err != nil {
		t.Fatal(err)
	}
	var enabled, locked bool
	var attempts int
	if err := conn.Pool.QueryRow(ctx, `SELECT enabled, admin_locked, recovery_fail_count FROM account_pool WHERE account_id=$1`, accountID).Scan(&enabled, &locked, &attempts); err != nil {
		t.Fatal(err)
	}
	if !enabled || locked || attempts != 0 {
		t.Fatalf("manual live did not clear lock: enabled=%v locked=%v attempts=%d", enabled, locked, attempts)
	}
}

func TestRecoveryMissDelayMatchesLadder(t *testing.T) {
	// Documents applyRecoveryMiss SQL CASE: schedule uses pre-increment fail count.
	cases := []struct {
		outcome RecoveryOutcome
		fails   int
		want    time.Duration
	}{
		{RecoveryOutcomeInconclusive, 0, 15 * time.Minute},
		{RecoveryOutcomeInconclusive, 5, 15 * time.Minute},
		{RecoveryOutcomeFailure, 0, 15 * time.Minute},
		{RecoveryOutcomeFailure, 1, 30 * time.Minute},
		{RecoveryOutcomeFailure, 2, time.Hour},
		{RecoveryOutcomeFailure, 3, 3 * time.Hour},
		{RecoveryOutcomeFailure, 4, 6 * time.Hour},
		{RecoveryOutcomeFailure, 5, 12 * time.Hour},
		{RecoveryOutcomeFailure, 99, 12 * time.Hour},
	}
	for _, tc := range cases {
		if got := recoveryMissDelay(tc.outcome, tc.fails); got != tc.want {
			t.Fatalf("outcome=%s fails=%d: got %s want %s", tc.outcome, tc.fails, got, tc.want)
		}
	}
}

func copyRecoveryTestMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
