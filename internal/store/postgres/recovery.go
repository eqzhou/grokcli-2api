package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type RecoveryOutcome string

const (
	RecoveryOutcomeSuccess      RecoveryOutcome = "success"
	RecoveryOutcomeFailure      RecoveryOutcome = "failure"
	RecoveryOutcomeInconclusive RecoveryOutcome = "inconclusive"
	RecoveryOutcomeExhausted    RecoveryOutcome = "exhausted"
)

// RecoveryCandidate is a leased disabled account plus the optimistic-lock
// versions captured before making any external probe request.
type RecoveryCandidate struct {
	AccountID         string
	Email             string
	Token             string
	AccountVersion    int64
	PoolVersion       int64
	RecoveryFailCount int
	DisabledForQuota  bool
	DisabledReason    string
	QuotaSource       string
	LeaseUntil        time.Time
}

type RecoveryApplyInput struct {
	AccountID              string
	Model                  string
	ExpectedAccountVersion int64
	ExpectedPoolVersion    int64
	Outcome                RecoveryOutcome
	Probe                  map[string]any
	Quota                  map[string]any
}

type RecoveryApplyResult struct {
	Applied     bool
	Recovered   bool
	Status      string
	PoolVersion int64
	PoolStatus  string
}

// ListRecoveryCandidates atomically leases disabled, non-admin-locked rows.
// The returned PoolVersion is the version after acquiring the lease and must
// be supplied unchanged to ApplyRecoveryDecision.
func (c *Connector) ListRecoveryCandidates(ctx context.Context, limit int) ([]RecoveryCandidate, error) {
	if c == nil || c.Pool == nil {
		return nil, errors.New("postgres connector is not configured")
	}
	if limit <= 0 {
		limit = 25
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := c.Pool.Query(ctx, `
		WITH picked AS (
			SELECT ap.account_id
			FROM account_pool ap
			JOIN accounts a ON a.id = ap.account_id
			WHERE (ap.enabled = false OR ap.disabled_for_quota = true)
			  AND ap.admin_locked = false
			  AND COALESCE(ap.pool_status, 'disabled') <> 'expired'
			  AND (ap.cooldown_until IS NULL OR ap.cooldown_until <= now())
			  AND (a.expires_at IS NULL OR a.expires_at > now())
			  AND COALESCE(a.payload->>'refresh_invalid', 'false') <> 'true'
			  AND (ap.recovery_next_probe_at IS NULL OR ap.recovery_next_probe_at <= now())
			  AND (ap.recovery_lease_until IS NULL OR ap.recovery_lease_until <= now())
			  AND (
			       COALESCE(a.payload->>'key', '') <> ''
			    OR COALESCE(a.payload->>'access_token', '') <> ''
			    OR COALESCE(a.payload->>'token', '') <> ''
			  )
			ORDER BY ap.recovery_next_probe_at NULLS FIRST, ap.updated_at ASC, ap.account_id ASC
			FOR UPDATE OF ap SKIP LOCKED
			LIMIT $1
		), leased AS (
			UPDATE account_pool ap
			SET recovery_lease_until = now() + interval '5 minutes'
			FROM picked p
			WHERE ap.account_id = p.account_id
			RETURNING ap.account_id, ap.row_version, ap.recovery_fail_count,
			          ap.disabled_for_quota, ap.disabled_reason, ap.quota_source,
			          ap.recovery_lease_until
		)
		SELECT l.account_id, a.email, a.payload, a.row_version, l.row_version,
		       l.recovery_fail_count, l.disabled_for_quota, l.disabled_reason,
		       l.quota_source, l.recovery_lease_until
		FROM leased l
		JOIN accounts a ON a.id = l.account_id
		ORDER BY l.account_id`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]RecoveryCandidate, 0, limit)
	for rows.Next() {
		var candidate RecoveryCandidate
		var email, reason, source *string
		var payloadBytes []byte
		if err := rows.Scan(
			&candidate.AccountID, &email, &payloadBytes,
			&candidate.AccountVersion, &candidate.PoolVersion,
			&candidate.RecoveryFailCount, &candidate.DisabledForQuota,
			&reason, &source, &candidate.LeaseUntil,
		); err != nil {
			return nil, err
		}
		payload := decodeMap(payloadBytes)
		candidate.Token, _ = firstString(payload, "key", "access_token", "token")
		candidate.Email = stringValue(email, stringFromMap(payload, "email"))
		candidate.DisabledReason = stringValue(reason, "")
		candidate.QuotaSource = stringValue(source, "")
		if strings.TrimSpace(candidate.Token) != "" {
			out = append(out, candidate)
		}
	}
	return out, rows.Err()
}

// ApplyRecoveryDecision persists the observation and, only for strong success,
// restores eligibility in the same CAS update. A concurrent account/pool write
// makes the decision stale and therefore unable to re-enable the account.
func (c *Connector) ApplyRecoveryDecision(ctx context.Context, in RecoveryApplyInput) (RecoveryApplyResult, error) {
	if err := validateRecoveryApplyInput(in); err != nil {
		return RecoveryApplyResult{}, err
	}
	probeBytes, err := json.Marshal(in.Probe)
	if err != nil {
		return RecoveryApplyResult{}, fmt.Errorf("marshal recovery probe: %w", err)
	}
	quota := in.Quota
	if quota == nil {
		quota = map[string]any{}
	}
	quotaBytes, err := json.Marshal(quota)
	if err != nil {
		return RecoveryApplyResult{}, fmt.Errorf("marshal recovery quota: %w", err)
	}

	if in.Outcome == RecoveryOutcomeSuccess {
		return c.applyRecoverySuccess(ctx, in, probeBytes, quotaBytes)
	}
	if in.Outcome == RecoveryOutcomeExhausted {
		return c.applyRecoveryExhausted(ctx, in, probeBytes, quotaBytes)
	}
	return c.applyRecoveryMiss(ctx, in, probeBytes, quotaBytes)
}

func (c *Connector) applyRecoveryExhausted(ctx context.Context, in RecoveryApplyInput, probeBytes, quotaBytes []byte) (RecoveryApplyResult, error) {
	reason := firstNonEmptyString(stringFromAny(in.Quota["exhaust_reason"]), stringFromAny(in.Quota["summary"]), "额度已耗尽")
	code, seconds := quotaExhaustCoolParams(in.Quota, stringFromAny(in.Quota["source"]), reason)
	var result RecoveryApplyResult
	err := c.Pool.QueryRow(ctx, `
		UPDATE account_pool ap
		SET last_probe = $4::jsonb,
		    last_probe_status = 'ok',
		    last_quota = $5::jsonb,
		    cooldown_until = now() + ($6::text || ' seconds')::interval,
		    cooldown_reason = $7,
		    cooldown_code = $8,
		    last_error = $7,
		    last_recovery_probe_at = now(),
		    last_recovery_outcome = 'quota_exhausted',
		    recovery_next_probe_at = now() + ($6::text || ' seconds')::interval,
		    recovery_lease_until = NULL,
		    updated_at = now()
		FROM accounts a
		WHERE ap.account_id = $1 AND a.id = ap.account_id
		  AND ap.row_version = $2 AND a.row_version = $3
		  AND ap.admin_locked = false
		RETURNING ap.row_version, ap.pool_status`,
		in.AccountID, in.ExpectedPoolVersion, in.ExpectedAccountVersion,
		probeBytes, quotaBytes, seconds, reason, code,
	).Scan(&result.PoolVersion, &result.PoolStatus)
	if err != nil {
		if isNoRows(err) {
			return RecoveryApplyResult{Status: "stale_or_protected"}, nil
		}
		return RecoveryApplyResult{}, err
	}
	result.Applied = true
	result.Status = "quota_cooldown"
	return result, nil
}

func (c *Connector) applyRecoverySuccess(ctx context.Context, in RecoveryApplyInput, probeBytes, quotaBytes []byte) (RecoveryApplyResult, error) {
	var result RecoveryApplyResult
	err := c.Pool.QueryRow(ctx, `
		UPDATE account_pool ap
		SET enabled = true,
		    disabled_for_quota = false,
		    disabled_reason = NULL,
		    quota_disabled_at = NULL,
		    quota_source = NULL,
		    blocked_models = COALESCE(ap.blocked_models, '{}'::jsonb) - $2,
		    cooldown_until = CASE WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) THEN NULL ELSE ap.cooldown_until END,
		    cooldown_reason = CASE WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) THEN NULL ELSE ap.cooldown_reason END,
		    cooldown_code = CASE WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) THEN NULL ELSE ap.cooldown_code END,
		    cooldown_model = CASE WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) THEN NULL ELSE ap.cooldown_model END,
		    cooldown_tokens_actual = CASE WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) THEN NULL ELSE ap.cooldown_tokens_actual END,
		    cooldown_tokens_limit = CASE WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) THEN NULL ELSE ap.cooldown_tokens_limit END,
		    cooldown_count = CASE WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) THEN 0 ELSE ap.cooldown_count END,
		    last_error = CASE
		      WHEN lower(COALESCE(ap.cooldown_code, '')) = ANY($7::text[]) OR ap.cooldown_until IS NULL OR ap.cooldown_until <= now()
		      THEN NULL ELSE ap.last_error END,
		    last_probe = $5::jsonb,
		    last_probe_status = 'ok',
		    last_quota = $6::jsonb,
		    last_recovery_probe_at = now(),
		    last_recovery_outcome = 'recovered',
		    recovery_next_probe_at = NULL,
		    recovery_fail_count = 0,
		    recovery_lease_until = NULL,
		    pool_status = CASE
		      WHEN (COALESCE(ap.blocked_models, '{}'::jsonb) - $2) <> '{}'::jsonb THEN 'model_blocked'
		      WHEN lower(COALESCE(ap.cooldown_code, '')) <> ALL($7::text[])
		       AND ap.cooldown_until IS NOT NULL AND ap.cooldown_until > now() THEN 'cooldown'
		      ELSE 'normal' END,
		    extra = (COALESCE(ap.extra, '{}'::jsonb) - 'probe_fail_streak' - 'quota_cool_source')
		      || jsonb_build_object('last_recovered_at', extract(epoch from now())),
		    updated_at = now()
		FROM accounts a
		WHERE ap.account_id = $1
		  AND a.id = ap.account_id
		  AND ap.row_version = $3
		  AND a.row_version = $4
		  AND ap.admin_locked = false
		  AND (a.expires_at IS NULL OR a.expires_at > now())
		  AND COALESCE(a.payload->>'refresh_invalid', 'false') <> 'true'
		RETURNING ap.row_version, ap.pool_status`,
		in.AccountID, strings.TrimSpace(in.Model), in.ExpectedPoolVersion,
		in.ExpectedAccountVersion, probeBytes, quotaBytes, recoveryQuotaCooldownCodes,
	).Scan(&result.PoolVersion, &result.PoolStatus)
	if err != nil {
		if isNoRows(err) {
			return RecoveryApplyResult{Status: "stale_or_protected"}, nil
		}
		return RecoveryApplyResult{}, err
	}
	result.Applied = true
	result.Recovered = true
	result.Status = "recovered"
	c.InvalidateCandidateCache()
	c.InvalidatePoolSummaryCache()
	return result, nil
}

func (c *Connector) applyRecoveryMiss(ctx context.Context, in RecoveryApplyInput, probeBytes, quotaBytes []byte) (RecoveryApplyResult, error) {
	status := "fail"
	if in.Outcome == RecoveryOutcomeInconclusive {
		status = "inconclusive"
	}
	var result RecoveryApplyResult
	err := c.Pool.QueryRow(ctx, `
		UPDATE account_pool ap
		SET last_probe = $4::jsonb,
		    last_probe_status = $6,
		    last_quota = CASE WHEN $5::jsonb = '{}'::jsonb THEN ap.last_quota ELSE $5::jsonb END,
		    last_recovery_probe_at = now(),
		    last_recovery_outcome = $6,
		    recovery_fail_count = CASE WHEN $6 = 'fail' THEN ap.recovery_fail_count + 1 ELSE ap.recovery_fail_count END,
		    recovery_next_probe_at = now() + CASE
		      WHEN $6 = 'inconclusive' THEN interval '15 minutes'
		      WHEN ap.recovery_fail_count <= 0 THEN interval '30 minutes'
		      WHEN ap.recovery_fail_count = 1 THEN interval '1 hour'
		      WHEN ap.recovery_fail_count = 2 THEN interval '3 hours'
		      WHEN ap.recovery_fail_count = 3 THEN interval '6 hours'
		      ELSE interval '12 hours' END,
		    recovery_lease_until = NULL,
		    updated_at = now()
		FROM accounts a
		WHERE ap.account_id = $1 AND a.id = ap.account_id
		  AND ap.row_version = $2 AND a.row_version = $3
		  AND ap.admin_locked = false
		RETURNING ap.row_version, ap.pool_status`,
		in.AccountID, in.ExpectedPoolVersion, in.ExpectedAccountVersion,
		probeBytes, quotaBytes, status,
	).Scan(&result.PoolVersion, &result.PoolStatus)
	if err != nil {
		if isNoRows(err) {
			return RecoveryApplyResult{Status: "stale_or_protected"}, nil
		}
		return RecoveryApplyResult{}, err
	}
	result.Applied = true
	result.Status = "scheduled"
	return result, nil
}

// SetAccountAdminLocked changes the explicit automation lock. Unlocking never
// enables an account; a later verified recovery decision is still required.
func (c *Connector) SetAccountAdminLocked(ctx context.Context, accountID string, locked bool, reason string) (map[string]any, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, errors.New("account id required")
	}
	if err := c.ensureAccountExists(ctx, accountID); err != nil {
		return nil, err
	}
	reason = strings.TrimSpace(reason)
	if len(reason) > 300 {
		reason = reason[:300]
	}
	if reason == "" {
		reason = "管理员停用"
	}
	_, err := c.Pool.Exec(ctx, `
		INSERT INTO account_pool (account_id, enabled, admin_locked, disabled_reason, pool_status, extra, updated_at)
		VALUES ($1, false, $2, CASE WHEN $2 THEN $3 ELSE NULL END,
		        'disabled',
		        jsonb_build_object('manual_status', CASE WHEN $2 THEN 'disabled' ELSE 'unlocked' END,
		                           'manual_status_reason', $3::text), now())
		ON CONFLICT (account_id) DO UPDATE SET
		  admin_locked = $2,
		  enabled = CASE WHEN $2 THEN false ELSE account_pool.enabled END,
		  disabled_reason = CASE WHEN $2 THEN $3 ELSE account_pool.disabled_reason END,
		  pool_status = CASE WHEN $2 THEN 'disabled' ELSE account_pool.pool_status END,
		  recovery_lease_until = NULL,
		  recovery_next_probe_at = CASE WHEN $2 THEN account_pool.recovery_next_probe_at ELSE now() END,
		  extra = COALESCE(account_pool.extra, '{}'::jsonb)
		    || jsonb_build_object('manual_status', CASE WHEN $2 THEN 'disabled' ELSE 'unlocked' END,
		                          'manual_status_reason', $3::text),
		  updated_at = now()`, accountID, locked, reason)
	if err != nil {
		return nil, err
	}
	c.InvalidateCandidateCache()
	c.InvalidatePoolSummaryCache()
	return c.GetAccountPoolView(ctx, accountID)
}

// DisableAccountRecoverable removes an account from scheduling while keeping it
// eligible for the verified recovery lane. Automated health/auth paths use this
// instead of the administrator lock.
func (c *Connector) DisableAccountRecoverable(ctx context.Context, accountID, reason, source string) (map[string]any, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, errors.New("account id required")
	}
	if err := c.ensureAccountExists(ctx, accountID); err != nil {
		return nil, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "账号认证失败"
	}
	if len(reason) > 300 {
		reason = reason[:300]
	}
	source = strings.TrimSpace(source)
	if source == "" {
		source = "model_health"
	}
	_, err := c.Pool.Exec(ctx, `
		INSERT INTO account_pool (
		  account_id, enabled, admin_locked, disabled_reason, pool_status,
		  recovery_next_probe_at, recovery_fail_count, last_error, extra, updated_at
		) VALUES (
		  $1, false, false, $2, 'disabled', now() + interval '30 minutes', 1, $2,
		  jsonb_build_object('disabled_source', $3::text), now()
		)
		ON CONFLICT (account_id) DO UPDATE SET
		  enabled = false,
		  admin_locked = account_pool.admin_locked,
		  disabled_reason = CASE WHEN account_pool.admin_locked THEN account_pool.disabled_reason ELSE $2 END,
		  pool_status = 'disabled',
		  recovery_lease_until = NULL,
		  recovery_next_probe_at = CASE WHEN account_pool.admin_locked THEN account_pool.recovery_next_probe_at ELSE now() + interval '30 minutes' END,
		  recovery_fail_count = CASE WHEN account_pool.admin_locked THEN account_pool.recovery_fail_count ELSE GREATEST(account_pool.recovery_fail_count, 0) + 1 END,
		  last_error = $2,
		  extra = CASE WHEN account_pool.admin_locked THEN account_pool.extra
		               ELSE COALESCE(account_pool.extra, '{}'::jsonb) || jsonb_build_object('disabled_source', $3::text) END,
		  updated_at = now()`, accountID, reason, source)
	if err != nil {
		return nil, err
	}
	c.InvalidateCandidateCache()
	c.InvalidatePoolSummaryCache()
	return c.GetAccountPoolView(ctx, accountID)
}

var recoveryQuotaCooldownCodes = []string{
	"free-usage", "free_usage", "subscription:free-usage-exhausted",
	"billing", "billing_quota", "quota", "quota_exhausted",
}

func isRecoveryQuotaCooldownCode(code string) bool {
	code = strings.ToLower(strings.TrimSpace(code))
	for _, allowed := range recoveryQuotaCooldownCodes {
		if code == allowed {
			return true
		}
	}
	return false
}

func recoveryBackoff(failureCount int) time.Duration {
	switch {
	case failureCount <= 0:
		return 15 * time.Minute
	case failureCount == 1:
		return 30 * time.Minute
	case failureCount == 2:
		return time.Hour
	case failureCount == 3:
		return 3 * time.Hour
	case failureCount == 4:
		return 6 * time.Hour
	default:
		return 12 * time.Hour
	}
}

func validateRecoveryApplyInput(in RecoveryApplyInput) error {
	if strings.TrimSpace(in.AccountID) == "" {
		return errors.New("account id required")
	}
	if in.ExpectedAccountVersion <= 0 || in.ExpectedPoolVersion <= 0 {
		return errors.New("positive account and pool versions are required")
	}
	if len(in.Probe) == 0 {
		return errors.New("probe observation required")
	}
	switch in.Outcome {
	case RecoveryOutcomeFailure, RecoveryOutcomeInconclusive:
		return nil
	case RecoveryOutcomeExhausted:
		if !truthyAny(in.Quota["ok"]) || !truthyAny(in.Quota["exhausted"]) {
			return errors.New("exhausted recovery requires a trusted exhausted quota observation")
		}
		return nil
	case RecoveryOutcomeSuccess:
	default:
		return errors.New("unsupported recovery outcome")
	}
	if strings.TrimSpace(in.Model) == "" {
		return errors.New("model required for successful recovery")
	}
	probeStatus := strings.ToLower(strings.TrimSpace(stringFromAny(in.Probe["probe_status"])))
	probeOutcome := strings.ToLower(strings.TrimSpace(stringFromAny(in.Probe["outcome"])))
	terminal := strings.ToLower(strings.TrimSpace(stringFromAny(in.Probe["terminal_event"])))
	outputText := strings.TrimSpace(stringFromAny(in.Probe["output_text"]))
	if probeStatus != "ok" || probeOutcome != "success" || !truthyAny(in.Probe["has_output_text"]) || terminal != "completed" || outputText != "OK" {
		return errors.New("successful recovery requires a completed probe with model output")
	}
	if !truthyAny(in.Quota["ok"]) || truthyAny(in.Quota["exhausted"]) {
		return errors.New("successful recovery requires healthy live quota")
	}
	return nil
}

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
