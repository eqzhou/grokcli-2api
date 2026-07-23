package modelhealth

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	quotasvc "github.com/hm2899/grokcli-2api/internal/quota"
	"github.com/hm2899/grokcli-2api/internal/store/postgres"
)

var recoveryHeaderNames = [...]string{
	"X-RateLimit-Limit-Tokens",
	"X-RateLimit-Remaining-Tokens",
	"X-RateLimit-Limit-Requests",
	"X-RateLimit-Remaining-Requests",
}

func recoveryRateHeaders(headers http.Header) map[string]string {
	out := make(map[string]string, len(recoveryHeaderNames))
	for _, name := range recoveryHeaderNames {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			out[name] = value
		}
	}
	return out
}

func recoveryHTTPHeaders(raw any) http.Header {
	out := http.Header{}
	values, _ := raw.(map[string]string)
	if values == nil {
		if generic, ok := raw.(map[string]any); ok {
			values = make(map[string]string, len(generic))
			for key, value := range generic {
				if text, ok := value.(string); ok {
					values[key] = text
				}
			}
		}
	}
	for key, value := range values {
		out.Set(key, value)
	}
	return out
}

func isStrictProbeReply(text string) bool {
	return strings.TrimSpace(text) == "OK"
}

func recoveryBackoff(failures int) time.Duration {
	steps := [...]time.Duration{
		15 * time.Minute,
		30 * time.Minute,
		time.Hour,
		3 * time.Hour,
		6 * time.Hour,
		12 * time.Hour,
	}
	if failures <= 0 {
		return steps[0]
	}
	if failures >= len(steps)-1 {
		return steps[len(steps)-1]
	}
	return steps[failures]
}

func evaluateRecovery(
	ctx context.Context,
	model probeStreamResult,
	verifyQuota func(context.Context) probeOutcome,
) probeOutcome {
	if model.Outcome == probeInconclusive {
		return probeInconclusive
	}
	if model.Outcome != probeSuccess || !model.Completed || !isStrictProbeReply(model.Text) {
		return probeFailure
	}
	if verifyQuota == nil {
		return probeInconclusive
	}
	return verifyQuota(ctx)
}

type recoveryRunStats struct {
	Candidates   int `json:"candidates"`
	Checked      int `json:"checked"`
	Recovered    int `json:"recovered"`
	Exhausted    int `json:"exhausted"`
	Failed       int `json:"failed"`
	Inconclusive int `json:"inconclusive"`
	Conflicts    int `json:"cas_conflicts"`
	Protected    int `json:"protected"`
}

func (s *Service) recoveryConfig() (bool, int, int) {
	if s == nil {
		return false, 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.AutoRecover, s.RecoveryBatch, s.RecoveryWorkers
}

func (s *Service) runRecoveryLane(ctx context.Context, model string) recoveryRunStats {
	enabled, batch, workers := s.recoveryConfig()
	stats := recoveryRunStats{}
	if !enabled || s == nil || s.Store == nil || s.Quota == nil || ctx.Err() != nil {
		return stats
	}
	if batch <= 0 {
		batch = 10
	}
	if workers <= 0 {
		workers = 2
	}
	candidates, err := s.Store.ListRecoveryCandidates(ctx, batch)
	if err != nil {
		return stats
	}
	stats.Candidates = len(candidates)
	if workers > len(candidates) {
		workers = len(candidates)
	}
	if workers == 0 {
		return stats
	}

	jobs := make(chan postgres.RecoveryCandidate)
	var mu sync.Mutex
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for candidate := range jobs {
			outcome, status := s.recoverCandidate(ctx, candidate, model)
			mu.Lock()
			stats.Checked++
			if status == "apply_error" {
				stats.Failed++
				mu.Unlock()
				continue
			}
			if status == "stale_or_protected" {
				// CAS miss or admin lock: surface both conflict and protected counters.
				stats.Conflicts++
				stats.Protected++
				mu.Unlock()
				continue
			}
			switch outcome {
			case postgres.RecoveryOutcomeSuccess:
				if status == "recovered" {
					stats.Recovered++
				}
			case postgres.RecoveryOutcomeExhausted:
				stats.Exhausted++
			case postgres.RecoveryOutcomeInconclusive:
				stats.Inconclusive++
			default:
				stats.Failed++
			}
			mu.Unlock()
		}
	}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}
	for _, candidate := range candidates {
		select {
		case jobs <- candidate:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return stats
		}
	}
	close(jobs)
	wg.Wait()
	return stats
}

func (s *Service) recoverCandidate(ctx context.Context, candidate postgres.RecoveryCandidate, model string) (postgres.RecoveryOutcome, string) {
	auth := postgres.AccountAuth{ID: candidate.AccountID, Email: candidate.Email, Token: candidate.Token}
	probe := s.observeRecoveryModel(ctx, auth, model)
	outcome := postgres.RecoveryOutcomeFailure
	quotaSnapshot := map[string]any{}
	probeStatus := strings.ToLower(strings.TrimSpace(stringValueAny(probe["probe_status"])))
	if probeStatus == "inconclusive" || strings.EqualFold(stringValueAny(probe["outcome"]), string(probeInconclusive)) {
		outcome = postgres.RecoveryOutcomeInconclusive
	} else if probeStatus == "ok" && isStrictProbeReply(stringValueAny(probe["output_text"])) && strings.EqualFold(stringValueAny(probe["terminal_event"]), "completed") {
		quotaObservation := s.Quota.ObserveRecovery(ctx, auth, recoveryHTTPHeaders(probe["rate_limit_headers"]))
		quotaSnapshot = quotaObservation.Snapshot
		switch quotaObservation.Outcome {
		case quotasvc.RecoveryQuotaHealthy:
			outcome = postgres.RecoveryOutcomeSuccess
		case quotasvc.RecoveryQuotaExhausted:
			outcome = postgres.RecoveryOutcomeExhausted
		default:
			outcome = postgres.RecoveryOutcomeInconclusive
		}
	}
	result, err := s.Store.ApplyRecoveryDecision(ctx, postgres.RecoveryApplyInput{
		AccountID:              candidate.AccountID,
		Model:                  model,
		ExpectedAccountVersion: candidate.AccountVersion,
		ExpectedPoolVersion:    candidate.PoolVersion,
		Outcome:                outcome,
		Probe:                  probe,
		Quota:                  quotaSnapshot,
	})
	if err != nil {
		return outcome, "apply_error"
	}
	return outcome, result.Status
}

func stringValueAny(value any) string {
	text, _ := value.(string)
	return text
}
