package proxy

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

type countingFailureReporter struct {
	calls atomic.Int32
}

func (r *countingFailureReporter) ReportAccountFailure(string, string, error) {
	r.calls.Add(1)
}

func TestCheckPreparedContextRejectsConservativeOversize(t *testing.T) {
	body := map[string]any{
		"messages": []any{map[string]any{
			"role":    "system",
			"content": strings.Repeat("a", maxPreparedContextTokens*3+4096),
		}},
	}

	err := CheckPreparedContext(body, "grok-4.5")
	var limitErr *ContextLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("error=%v, want ContextLimitError", err)
	}
	if limitErr.LimitTokens != maxPreparedContextTokens || limitErr.EstimatedTokens <= limitErr.LimitTokens {
		t.Fatalf("unexpected context limit error: %+v", limitErr)
	}
}

func TestCheckPreparedContextAllowsRequestBelowLimit(t *testing.T) {
	body := map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": strings.Repeat("a", 32_000)}},
	}
	if err := CheckPreparedContext(body, "grok-4.5"); err != nil {
		t.Fatalf("small request rejected: %v", err)
	}
}

func TestCheckPreparedContextDoesNotCountBase64ImageAsText(t *testing.T) {
	body := map[string]any{
		"messages": []any{map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "text", "text": "describe this screenshot"},
				map[string]any{"type": "image_url", "image_url": map[string]any{
					"url": "data:image/png;base64," + strings.Repeat("A", 2_000_000),
				}},
			},
		}},
	}
	if err := CheckPreparedContext(body, "grok-4.5"); err != nil {
		t.Fatalf("base64 image was misclassified as text context: %v", err)
	}
}

func TestCompleteRejectsOversizeBeforePickingOrCallingUpstream(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	service := &ChatService{
		Client: &grok.Client{BaseURL: upstream.URL + "/v1", HTTP: upstream.Client()},
		Now:    func() time.Time { return time.Unix(1000, 0) },
	}
	_, err := service.CompleteWithResult(t.Context(), ChatRequest{
		Model: "grok-4.5",
		Raw: map[string]any{
			"model": "grok-4.5",
			"messages": []any{map[string]any{
				"role":    "system",
				"content": strings.Repeat("a", maxPreparedContextTokens*3+4096),
			}},
		},
	}, []pool.Candidate{{ID: "account-1", Token: "token-1", Enabled: true}}, "least_used")

	var limitErr *ContextLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("error=%v, want ContextLimitError", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("oversize request reached upstream %d times", calls.Load())
	}
}

func TestUpstreamContextLimitStopsFailoverWithoutPenalizingAccounts(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"code":"invalid-argument","error":"This model's maximum prompt length is 500000 but the request contains 900000 tokens."}`))
	}))
	defer upstream.Close()

	reporter := &countingFailureReporter{}
	service := &ChatService{
		Client:          &grok.Client{BaseURL: upstream.URL + "/v1", HTTP: upstream.Client()},
		FailureReporter: reporter,
		Now:             func() time.Time { return time.Unix(1000, 0) },
	}
	_, err := service.CompleteWithResult(t.Context(), ChatRequest{
		Model: "grok-4.5",
		Raw: map[string]any{
			"model":    "grok-4.5",
			"messages": []any{map[string]any{"role": "user", "content": "small locally, rejected by synthetic upstream"}},
		},
	}, []pool.Candidate{
		{ID: "account-1", Token: "token-1", Enabled: true, RequestCount: 0},
		{ID: "account-2", Token: "token-2", Enabled: true, RequestCount: 1},
	}, "least_used")

	if !IsContextLimitFailure(err) {
		t.Fatalf("error=%v, want context-limit failure", err)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("context-limit request retried %d times, want 1", upstreamCalls.Load())
	}
	if reporter.calls.Load() != 0 {
		t.Fatalf("context-limit request penalized accounts %d times", reporter.calls.Load())
	}
}
