package proxy

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hm2899/grokcli-2api/internal/protocol/anthropic"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

// Keep a 10% reserve below grok-4.5's current 500k input limit for tokenizer
// estimation error, tool schemas, and upstream-added framing.
const maxPreparedContextTokens = 450_000

// ContextLimitError is a client request error. It must not be reported as an
// account failure or retried with another account.
type ContextLimitError struct {
	Model           string
	EstimatedTokens int
	LimitTokens     int
}

// IsContextLimitFailure recognizes both local preflight failures and the same
// request-specific rejection returned by upstream. Callers must not rotate or
// penalize accounts for these errors.
func IsContextLimitFailure(err error) bool {
	if err == nil {
		return false
	}
	var limitErr *ContextLimitError
	if errors.As(err, &limitErr) {
		return true
	}
	text := strings.ToLower(err.Error())
	var upstream *grok.UpstreamError
	if errors.As(err, &upstream) && upstream != nil {
		text += " " + strings.ToLower(upstream.Body)
	}
	return strings.Contains(text, "context length exceeded") ||
		strings.Contains(text, "maximum context length") ||
		strings.Contains(text, "maximum prompt length")
}

func (e *ContextLimitError) Error() string {
	model := strings.TrimSpace(e.Model)
	if model == "" {
		model = "requested model"
	}
	return fmt.Sprintf(
		"context length exceeded for %s: conservatively estimated %d input tokens after history compaction; safe limit is %d",
		model,
		e.EstimatedTokens,
		e.LimitTokens,
	)
}

// CheckPreparedContext validates the final, compacted chat body before an
// account is selected. The estimator intentionally takes the larger of the
// protocol-aware estimate and JSON bytes/3: code, JSON, and non-Latin text can
// tokenize more densely than the common four-characters-per-token heuristic.
func CheckPreparedContext(body map[string]any, model string) error {
	if body == nil {
		return nil
	}
	estimated := 0
	if value, ok := anthropic.CountTokensForRequest(body)["input_tokens"].(int); ok {
		estimated = value
	}
	byteEstimate := (contextTextBytes(body, "") + 2) / 3
	if byteEstimate > estimated {
		estimated = byteEstimate
	}
	if estimated <= maxPreparedContextTokens {
		return nil
	}
	return &ContextLimitError{
		Model:           model,
		EstimatedTokens: estimated,
		LimitTokens:     maxPreparedContextTokens,
	}
}

// contextTextBytes counts request text without treating embedded image/file
// base64 as language-model text. Multimodal payloads have separate upstream
// accounting and a normal screenshot can be several megabytes.
func contextTextBytes(value any, key string) int {
	switch v := value.(type) {
	case nil:
		return 0
	case string:
		if isEmbeddedBase64(v, key) {
			// Keep a fixed conservative allowance for the multimodal part without
			// charging every encoded byte as a text token.
			return 4096
		}
		return len(v)
	case []any:
		total := 0
		for _, item := range v {
			total += contextTextBytes(item, key)
		}
		return total
	case []map[string]any:
		total := 0
		for _, item := range v {
			total += contextTextBytes(item, key)
		}
		return total
	case map[string]any:
		total := 0
		for childKey, item := range v {
			total += len(childKey) + contextTextBytes(item, strings.ToLower(childKey))
		}
		return total
	default:
		return 32
	}
}

func isEmbeddedBase64(value, key string) bool {
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(trimmed), "data:") {
		if marker := strings.Index(strings.ToLower(trimmed[:min(len(trimmed), 256)]), ";base64,"); marker >= 0 {
			return true
		}
	}
	if key != "data" || len(trimmed) < 4096 {
		return false
	}
	// Anthropic source.data and similar file blocks carry raw base64 without
	// the data: prefix. Sampling is enough to distinguish it from prose.
	for i := 0; i < min(len(trimmed), 256); i++ {
		ch := trimmed[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '+' || ch == '/' || ch == '=' ||
			ch == '-' || ch == '_' {
			continue
		}
		return false
	}
	return true
}
