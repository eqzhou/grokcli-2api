package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

func TestOversizeUpstreamContextIsClientErrorAcrossPublicProtocols(t *testing.T) {
	upstreamErr := &grok.UpstreamError{
		Status: http.StatusBadGateway,
		Body:   `This model's maximum context length is 131072 tokens. However, your messages resulted in 160000 tokens.`,
	}

	tests := []struct {
		name  string
		write func(http.ResponseWriter, error)
	}{
		{name: "responses", write: writeOpenAIProxyError},
		{name: "anthropic", write: writeAnthropicProxyError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.write(rec, upstreamErr)

			if rec.Code != http.StatusRequestEntityTooLarge && rec.Code != http.StatusBadRequest {
				t.Fatalf("oversize context must be a client error, status=%d body=%s", rec.Code, rec.Body.String())
			}
			var payload map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("invalid JSON error body: %v body=%s", err, rec.Body.String())
			}
		})
	}
}
