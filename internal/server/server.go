package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/admin"
	"github.com/hm2899/grokcli-2api/internal/auth"
	"github.com/hm2899/grokcli-2api/internal/buildinfo"
	"github.com/hm2899/grokcli-2api/internal/config"
	"github.com/hm2899/grokcli-2api/internal/models"
	"github.com/hm2899/grokcli-2api/internal/pool"
	"github.com/hm2899/grokcli-2api/internal/protocol/anthropic"
	"github.com/hm2899/grokcli-2api/internal/protocol/responses"
	"github.com/hm2899/grokcli-2api/internal/proxy"
	"github.com/hm2899/grokcli-2api/internal/store/postgres"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

type Options struct {
	Ready             func() bool
	Reason            func() string
	StaticDir         string
	PublicReadEnabled bool
	AdminReadEnabled  bool
	ChatEnabled       bool
	MessagesEnabled   bool
	ResponsesEnabled  bool
	APIKeys           *auth.APIKeyVerifier
	Models            *models.Catalog
	Store             *postgres.Connector
	// Candidates, when non-empty, is used by proxy routes instead of Store.ListPoolCandidates.
	// Intended for contract/e2e tests against a fake upstream.
	Candidates    []pool.Candidate
	AdminSessions admin.SessionVerifier
	PickObserver  proxy.PickObserver
	AffinityStore proxy.AffinityStore
	Config        config.Config
}

// NewMigrationMux exposes migration-safe process probes plus low-risk read-only
// shells. Compatibility proxy/admin API endpoints are added only after their
// Python wire contracts are frozen.
func NewMigrationMux(ready func() bool) http.Handler {
	return NewMux(Options{Ready: ready})
}

func NewMux(options Options) http.Handler {
	mux := http.NewServeMux()
	staticDir := options.StaticDir
	if strings.TrimSpace(staticDir) == "" {
		staticDir = "static"
	}

	mux.HandleFunc("GET /live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"implementation": buildinfo.Implementation,
			"version":        buildinfo.Version,
		})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		if !isReady(options) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok":             false,
				"implementation": buildinfo.Implementation,
				"version":        buildinfo.Version,
				"reason":         readyReason(options),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"implementation": buildinfo.Implementation,
			"version":        buildinfo.Version,
		})
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		status := "ok"
		if !isReady(options) {
			status = "starting"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":         status,
			"implementation": buildinfo.Implementation,
			"version":        buildinfo.Version,
			"ready":          status == "ok",
		})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		ready := 0
		if isReady(options) {
			ready = 1
		}
		_, _ = w.Write([]byte("# HELP g2a_runtime_ready Go runtime readiness gate.\n"))
		_, _ = w.Write([]byte("# TYPE g2a_runtime_ready gauge\n"))
		_, _ = w.Write([]byte("g2a_runtime_ready{implementation=\"go\"} " + itoa(ready) + "\n"))
	})
	// Exact root only. A bare "GET /" is a subtree pattern in Go 1.22+ and would
	// incorrectly serve index.html for every unmatched path (e.g. /unknown).
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		serveFile(w, r, filepath.Join(staticDir, "index.html"), false)
	})
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		serveFile(w, r, filepath.Join(staticDir, "favicon.ico"), false)
	})
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) {
		serveAdminPage(w, r, staticDir, "index")
	})
	mux.HandleFunc("GET /admin/{page}", func(w http.ResponseWriter, r *http.Request) {
		serveAdminPage(w, r, staticDir, r.PathValue("page"))
	})
	mux.HandleFunc("GET /static/{file...}", func(w http.ResponseWriter, r *http.Request) {
		serveStatic(w, r, staticDir, r.PathValue("file"))
	})
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
		serveModels(w, r, options)
	})
	mux.HandleFunc("GET /models", func(w http.ResponseWriter, r *http.Request) {
		serveModels(w, r, options)
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		serveChatCompletions(w, r, options)
	})
	mux.HandleFunc("POST /chat/completions", func(w http.ResponseWriter, r *http.Request) {
		serveChatCompletions(w, r, options)
	})
	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		serveMessages(w, r, options)
	})
	mux.HandleFunc("POST /messages", func(w http.ResponseWriter, r *http.Request) {
		serveMessages(w, r, options)
	})
	mux.HandleFunc("POST /v1/messages/count_tokens", func(w http.ResponseWriter, r *http.Request) {
		serveMessagesCountTokens(w, r, options)
	})
	mux.HandleFunc("POST /messages/count_tokens", func(w http.ResponseWriter, r *http.Request) {
		serveMessagesCountTokens(w, r, options)
	})
	mux.HandleFunc("POST /v1/responses", func(w http.ResponseWriter, r *http.Request) {
		serveResponses(w, r, options)
	})
	mux.HandleFunc("POST /responses", func(w http.ResponseWriter, r *http.Request) {
		serveResponses(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/status", func(w http.ResponseWriter, r *http.Request) {
		serveAdminStatus(w, r, options, false)
	})
	mux.HandleFunc("GET /admin/api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		serveAdminStatus(w, r, options, true)
	})
	mux.HandleFunc("GET /admin/api/models", func(w http.ResponseWriter, r *http.Request) {
		serveAdminModels(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/keys", func(w http.ResponseWriter, r *http.Request) {
		serveAdminKeys(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/accounts", func(w http.ResponseWriter, r *http.Request) {
		serveAdminAccounts(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/settings", func(w http.ResponseWriter, r *http.Request) {
		serveAdminSettings(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/logs", func(w http.ResponseWriter, r *http.Request) {
		serveAdminLogs(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/logs/actions", func(w http.ResponseWriter, r *http.Request) {
		serveAdminLogActions(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/usage/summary", func(w http.ResponseWriter, r *http.Request) {
		serveUsageSummary(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/usage/series", func(w http.ResponseWriter, r *http.Request) {
		serveUsageSeries(w, r, options)
	})
	mux.HandleFunc("GET /admin/api/usage/by-key", func(w http.ResponseWriter, r *http.Request) {
		serveUsageBreakdown(w, r, options, "key")
	})
	mux.HandleFunc("GET /admin/api/usage/by-account", func(w http.ResponseWriter, r *http.Request) {
		serveUsageBreakdown(w, r, options, "account")
	})
	mux.HandleFunc("GET /admin/api/usage/by-model", func(w http.ResponseWriter, r *http.Request) {
		serveUsageBreakdown(w, r, options, "model")
	})
	mux.HandleFunc("GET /admin/api/usage/events", func(w http.ResponseWriter, r *http.Request) {
		serveUsageEvents(w, r, options)
	})
	return mux
}

func serveModels(w http.ResponseWriter, r *http.Request, options Options) {
	if !options.PublicReadEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "Go public read routes are not enabled"})
		return
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": readyReason(options)})
		return
	}
	if options.APIKeys != nil {
		if _, err := options.APIKeys.Require(r.Context(), r); err != nil {
			status := http.StatusInternalServerError
			message := err.Error()
			if errors.Is(err, auth.ErrInvalidAPIKey) {
				status = http.StatusUnauthorized
				message = "Invalid or missing API key"
			}
			writeJSON(w, status, map[string]any{"detail": message})
			return
		}
	}
	catalog := options.Models
	if catalog == nil {
		catalog = models.NewCatalog(config.Config{DefaultModel: "grok-4.5"}, nil)
	}
	writeJSON(w, http.StatusOK, catalog.OpenAIList(r.Context()))
}

func serveChatCompletions(w http.ResponseWriter, r *http.Request, options Options) {
	if !options.ChatEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Go chat route is not enabled"})
		return
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": readyReason(options)})
		return
	}
	var apiKey *auth.APIKeyRecord
	if options.APIKeys != nil {
		verified, err := options.APIKeys.Require(r.Context(), r)
		if err != nil {
			status := http.StatusInternalServerError
			message := err.Error()
			if errors.Is(err, auth.ErrInvalidAPIKey) {
				status = http.StatusUnauthorized
				message = "Invalid or missing API key"
			}
			writeJSON(w, status, map[string]any{"detail": message})
			return
		}
		apiKey = verified
	}
	if options.Store == nil && len(options.Candidates) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	chatReq, err := proxy.DecodeChatRequest(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	candidates, err := listCandidates(r.Context(), options)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	service := proxy.ChatService{
		Catalog:       modelCatalog(options),
		Client:        &grok.Client{BaseURL: options.Config.UpstreamBase},
		PickObserver:  options.PickObserver,
		AffinityStore: options.AffinityStore,
	}
	started := time.Now()
	if chatReq.Stream {
		opened, err := service.OpenStreamWithResult(r.Context(), chatReq, candidates, "least_used")
		if err != nil {
			recordChatUsage(r, options, apiKey, "", chatReq.Model, chatReq.Stream, false, http.StatusBadGateway, started, nil, err)
			writeProxyError(w, err)
			return
		}
		defer opened.Body.Close()
		defer releaseServerPick(options, opened.AccountID)
		req := r
		if options.Config.SSEKeepalive > 0 {
			req = r.WithContext(withAnthropicKeepalive(r.Context(), options.Config.SSEKeepalive))
		}
		setProtocolObservationHeaders(w, protocolObservation{
			Protocol: "openai_chat", AccountID: opened.AccountID, PreferAccount: opened.PreferAccount,
			Failover: opened.Failover, Fingerprint: opened.Fingerprint, Accounts: opened.Accounts, Prep: opened.Prep,
		})
		stats, err := streamChatCompletions(w, req, opened.Body, optionsFromRequest(req).Keepalive)
		ok := err == nil || errors.Is(err, r.Context().Err())
		status := http.StatusOK
		if !ok {
			status = http.StatusBadGateway
		}
		recordChatUsage(r, options, apiKey, opened.AccountID, opened.Model, chatReq.Stream, ok, status, started, stats.Usage, err)
		reportChatPool(r, options, opened.AccountID, ok, err, status)
		return
	}
	result, err := service.CompleteWithResult(r.Context(), chatReq, candidates, "least_used")
	if result.AccountID != "" {
		defer releaseServerPick(options, result.AccountID)
	}
	if err != nil {
		recordChatUsage(r, options, apiKey, result.AccountID, chatReq.Model, chatReq.Stream, false, http.StatusBadGateway, started, nil, err)
		writeProxyError(w, err)
		return
	}
	recordChatUsage(r, options, apiKey, result.AccountID, result.Model, chatReq.Stream, true, http.StatusOK, started, result.Usage, nil)
	reportChatPool(r, options, result.AccountID, true, nil, http.StatusOK)
	setProtocolObservationHeaders(w, protocolObservation{
		Protocol: "openai_chat", AccountID: result.AccountID, PreferAccount: result.PreferAccount,
		Failover: result.Failover, Fingerprint: result.Fingerprint, Accounts: result.Accounts, Prep: result.Prep,
	})
	writeJSON(w, http.StatusOK, result.Payload)
}

func streamChatCompletions(w http.ResponseWriter, r *http.Request, body io.Reader, keepalive time.Duration) (proxy.StreamStats, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": "streaming is not supported by this response writer"})
		return proxy.StreamStats{}, errors.New("streaming is not supported by this response writer")
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	var stats proxy.StreamStats
	write := func(data []byte, force bool) error {
		if !force {
			select {
			case <-r.Context().Done():
				return r.Context().Err()
			default:
			}
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}
	err := grok.ReadSSEWithIdle(body, keepalive, func(event grok.Event) error {
		if event.Done {
			return write([]byte("data: [DONE]\n\n"), true)
		}
		delta, err := proxy.ParseChatDelta(event.Data)
		if err == nil && delta.Usage != nil {
			stats.Usage = delta.Usage
		}
		data := append([]byte("data: "), event.Data...)
		data = append(data, '\n', '\n')
		return write(data, false)
	}, func() error {
		return write([]byte(": keepalive\n\n"), false)
	})
	if err != nil && !errors.Is(err, r.Context().Err()) {
		encoded, _ := json.Marshal(map[string]any{"error": map[string]any{"message": err.Error(), "type": "api_error"}})
		_ = write(append(append([]byte("data: "), encoded...), '\n', '\n'), true)
		_ = write([]byte("data: [DONE]\n\n"), true)
	}
	if err == nil || errors.Is(err, r.Context().Err()) {
		// Ensure terminal DONE if upstream ended without it (soft disconnect).
	}
	return stats, err
}

func releaseServerPick(options Options, accountID string) {
	if options.PickObserver == nil || accountID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	options.PickObserver.ReleasePick(ctx, accountID)
}

func recordChatUsage(r *http.Request, options Options, apiKey *auth.APIKeyRecord, accountID, model string, stream bool, ok bool, status int, started time.Time, usage any, cause error) {
	if options.Store == nil {
		return
	}
	prompt, completion, total, cacheRead, cacheCreate, reasoning := postgres.UsageFromOpenAI(usage)
	streamValue := stream
	var apiKeyID string
	if apiKey != nil {
		apiKeyID = apiKey.ID
	}
	var errText string
	if cause != nil {
		errText = cause.Error()
	}
	latency := int(time.Since(started).Milliseconds())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, _ = options.Store.RecordUsage(ctx, postgres.UsageRecord{
		RequestID:           requestID(r),
		Implementation:      "go",
		APIKeyID:            apiKeyID,
		AccountID:           accountID,
		Model:               model,
		Protocol:            "openai_chat",
		Path:                r.URL.Path,
		Stream:              &streamValue,
		OK:                  ok,
		PromptTokens:        prompt,
		CompletionTokens:    completion,
		TotalTokens:         total,
		CacheReadTokens:     cacheRead,
		CacheCreationTokens: cacheCreate,
		ReasoningTokens:     reasoning,
		ClientIP:            clientIP(r),
		UserAgent:           r.UserAgent(),
		StatusCode:          &status,
		LatencyMS:           &latency,
		Error:               errText,
		Detail:              map[string]any{"route": "go_chat"},
	})
}

func reportChatPool(r *http.Request, options Options, accountID string, ok bool, cause error, status int) {
	if options.Store == nil || strings.TrimSpace(accountID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if ok {
		_ = options.Store.ReportPoolSuccess(ctx, accountID, true)
		return
	}
	var cooldown *time.Time
	if status == http.StatusTooManyRequests || status >= 500 {
		until := time.Now().Add(15 * time.Minute)
		cooldown = &until
	}
	var errText string
	if cause != nil {
		errText = cause.Error()
	}
	_ = options.Store.ReportPoolFailure(ctx, postgres.PoolFailure{AccountID: accountID, Error: errText, StatusCode: &status, CooldownUntil: cooldown, CooldownReason: errText, Detail: map[string]any{"source": "go_chat"}})
}

func requestID(r *http.Request) string {
	for _, name := range []string{"X-Request-ID", "X-Correlation-ID", "X-Client-Request-ID"} {
		if value := strings.TrimSpace(r.Header.Get(name)); value != "" {
			return value
		}
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "go-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return "go-" + hex.EncodeToString(buf)
}

func clientIP(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwarded != "" {
		return strings.TrimSpace(strings.Split(forwarded, ",")[0])
	}
	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}
	return r.RemoteAddr
}

func writeProxyError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, pool.ErrNoEligibleAccounts) {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"detail": err.Error()})
}

func serveMessages(w http.ResponseWriter, r *http.Request, options Options) {
	apiKey, ok := messageRouteAllowed(w, r, options)
	if !ok {
		return
	}
	// Accepted for client compatibility (Claude SDKs send it); not enforced.
	_ = r.Header.Get("anthropic-version")
	var raw map[string]any
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	messages, _ := raw["messages"].([]any)
	if len(messages) == 0 {
		writeAnthropicError(w, http.StatusBadRequest, "messages: Field required", "invalid_request_error")
		return
	}
	if !positiveNumber(raw["max_tokens"]) {
		writeAnthropicError(w, http.StatusBadRequest, "max_tokens: Input should be greater than or equal to 1", "invalid_request_error")
		return
	}
	stream, _ := raw["stream"].(bool)
	model := modelCatalog(options).Resolve(stringValue(raw["model"]))
	body, err := anthropic.BuildOpenAIChatBody(raw, model)
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, err.Error(), "invalid_request_error")
		return
	}
	allowedTools := allowedAnthropicToolNames(body)
	chatReq := proxy.ChatRequest{Model: model, Stream: false, Raw: body}
	candidates, err := listCandidates(r.Context(), options)
	if err != nil {
		writeAnthropicError(w, http.StatusInternalServerError, err.Error(), "api_error")
		return
	}
	service := proxy.ChatService{
		Catalog:       modelCatalog(options),
		Client:        &grok.Client{BaseURL: options.Config.UpstreamBase},
		PickObserver:  options.PickObserver,
		AffinityStore: options.AffinityStore,
	}
	started := time.Now()
	messageID := newAnthropicMessageID()
	// Match Python default for Claude Code: at most one outbound tool_use block.
	maxTools := options.Config.OutboundMaxTools
	if maxTools < 0 {
		maxTools = 0
	}
	if stream {
		chatReq.Stream = true
		opened, err := service.OpenStreamWithResult(r.Context(), chatReq, candidates, "least_used")
		if err != nil {
			recordAnthropicUsage(r, options, apiKey, "", model, true, false, http.StatusBadGateway, started, nil, err)
			writeAnthropicError(w, http.StatusBadGateway, err.Error(), "api_error")
			return
		}
		defer opened.Body.Close()
		defer releaseServerPick(options, opened.AccountID)
		req := r
		if options.Config.SSEKeepalive > 0 {
			req = r.WithContext(withAnthropicKeepalive(r.Context(), options.Config.SSEKeepalive))
		}
		setAnthropicObservationHeaders(w, protocolObservation{Protocol: "anthropic",
			AccountID: opened.AccountID, PreferAccount: opened.PreferAccount, Failover: opened.Failover,
			Fingerprint: opened.Fingerprint, Accounts: opened.Accounts, Prep: opened.Prep, Stream: true,
		})
		usage, err := streamAnthropicMessages(w, req, opened.Body, messageID, opened.Model, len(allowedTools) > 0, allowedTools, maxTools)
		ok := err == nil || errors.Is(err, r.Context().Err())
		status := http.StatusOK
		if !ok {
			status = http.StatusBadGateway
		}
		recordAnthropicUsage(r, options, apiKey, opened.AccountID, opened.Model, true, ok, status, started, usage, err)
		reportChatPool(r, options, opened.AccountID, ok, err, status)
		return
	}
	result, err := service.CompleteWithResult(r.Context(), chatReq, candidates, "least_used")
	if result.AccountID != "" {
		defer releaseServerPick(options, result.AccountID)
	}
	if err != nil {
		recordAnthropicUsage(r, options, apiKey, result.AccountID, model, false, false, http.StatusBadGateway, started, nil, err)
		writeAnthropicError(w, http.StatusBadGateway, err.Error(), "api_error")
		return
	}
	recordAnthropicUsage(r, options, apiKey, result.AccountID, result.Model, false, true, http.StatusOK, started, result.Usage, nil)
	reportChatPool(r, options, result.AccountID, true, nil, http.StatusOK)
	content, reasoning, finish, usage, toolCalls := anthropicCompletionParts(result.Payload)
	if maxTools > 0 && len(toolCalls) > maxTools {
		toolCalls = toolCalls[:maxTools]
	}
	setAnthropicObservationHeaders(w, protocolObservation{Protocol: "anthropic",
		AccountID: result.AccountID, PreferAccount: result.PreferAccount, Failover: result.Failover,
		Fingerprint: result.Fingerprint, Accounts: result.Accounts, Prep: result.Prep, Stream: false,
	})
	writeJSON(w, http.StatusOK, anthropic.Completion(messageID, result.Model, content, reasoning, finish, toolCalls, usage, allowedTools))
}

func serveMessagesCountTokens(w http.ResponseWriter, r *http.Request, options Options) {
	// Local heuristic only — no pool/store required (matches Python).
	if !messageCountRouteAllowed(w, r, options) {
		return
	}
	var raw map[string]any
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	if !anthropic.HasMessagesOrSystem(raw) {
		writeAnthropicError(w, http.StatusBadRequest, "messages or system required", "invalid_request_error")
		return
	}
	writeJSON(w, http.StatusOK, anthropic.CountTokensForRequest(raw))
}

func messageRouteAllowed(w http.ResponseWriter, r *http.Request, options Options) (*auth.APIKeyRecord, bool) {
	if !options.MessagesEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Go messages route is not enabled"})
		return nil, false
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": readyReason(options)})
		return nil, false
	}
	var apiKey *auth.APIKeyRecord
	if options.APIKeys != nil {
		verified, err := options.APIKeys.Require(r.Context(), r)
		if err != nil {
			status := http.StatusInternalServerError
			message := err.Error()
			if errors.Is(err, auth.ErrInvalidAPIKey) {
				status = http.StatusUnauthorized
				message = "Invalid or missing API key"
			}
			writeJSON(w, status, map[string]any{"detail": message})
			return nil, false
		}
		apiKey = verified
	}
	if options.Store == nil && len(options.Candidates) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return nil, false
	}
	return apiKey, true
}

func messageCountRouteAllowed(w http.ResponseWriter, r *http.Request, options Options) bool {
	if !options.MessagesEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Go messages route is not enabled"})
		return false
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": readyReason(options)})
		return false
	}
	if options.APIKeys != nil {
		if _, err := options.APIKeys.Require(r.Context(), r); err != nil {
			status := http.StatusInternalServerError
			message := err.Error()
			if errors.Is(err, auth.ErrInvalidAPIKey) {
				status = http.StatusUnauthorized
				message = "Invalid or missing API key"
			}
			writeJSON(w, status, map[string]any{"detail": message})
			return false
		}
	}
	return true
}

func serveResponses(w http.ResponseWriter, r *http.Request, options Options) {
	if !options.ResponsesEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Go responses route is not enabled"})
		return
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": readyReason(options)})
		return
	}
	if options.APIKeys != nil {
		if _, err := options.APIKeys.Require(r.Context(), r); err != nil {
			status := http.StatusInternalServerError
			message := err.Error()
			if errors.Is(err, auth.ErrInvalidAPIKey) {
				status = http.StatusUnauthorized
				message = "Invalid or missing API key"
			}
			writeJSON(w, status, map[string]any{"detail": message})
			return
		}
	}
	if options.Store == nil && len(options.Candidates) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	var raw map[string]any
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	stream, _ := raw["stream"].(bool)
	model := modelCatalog(options).Resolve(stringValue(raw["model"]))
	body := responses.BuildChatBody(raw, model)
	messages, _ := body["messages"].([]map[string]any)
	if len(messages) == 0 {
		writeOpenAIError(w, http.StatusBadRequest, "input must contain at least one message", "invalid_request_error")
		return
	}
	candidates, err := listCandidates(r.Context(), options)
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, err.Error(), "server_error")
		return
	}
	service := proxy.ChatService{Catalog: modelCatalog(options), Client: &grok.Client{BaseURL: options.Config.UpstreamBase}, PickObserver: options.PickObserver, AffinityStore: options.AffinityStore}
	started := time.Now()
	responseID := responses.NewResponseID()
	chatReq := proxy.ChatRequest{Model: model, Stream: stream, Raw: body}
	if stream {
		opened, err := service.OpenStreamWithResult(r.Context(), chatReq, candidates, "least_used")
		if err != nil {
			recordResponsesUsage(r, options, "", model, true, false, http.StatusBadGateway, started, nil, err)
			writeOpenAIError(w, http.StatusBadGateway, err.Error(), "server_error")
			return
		}
		defer opened.Body.Close()
		defer releaseServerPick(options, opened.AccountID)
		req := r
		if options.Config.SSEKeepalive > 0 {
			req = r.WithContext(withAnthropicKeepalive(r.Context(), options.Config.SSEKeepalive))
		}
		setProtocolObservationHeaders(w, protocolObservation{
			Protocol: "openai_responses", AccountID: opened.AccountID, PreferAccount: opened.PreferAccount,
			Failover: opened.Failover, Fingerprint: opened.Fingerprint, Accounts: opened.Accounts, Prep: opened.Prep,
		})
		usage, err := streamOpenAIResponses(w, req, opened.Body, responseID, opened.Model, allowedResponsesToolNames(body), optionsFromRequest(req).Keepalive)
		ok := err == nil || errors.Is(err, r.Context().Err())
		status := http.StatusOK
		if !ok {
			status = http.StatusBadGateway
		}
		recordResponsesUsage(r, options, opened.AccountID, opened.Model, true, ok, status, started, usage, err)
		reportChatPool(r, options, opened.AccountID, ok, err, status)
		return
	}
	chatReq.Stream = false
	result, err := service.CompleteWithResult(r.Context(), chatReq, candidates, "least_used")
	if result.AccountID != "" {
		defer releaseServerPick(options, result.AccountID)
	}
	if err != nil {
		recordResponsesUsage(r, options, result.AccountID, model, false, false, http.StatusBadGateway, started, nil, err)
		writeOpenAIError(w, http.StatusBadGateway, err.Error(), "server_error")
		return
	}
	recordResponsesUsage(r, options, result.AccountID, result.Model, false, true, http.StatusOK, started, result.Usage, nil)
	reportChatPool(r, options, result.AccountID, true, nil, http.StatusOK)
	content, reasoning, _, _, toolCalls := anthropicCompletionParts(result.Payload)
	setProtocolObservationHeaders(w, protocolObservation{
		Protocol: "openai_responses", AccountID: result.AccountID, PreferAccount: result.PreferAccount,
		Failover: result.Failover, Fingerprint: result.Fingerprint, Accounts: result.Accounts, Prep: result.Prep,
	})
	writeJSON(w, http.StatusOK, responses.BuildObject(responseID, result.Model, content, reasoning, responseToolCalls(toolCalls), usageMap(result.Usage), time.Now().Unix(), stringValue(raw["previous_response_id"]), metadataMap(raw["metadata"])))
}

func streamOpenAIResponses(w http.ResponseWriter, r *http.Request, body io.Reader, responseID, model string, allowed []string, keepalive time.Duration) (map[string]any, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": "streaming is not supported by this response writer"})
		return nil, errors.New("streaming is not supported by this response writer")
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Grok2API-Protocol", "openai_responses")
	w.WriteHeader(http.StatusOK)
	streamer := responses.NewLiveStreamer(responseID, model, allowed)
	writeFrame := func(frame string, force bool) error {
		if !force {
			select {
			case <-r.Context().Done():
				return r.Context().Err()
			default:
			}
		}
		_, err := w.Write([]byte(frame))
		if err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}
	emitFrames := func(frames []string, force bool) error {
		for _, frame := range frames {
			if err := writeFrame(frame, force); err != nil {
				return err
			}
		}
		return nil
	}
	var usage map[string]any
	err := grok.ReadSSEWithIdle(body, keepalive, func(event grok.Event) error {
		if event.Done {
			return nil
		}
		delta, err := proxy.ParseChatDelta(event.Data)
		if err != nil {
			return nil
		}
		if raw, ok := delta.Usage.(map[string]any); ok {
			usage = raw
		}
		if err := emitFrames(streamer.Reasoning(delta.Reasoning), true); err != nil {
			return err
		}
		if err := emitFrames(streamer.Text(delta.Content), true); err != nil {
			return err
		}
		return emitFrames(streamer.ToolDeltas(responsesToolDeltas(delta)), true)
	}, func() error {
		return writeFrame(": keepalive\n\n", false)
	})
	if err != nil && !errors.Is(err, r.Context().Err()) {
		_ = emitFrames(streamer.Fail(err.Error(), "server_error"), true)
		return usage, err
	}
	respUsage := responsesUsageFromOpenAI(usage)
	if err := emitFrames(streamer.Complete(&respUsage), true); err != nil {
		return usage, err
	}
	return usage, err
}

func responsesToolDeltas(delta proxy.ChatDelta) []responses.ToolDelta {
	chatDeltas := delta.AnthropicToolDeltas()
	out := make([]responses.ToolDelta, 0, len(chatDeltas))
	for _, item := range chatDeltas {
		out = append(out, responses.ToolDelta{Index: item.Index, ID: item.ID, Name: item.Name, Arguments: item.Arguments})
	}
	return out
}

func responsesUsageFromOpenAI(usage map[string]any) responses.Usage {
	prompt, completion, total, cacheRead, cacheCreate, reasoning := postgres.UsageFromOpenAI(usage)
	return responses.Usage{InputTokens: int(prompt), OutputTokens: int(completion), TotalTokens: int(total), CachedTokens: int(cacheRead), CacheCreationTokens: int(cacheCreate), ReasoningTokens: int(reasoning)}
}

func allowedResponsesToolNames(body map[string]any) []string {
	return allowedAnthropicToolNames(body)
}

func writeOpenAIError(w http.ResponseWriter, status int, message, errorType string) {
	if errorType == "" {
		errorType = "server_error"
	}
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": message, "type": errorType}})
}

func recordResponsesUsage(r *http.Request, options Options, accountID, model string, stream bool, ok bool, status int, started time.Time, usage any, cause error) {
	if options.Store == nil {
		return
	}
	prompt, completion, total, cacheRead, cacheCreate, reasoning := postgres.UsageFromOpenAI(usage)
	streamValue := stream
	var errText string
	if cause != nil {
		errText = cause.Error()
	}
	latency := int(time.Since(started).Milliseconds())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, _ = options.Store.RecordUsage(ctx, postgres.UsageRecord{
		RequestID:           requestID(r),
		Implementation:      "go",
		AccountID:           accountID,
		Model:               model,
		Protocol:            "openai_responses",
		Path:                r.URL.Path,
		Stream:              &streamValue,
		OK:                  ok,
		PromptTokens:        prompt,
		CompletionTokens:    completion,
		TotalTokens:         total,
		CacheReadTokens:     cacheRead,
		CacheCreationTokens: cacheCreate,
		ReasoningTokens:     reasoning,
		ClientIP:            clientIP(r),
		UserAgent:           r.UserAgent(),
		StatusCode:          &status,
		LatencyMS:           &latency,
		Error:               errText,
		Detail:              map[string]any{"route": "go_responses"},
	})
}

func responseToolCalls(calls []anthropic.ToolCall) []map[string]any {
	out := make([]map[string]any, 0, len(calls))
	for _, call := range calls {
		out = append(out, map[string]any{"id": call.ID, "type": "function", "function": map[string]any{"name": call.Name, "arguments": call.Arguments}})
	}
	return out
}

func usageMap(value any) map[string]any {
	usage, _ := value.(map[string]any)
	if usage == nil {
		return map[string]any{}
	}
	return usage
}

func metadataMap(value any) map[string]any {
	metadata, _ := value.(map[string]any)
	return metadata
}

type protocolObservation struct {
	Protocol      string
	AccountID     string
	PreferAccount string
	Failover      bool
	Fingerprint   string
	Accounts      int
	Prep          proxy.BodyPrepStats
	Stream        bool
}

func setAnthropicObservationHeaders(w http.ResponseWriter, obs protocolObservation) {
	if obs.Protocol == "" {
		obs.Protocol = "anthropic"
	}
	setProtocolObservationHeaders(w, obs)
}

func setProtocolObservationHeaders(w http.ResponseWriter, obs protocolObservation) {
	if obs.Protocol == "" {
		obs.Protocol = "go"
	}
	w.Header().Set("X-Grok2API-Protocol", obs.Protocol)
	if obs.Accounts > 0 {
		w.Header().Set("X-Grok2API-Accounts", strconv.Itoa(obs.Accounts))
	}
	if obs.PreferAccount != "" {
		w.Header().Set("X-Grok2API-Affinity", "1")
	} else {
		w.Header().Set("X-Grok2API-Affinity", "0")
	}
	if obs.Failover {
		w.Header().Set("X-Grok2API-Affinity-Rebind", "1")
	}
	if obs.Fingerprint != "" {
		w.Header().Set("X-Grok2API-Conversation-Fp", obs.Fingerprint)
	}
	if obs.AccountID != "" {
		w.Header().Set("X-Grok2API-Account", obs.AccountID)
	}
	if compact := obs.Prep.Compact; compact != nil {
		if truthyAny(compact["applied"]) {
			w.Header().Set("X-Grok2API-History-Compact", "1")
		} else {
			w.Header().Set("X-Grok2API-History-Compact", "0")
		}
		if v, ok := compact["before_chars"]; ok {
			w.Header().Set("X-Grok2API-History-Before", fmt.Sprint(v))
		}
		if v, ok := compact["after_chars"]; ok {
			w.Header().Set("X-Grok2API-History-After", fmt.Sprint(v))
		}
		if v, ok := compact["tool_rounds"]; ok {
			w.Header().Set("X-Grok2API-History-Tool-Rounds", fmt.Sprint(v))
		}
		if truthyAny(compact["prefix_stable"]) {
			w.Header().Set("X-Grok2API-History-Prefix-Stable", "1")
		}
		if truthyAny(compact["auto"]) {
			w.Header().Set("X-Grok2API-History-Auto", "1")
		}
	}
	if stabilize := obs.Prep.Stabilize; stabilize != nil {
		w.Header().Set("X-Grok2API-Prompt-Stable", "1")
		w.Header().Set("X-Grok2API-Prompt-Stable-Messages", fmt.Sprint(stabilize["messages_stabilized"]))
		w.Header().Set("X-Grok2API-Prompt-Stable-Tools", fmt.Sprint(stabilize["tools_stabilized"]))
	} else {
		w.Header().Set("X-Grok2API-Prompt-Stable", "0")
	}
}

func truthyAny(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func streamAnthropicMessages(w http.ResponseWriter, r *http.Request, body io.Reader, messageID, model string, toolsRequested bool, allowed []string, maxTools int) (map[string]any, error) {
	return streamAnthropicMessagesWithOptions(w, r, body, messageID, model, toolsRequested, allowed, maxTools, optionsFromRequest(r))
}

type anthropicStreamOptions struct {
	Keepalive time.Duration
}

func optionsFromRequest(r *http.Request) anthropicStreamOptions {
	// Default matches Python SSE_KEEPALIVE_INTERVAL (~4s). Tests can override via context value.
	keepalive := 4 * time.Second
	if r != nil {
		if value := r.Context().Value(anthropicKeepaliveContextKey{}); value != nil {
			if d, ok := value.(time.Duration); ok {
				keepalive = d
			}
		}
	}
	return anthropicStreamOptions{Keepalive: keepalive}
}

type anthropicKeepaliveContextKey struct{}

func withAnthropicKeepalive(ctx context.Context, d time.Duration) context.Context {
	return context.WithValue(ctx, anthropicKeepaliveContextKey{}, d)
}

func streamAnthropicMessagesWithOptions(w http.ResponseWriter, r *http.Request, body io.Reader, messageID, model string, toolsRequested bool, allowed []string, maxTools int, opts anthropicStreamOptions) (map[string]any, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": "streaming is not supported by this response writer"})
		return nil, errors.New("streaming is not supported by this response writer")
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Grok2API-Protocol", "anthropic")
	w.WriteHeader(http.StatusOK)

	assembler := anthropic.NewStreamAssembler(messageID, model, toolsRequested, maxTools, allowed)
	probe := newDisconnectProbe(5, 2500*time.Millisecond)
	envelopeOpen := false
	var writeMu sync.Mutex

	writeFrame := func(frame string, force bool) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if !force {
			select {
			case <-r.Context().Done():
				// Soft disconnect: only hard-stop before envelope open.
				if !envelopeOpen {
					return r.Context().Err()
				}
			default:
			}
		}
		_, err := w.Write([]byte(frame))
		if err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}
	emitFrames := func(frames []string, force bool) error {
		for _, frame := range frames {
			if err := writeFrame(frame, force); err != nil {
				return err
			}
			envelopeOpen = true
		}
		return nil
	}

	var finish string
	var usage anthropic.Usage
	var openAIUsage map[string]any
	sawModel := false

	onIdle := func() error {
		if probe.check(r.Context()) && !envelopeOpen {
			return r.Context().Err()
		}
		// Anthropic named ping + SSE comment for picky reverse proxies.
		if err := writeFrame(anthropic.Ping(), false); err != nil {
			return err
		}
		return writeFrame(anthropic.CommentKeepalive(), false)
	}

	err := grok.ReadSSEWithIdle(body, opts.Keepalive, func(event grok.Event) error {
		if probe.check(r.Context()) && !envelopeOpen {
			return r.Context().Err()
		}
		if event.Done {
			return nil
		}
		delta, err := proxy.ParseChatDelta(event.Data)
		if err != nil {
			return nil
		}
		if delta.FinishReason != nil {
			finish = stringValue(delta.FinishReason)
		}
		if delta.Usage != nil {
			if raw, ok := delta.Usage.(map[string]any); ok {
				openAIUsage = raw
				prompt, completion, total, cacheRead, cacheCreate, _ := postgres.UsageFromOpenAI(raw)
				usage = anthropic.Usage{PromptTokens: int(prompt), CompletionTokens: int(completion), TotalTokens: int(total), CacheReadTokens: int(cacheRead), CacheCreationTokens: int(cacheCreate)}
			}
		}
		if strings.TrimSpace(delta.Content) != "" || strings.TrimSpace(delta.Reasoning) != "" || len(delta.AnthropicToolDeltas()) > 0 {
			sawModel = true
		}
		return emitFrames(assembler.Feed(delta.Content, delta.Reasoning, delta.AnthropicToolDeltas()), true)
	}, onIdle)

	clientGone := probe.gone || errors.Is(err, r.Context().Err()) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	if err != nil && !clientGone {
		_ = emitFrames(anthropic.TerminalError(err.Error(), "api_error"), true)
		return openAIUsage, err
	}
	if !sawModel && !envelopeOpen {
		// Empty stream with no client bytes yet: surface as error without a half-open envelope.
		empty := errors.New("Upstream returned HTTP 200 with empty model output (no content/tool_calls)")
		_ = emitFrames(anthropic.TerminalError(empty.Error(), "api_error"), true)
		return openAIUsage, empty
	}
	if finish == "" {
		finish = "stop"
	}
	// Soft disconnect after envelope open still needs terminal frames so Claude Code
	// can leave "running" and update task status.
	if termErr := emitFrames(assembler.Finish(finish, usage), true); termErr != nil && !clientGone {
		return openAIUsage, termErr
	}
	if clientGone {
		return openAIUsage, nil
	}
	return openAIUsage, err
}

type disconnectProbe struct {
	hitsNeeded int
	minSpan    time.Duration
	hits       int
	firstHit   time.Time
	gone       bool
}

func newDisconnectProbe(hitsNeeded int, minSpan time.Duration) *disconnectProbe {
	if hitsNeeded < 1 {
		hitsNeeded = 1
	}
	return &disconnectProbe{hitsNeeded: hitsNeeded, minSpan: minSpan}
}

func (p *disconnectProbe) check(ctx context.Context) bool {
	if p == nil {
		return false
	}
	if p.gone {
		return true
	}
	select {
	case <-ctx.Done():
		now := time.Now()
		if p.hits == 0 {
			p.firstHit = now
		}
		p.hits++
		if p.hits >= p.hitsNeeded && (p.minSpan <= 0 || now.Sub(p.firstHit) >= p.minSpan) {
			p.gone = true
			return true
		}
		return false
	default:
		p.hits = 0
		p.firstHit = time.Time{}
		return false
	}
}

func recordAnthropicUsage(r *http.Request, options Options, apiKey *auth.APIKeyRecord, accountID, model string, stream bool, ok bool, status int, started time.Time, usage any, cause error) {
	if options.Store == nil {
		return
	}
	prompt, completion, total, cacheRead, cacheCreate, reasoning := postgres.UsageFromOpenAI(usage)
	streamValue := stream
	var apiKeyID string
	if apiKey != nil {
		apiKeyID = apiKey.ID
	}
	var errText string
	if cause != nil {
		errText = cause.Error()
	}
	latency := int(time.Since(started).Milliseconds())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, _ = options.Store.RecordUsage(ctx, postgres.UsageRecord{
		RequestID:           requestID(r),
		Implementation:      "go",
		APIKeyID:            apiKeyID,
		AccountID:           accountID,
		Model:               model,
		Protocol:            "anthropic",
		Path:                r.URL.Path,
		Stream:              &streamValue,
		OK:                  ok,
		PromptTokens:        prompt,
		CompletionTokens:    completion,
		TotalTokens:         total,
		CacheReadTokens:     cacheRead,
		CacheCreationTokens: cacheCreate,
		ReasoningTokens:     reasoning,
		ClientIP:            clientIP(r),
		UserAgent:           r.UserAgent(),
		StatusCode:          &status,
		LatencyMS:           &latency,
		Error:               errText,
		Detail:              map[string]any{"route": "go_messages"},
	})
}

func anthropicCompletionParts(payload map[string]any) (content, reasoning, finish string, usage anthropic.Usage, calls []anthropic.ToolCall) {
	usage = anthropic.Usage{}
	if rawUsage, ok := payload["usage"].(map[string]any); ok {
		prompt, completion, total, cacheRead, cacheCreate, _ := postgres.UsageFromOpenAI(rawUsage)
		usage = anthropic.Usage{PromptTokens: int(prompt), CompletionTokens: int(completion), TotalTokens: int(total), CacheReadTokens: int(cacheRead), CacheCreationTokens: int(cacheCreate)}
	}
	choices, _ := payload["choices"].([]map[string]any)
	if len(choices) == 0 {
		return "", "", "stop", usage, nil
	}
	finish = stringValue(choices[0]["finish_reason"])
	message, _ := choices[0]["message"].(map[string]any)
	content = stringValue(message["content"])
	reasoning = firstNonEmpty(stringValue(message["reasoning_content"]), stringValue(message["reasoning"]))
	if items, ok := message["tool_calls"].([]map[string]any); ok {
		for _, item := range items {
			fn, _ := item["function"].(map[string]any)
			calls = append(calls, anthropic.ToolCall{ID: stringValue(item["id"]), Name: stringValue(fn["name"]), Arguments: stringValue(fn["arguments"])})
		}
	}
	if fn, ok := message["function_call"].(map[string]any); ok {
		calls = append(calls, anthropic.ToolCall{Name: stringValue(fn["name"]), Arguments: stringValue(fn["arguments"])})
	}
	return content, reasoning, finish, usage, calls
}

func allowedAnthropicToolNames(body map[string]any) []string {
	items, _ := body["tools"].([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		tool, _ := item.(map[string]any)
		fn, _ := tool["function"].(map[string]any)
		if name := stringValue(fn["name"]); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func newAnthropicMessageID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "msg_go_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return "msg_" + hex.EncodeToString(buf)
}

func positiveNumber(value any) bool {
	switch v := value.(type) {
	case int:
		return v >= 1
	case int64:
		return v >= 1
	case float64:
		return v >= 1
	case json.Number:
		n, err := v.Int64()
		return err == nil && n >= 1
	default:
		return false
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func writeAnthropicError(w http.ResponseWriter, status int, message, errorType string) {
	if errorType == "" {
		switch status {
		case http.StatusUnauthorized:
			errorType = "authentication_error"
		case http.StatusForbidden:
			errorType = "permission_error"
		case http.StatusNotFound:
			errorType = "not_found_error"
		case http.StatusTooManyRequests:
			errorType = "rate_limit_error"
		case http.StatusBadRequest:
			errorType = "invalid_request_error"
		default:
			errorType = "api_error"
		}
	}
	writeJSON(w, status, map[string]any{"type": "error", "error": map[string]any{"type": errorType, "message": message}})
}

func serveAdminStatus(w http.ResponseWriter, r *http.Request, options Options, protected bool) {
	if !options.AdminReadEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Go admin read routes are not enabled"})
		return
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": readyReason(options)})
		return
	}
	if protected {
		if _, ok := admin.RequireSession(r, options.AdminSessions); !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Admin authentication required"})
			return
		}
	}
	store := options.Store
	accountCount, modelCount := int64(0), int64(0)
	keyStats := map[string]any{"total": int64(0), "enabled": int64(0), "disabled": int64(0), "total_requests": int64(0), "auth_required": false, "legacy_env_key": false}
	pool := postgres.PoolSummary{Mode: "round_robin", Source: "postgres"}
	if store != nil {
		if n, err := store.CountAccounts(r.Context()); err == nil {
			accountCount = n
		}
		if n, err := store.CountModels(r.Context(), false); err == nil {
			modelCount = n
		}
		if options.APIKeys != nil {
			if required, err := options.APIKeys.AuthRequired(r.Context()); err == nil {
				if stats, err := store.KeyStats(r.Context(), strings.TrimSpace(options.Config.LegacyAPIKey) != "", required); err == nil {
					keyStats = stats
				}
			}
		}
		if got, err := store.PoolSummary(r.Context()); err == nil {
			pool = got
		}
	}
	accounts := map[string]any{"account_count": accountCount, "active_count": pool.Live}
	payload := map[string]any{
		"ok":                   true,
		"setup_needed":         false,
		"version":              buildinfo.Version,
		"store":                map[string]any{"backend": "hybrid", "postgres": store != nil},
		"host":                 options.Config.Host,
		"port":                 options.Config.Port,
		"upstream":             options.Config.UpstreamBase,
		"default_model":        options.Config.DefaultModel,
		"require_api_key_mode": options.Config.RequireAPIKey,
		"api_base":             publicAPIBase(r, options.Config.Port),
		"credentials_ok":       pool.Live > 0,
		"credentials_email":    nil,
		"account_mode":         pool.Mode,
		"accounts":             accounts,
		"pool": map[string]any{
			"mode": pool.Mode, "total": pool.Total, "live": pool.Live, "rotatable": pool.Rotatable,
			"enabled": pool.Enabled, "in_cooldown": pool.InCooldown, "quota_disabled": pool.QuotaDisabled,
			"model_blocked": pool.ModelBlocked, "expired": pool.Expired, "disabled": pool.Disabled, "source": pool.Source,
		},
		"keys":                  keyStats,
		"models_count":          modelCount,
		"settings":              map[string]any{},
		"token_maintainer":      map[string]any{"enabled": false, "implementation": "go", "started": false},
		"model_health":          map[string]any{"enabled": false, "implementation": "go", "started": false},
		"conversation_affinity": map[string]any{"enabled": false, "implementation": "go"},
		"registration":          map[string]any{"mode": options.Config.RegistrationMode, "external": true, "available": options.Config.RegistrationServiceURL != ""},
		"usage":                 map[string]any{"today_requests": 0, "today_tokens": 0, "total_tokens": 0},
	}
	if protected {
		payload["credentials"] = map[string]any{"email": nil, "active_count": pool.Live, "account_count": accountCount, "ok": pool.Live > 0}
		payload["models"] = modelCatalog(options).PublicModels(r.Context())
		payload["account_modes"] = []string{"round_robin", "random", "least_used"}
		payload["full"] = false
	}
	writeJSON(w, http.StatusOK, payload)
}

func serveAdminModels(w http.ResponseWriter, r *http.Request, options Options) {
	if !options.AdminReadEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Go admin read routes are not enabled"})
		return
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": readyReason(options)})
		return
	}
	if _, ok := admin.RequireSession(r, options.AdminSessions); !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Admin authentication required"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object":        "list",
		"data":          modelCatalog(options).PublicModels(r.Context()),
		"default_model": options.Config.DefaultModel,
		"storage":       "postgres",
		"meta":          map[string]any{},
	})
}

func serveAdminKeys(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	keys, err := options.Store.ListAPIKeys(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	public := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		public = append(public, key.PublicMap())
	}
	required := false
	if options.APIKeys != nil {
		required, _ = options.APIKeys.AuthRequired(r.Context())
	}
	stats, err := options.Store.KeyStats(r.Context(), strings.TrimSpace(options.Config.LegacyAPIKey) != "", required)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": public, "stats": stats, "store_source": "postgres", "store_backend": "postgres"})
}

func serveAdminAccounts(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	query := r.URL.Query()
	if truthy(query.Get("summary")) {
		count, _ := options.Store.CountAccounts(r.Context())
		pool, _ := options.Store.PoolSummary(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{
			"account_count": count,
			"active_count":  pool.Live,
			"pool":          pool,
			"page":          1,
			"page_size":     0,
			"total":         count,
			"total_pages":   1,
			"q":             strings.TrimSpace(query.Get("q")),
			"sort":          query.Get("sort"),
		})
		return
	}
	page := intQuery(query.Get("page"), 1)
	pageSize := intQuery(query.Get("page_size"), 25)
	result, err := options.Store.ListAccountSummaries(r.Context(), page, pageSize, query.Get("q"), query.Get("sort"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func serveAdminSettings(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	settings, err := options.Store.PublicSettings(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, settings)
}

func serveAdminLogs(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	query := r.URL.Query()
	items, err := options.Store.ListTasks(r.Context(), intQuery(query.Get("page"), 1), intQuery(query.Get("page_size"), 50), query.Get("q"), firstNonEmpty(query.Get("kind"), query.Get("action")), query.Get("status"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func serveAdminLogActions(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	kinds, err := options.Store.ListTaskKinds(r.Context(), 50)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error(), "actions": kinds})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "actions": kinds, "kinds": kinds})
}

func serveUsageSummary(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	payload, err := options.Store.UsageSummary(r.Context(), intQuery(r.URL.Query().Get("days"), 7))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func serveUsageSeries(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	payload, err := options.Store.UsageSeries(r.Context(), intQuery(r.URL.Query().Get("days"), 7))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func serveUsageBreakdown(w http.ResponseWriter, r *http.Request, options Options, dim string) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	query := r.URL.Query()
	payload, err := options.Store.UsageBreakdown(r.Context(), dim, intQuery(query.Get("days"), 7), intQuery(query.Get("limit"), 50))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func serveUsageEvents(w http.ResponseWriter, r *http.Request, options Options) {
	if !adminRouteAllowed(w, r, options) {
		return
	}
	if options.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "PostgreSQL store unavailable"})
		return
	}
	query := r.URL.Query()
	var okFlag *bool
	switch strings.ToLower(strings.TrimSpace(query.Get("ok"))) {
	case "1", "true", "yes", "ok", "success":
		v := true
		okFlag = &v
	case "0", "false", "no", "fail", "failed", "error":
		v := false
		okFlag = &v
	}
	payload, err := options.Store.UsageEvents(r.Context(), intQuery(query.Get("page"), 1), intQuery(query.Get("page_size"), 50), map[string]string{
		"q": query.Get("q"), "api_key_id": query.Get("api_key_id"), "account_id": query.Get("account_id"), "model": query.Get("model"), "protocol": query.Get("protocol"), "client_ip": query.Get("client_ip"),
	}, okFlag)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func adminRouteAllowed(w http.ResponseWriter, r *http.Request, options Options) bool {
	if !options.AdminReadEnabled {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Go admin read routes are not enabled"})
		return false
	}
	if !isReady(options) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": readyReason(options)})
		return false
	}
	if _, ok := admin.RequireSession(r, options.AdminSessions); !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Admin authentication required"})
		return false
	}
	return true
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func intQuery(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func listCandidates(ctx context.Context, options Options) ([]pool.Candidate, error) {
	if len(options.Candidates) > 0 {
		out := make([]pool.Candidate, len(options.Candidates))
		copy(out, options.Candidates)
		return out, nil
	}
	if options.Store == nil {
		return nil, errors.New("PostgreSQL store unavailable")
	}
	return options.Store.ListPoolCandidates(ctx)
}

func modelCatalog(options Options) *models.Catalog {
	if options.Models != nil {
		return options.Models
	}
	return models.NewCatalog(config.Config{DefaultModel: "grok-4.5"}, nil)
}

func publicAPIBase(r *http.Request, port int) string {
	host := r.Host
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-Host")); forwarded != "" {
		host = forwarded
	}
	proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if proto == "" {
		proto = "http"
	}
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
		if port > 0 {
			host = host + ":" + itoaPort(port)
		}
	}
	return proto + "://" + host + "/v1"
}

func serveAdminPage(w http.ResponseWriter, r *http.Request, staticDir, page string) {
	name := strings.TrimSpace(strings.TrimSuffix(page, ".html"))
	if name == "" {
		name = "index"
	}
	if !allowedAdminPage(name) {
		http.NotFound(w, r)
		return
	}
	serveFile(w, r, filepath.Join(staticDir, "admin", name+".html"), true)
}

func allowedAdminPage(name string) bool {
	switch name {
	case "index", "overview", "login", "keys", "accounts", "models", "guide", "settings", "logs", "usage":
		return true
	default:
		return false
	}
}

func serveStatic(w http.ResponseWriter, r *http.Request, staticDir, name string) {
	cleaned := filepath.Clean("/" + name)
	if cleaned == "/" || strings.Contains(cleaned, "..") {
		http.NotFound(w, r)
		return
	}
	serveFile(w, r, filepath.Join(staticDir, cleaned), false)
}

func serveFile(w http.ResponseWriter, r *http.Request, name string, noStore bool) {
	if noStore {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
	}
	info, err := os.Stat(name)
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, name)
}

func isReady(options Options) bool {
	return options.Ready != nil && options.Ready()
}

func readyReason(options Options) string {
	if options.Reason == nil {
		return "not ready"
	}
	return options.Reason()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	return "1"
}

func itoaPort(value int) string {
	return strconv.Itoa(value)
}
