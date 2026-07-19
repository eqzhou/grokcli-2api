package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/store/postgres"
	"github.com/hm2899/grokcli-2api/internal/upstream/grok"
)

type Service struct {
	Store      *postgres.Connector
	Upstream   string
	Workers    int
	httpClient *http.Client
}

func New(store *postgres.Connector, upstream string) *Service {
	return &Service{
		Store:      store,
		Upstream:   strings.TrimRight(upstream, "/"),
		Workers:    envInt("GROK2API_QUOTA_WORKERS", 8, 1, 32),
		httpClient: newQuotaHTTPClient(),
	}
}

func newQuotaHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          128,
			MaxIdleConnsPerHost:   64,
			MaxConnsPerHost:       96,
			IdleConnTimeout:       60 * time.Second,
			TLSHandshakeTimeout:   8 * time.Second,
			ResponseHeaderTimeout: 12 * time.Second,
			ForceAttemptHTTP2:     true,
			DialContext: (&net.Dialer{
				Timeout:   6 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
}

func (s *Service) client() *http.Client {
	if s != nil && s.httpClient != nil {
		// Overlay explicit proxy if configured (parity with previous transport helper).
		if tr, ok := s.httpClient.Transport.(*http.Transport); ok && tr != nil {
			if p := proxyFromEnv(); p != nil {
				clone := tr.Clone()
				clone.Proxy = http.ProxyURL(p)
				return &http.Client{Timeout: s.httpClient.Timeout, Transport: clone}
			}
		}
		return s.httpClient
	}
	return newQuotaHTTPClient()
}

func (s *Service) FetchCached(ctx context.Context) (map[string]any, error) {
	if s.Store == nil {
		return map[string]any{"ok": false, "error": "store unavailable"}, nil
	}
	return s.Store.ListCachedQuotas(ctx)
}

func (s *Service) FetchOne(ctx context.Context, accountID string) (map[string]any, error) {
	if s.Store == nil {
		return map[string]any{"ok": false, "error": "store unavailable"}, nil
	}
	auth, err := s.Store.GetAccountAuth(ctx, accountID)
	if err != nil || auth == nil {
		return map[string]any{"ok": false, "account_id": accountID, "error": "account not found or has no token"}, nil
	}
	item := s.fetchOne(ctx, *auth)
	if item == nil {
		item = map[string]any{"ok": false, "account_id": auth.ID, "error": "empty quota result"}
	}
	// Persist AFTER the live fetch so the HTTP response is not blocked on PG write
	// for every single-account button click. Snapshot is fire-and-forget with a
	// detached context so request cancel does not drop the durable write.
	s.persistQuotaSnapshotAsync(auth.ID, item)
	if item["exhausted"] == true {
		item["auto_disabled"] = true
		item["pool_disabled"] = true
	}
	// Synthesize a lightweight pool view for immediate frontend feedback before
	// the async SaveQuotaSnapshot lands. Authoritative pool is re-read by UI later.
	// Must not embed `item` inside pool (see syntheticPoolFromQuota) — that creates
	// a JSON cycle and writeJSON encodes an empty body.
	item["pool"] = syntheticPoolFromQuota(auth.ID, item)
	return item, nil
}

func (s *Service) FetchAll(ctx context.Context) (map[string]any, error) {
	if s.Store == nil {
		return map[string]any{"ok": false, "error": "store unavailable"}, nil
	}
	// Include disabled accounts so recovery can re-enable them after billing heals.
	auths, err := s.Store.ListAccountAuths(ctx, 2000, false)
	if err != nil {
		return nil, err
	}
	workers := s.Workers
	if workers <= 0 {
		workers = 8
	}
	if workers > len(auths) && len(auths) > 0 {
		workers = len(auths)
	}
	type result struct{ item map[string]any }
	ch := make(chan result, len(auths))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	// Collect snapshots for a single async bulk persist after the response is built.
	// Live billing is the slow part; PG writes must not serialize the admin button.
	snaps := make([]quotaSnap, 0, len(auths))
	var snapsMu sync.Mutex
	for _, auth := range auths {
		wg.Add(1)
		go func(a postgres.AccountAuth) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			item := s.fetchOne(ctx, a)
			if item == nil {
				item = map[string]any{"ok": false, "account_id": a.ID, "error": "empty quota result"}
			}
			if item["exhausted"] == true {
				item["auto_disabled"] = true
				item["pool_disabled"] = true
			}
			item["pool"] = syntheticPoolFromQuota(a.ID, item)
			snapsMu.Lock()
			snaps = append(snaps, quotaSnap{id: a.ID, item: item})
			snapsMu.Unlock()
			ch <- result{item: item}
		}(auth)
	}
	wg.Wait()
	close(ch)
	// Fire bulk persist AFTER live results are ready so the HTTP handler can return.
	go s.persistQuotaSnapshots(snaps)
	results := make([]map[string]any, 0, len(auths))
	for r := range ch {
		results = append(results, r.item)
	}
	okCount, exhausted, autoDisabled, poolDisabled := 0, 0, 0, 0
	var totalUsed, totalLimit, totalRemaining float64
	activeOK := 0
	for _, r := range results {
		if r["ok"] == true {
			okCount++
		}
		if r["exhausted"] == true {
			exhausted++
		}
		if r["auto_disabled"] == true {
			autoDisabled++
		}
		if r["pool_disabled"] == true {
			poolDisabled++
		}
		if r["ok"] == true && r["pool_disabled"] != true && r["exhausted"] != true {
			activeOK++
			totalUsed += floatOf(r["used"])
			totalLimit += floatOf(r["monthly_limit"])
			totalRemaining += floatOf(r["remaining"])
		}
	}
	return map[string]any{
		"ok":                  true,
		"fetched_at":          time.Now().Unix(),
		"count":               len(results),
		"ok_count":            okCount,
		"exhausted_count":     exhausted,
		"auto_disabled_count": autoDisabled,
		"pool_disabled_count": poolDisabled,
		"active_ok_count":     activeOK,
		"total_used":          totalUsed,
		"total_monthly_limit": totalLimit,
		"total_remaining":     totalRemaining,
		"workers":             workers,
		// Both keys: frontend accepts results || accounts; keep both for clients.
		"accounts": results,
		"results":  results,
	}, nil
}

// persistQuotaSnapshotAsync writes last_quota + pool status without blocking the
// request that already has live billing data for the UI.
func (s *Service) persistQuotaSnapshotAsync(accountID string, item map[string]any) {
	if s == nil || s.Store == nil || strings.TrimSpace(accountID) == "" || item == nil {
		return
	}
	// Shallow copy so concurrent mutation of the response map cannot race the write.
	copyItem := make(map[string]any, len(item))
	for k, v := range item {
		copyItem[k] = v
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = s.Store.SaveQuotaSnapshot(ctx, accountID, copyItem)
	}()
}

type quotaSnap struct {
	id   string
	item map[string]any
}

func (s *Service) persistQuotaSnapshots(snaps []quotaSnap) {
	if s == nil || s.Store == nil || len(snaps) == 0 {
		return
	}
	// Cap concurrent PG writers so a 7k-account pool does not stampede the DB.
	workers := s.Workers
	if workers <= 0 {
		workers = 8
	}
	if workers > 16 {
		workers = 16
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, sn := range snaps {
		sn := sn
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_ = s.Store.SaveQuotaSnapshot(ctx, sn.id, sn.item)
		}()
	}
	wg.Wait()
}

// syntheticPoolFromQuota builds a client-side pool view so the admin UI can paint
// quota_disabled / re-enabled status immediately, before async SaveQuotaSnapshot.
//
// IMPORTANT: do NOT set pool["last_quota"] = item when the caller also attaches
// item["pool"] = pool. That creates a cycle and encoding/json fails with
// "unsupported value: encountered a cycle", producing an empty HTTP body.
// Frontend already treats the quota response itself as last_quota.
func syntheticPoolFromQuota(accountID string, item map[string]any) map[string]any {
	if item == nil {
		return map[string]any{
			"id":         accountID,
			"account_id": accountID,
		}
	}
	exhausted := item["exhausted"] == true || item["auto_disabled"] == true || item["disabled_for_quota"] == true
	ok := item["ok"] == true && !exhausted
	pool := map[string]any{
		"id":         accountID,
		"account_id": accountID,
	}
	if exhausted {
		pool["disabled_for_quota"] = true
		pool["enabled"] = false
		pool["pool_status"] = "quota_disabled"
		pool["pool_disabled"] = true
		if r, ok := item["exhaust_reason"].(string); ok && strings.TrimSpace(r) != "" {
			pool["disabled_reason"] = r
		} else if d, ok := item["display"].(map[string]any); ok {
			if s, ok := d["summary"].(string); ok {
				pool["disabled_reason"] = s
			}
		}
		pool["quota_source"] = firstNonEmpty(stringFromAny(item["source"]), "billing")
	} else if ok {
		pool["disabled_for_quota"] = false
		pool["enabled"] = true
		pool["pool_status"] = "normal"
		pool["disabled_reason"] = nil
		pool["quota_source"] = nil
	}
	return pool
}

func stringFromAny(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func (s *Service) fetchOne(ctx context.Context, auth postgres.AccountAuth) map[string]any {
	out := map[string]any{
		"ok":         false,
		"account_id": auth.ID,
		"email":      auth.Email,
		"fetched_at": time.Now().Unix(),
		"source":     "billing",
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Upstream+"/billing", nil)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	// Reuse grok headers style.
	gc := &grok.Client{BaseURL: s.Upstream}
	for k, v := range gc.Headers(auth.Token, "grok-4.5") {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.client().Do(req)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		out["error"] = fmt.Sprintf("billing HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
		out["status_code"] = resp.StatusCode
		return out
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		out["error"] = "parse billing: " + err.Error()
		return out
	}
	norm := normalizeBilling(raw)
	for k, v := range norm {
		out[k] = v
	}
	out["ok"] = norm["ok"] != false && norm["error"] == nil
	return out
}

func envInt(name string, fallback, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func proxyFromEnv() *url.URL {
	proxyURL := firstNonEmpty(
		os.Getenv("GROK2API_XAI_PROXY"),
		os.Getenv("GROK2API_PROXY"),
		os.Getenv("HTTPS_PROXY"),
		os.Getenv("HTTP_PROXY"),
		os.Getenv("https_proxy"),
		os.Getenv("http_proxy"),
	)
	if proxyURL == "" {
		return nil
	}
	u, err := url.Parse(proxyURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil
	}
	return u
}

func normalizeBilling(raw map[string]any) map[string]any {
	if raw == nil {
		return map[string]any{"ok": false, "error": "empty billing response"}
	}
	cfg := raw
	if nested, ok := raw["config"].(map[string]any); ok {
		cfg = nested
	}
	monthly := money(cfg["monthlyLimit"])
	if monthly == nil {
		monthly = money(cfg["monthly_limit"])
	}
	used := money(cfg["used"])
	var remaining *float64
	if monthly != nil && used != nil {
		v := *monthly - *used
		if v < 0 {
			v = 0
		}
		remaining = &v
	}
	exhausted := false
	if monthly != nil && used != nil && *monthly > 0 && *used >= *monthly {
		exhausted = true
	}
	out := map[string]any{
		"ok":            true,
		"monthly_limit": monthly,
		"used":          used,
		"remaining":     remaining,
		"exhausted":     exhausted,
		"raw":           raw,
	}
	if monthly != nil && used != nil {
		out["display"] = map[string]any{
			"summary": fmt.Sprintf("%s / %s", fmtUSD(used), fmtUSD(monthly)),
		}
	}
	return out
}

func money(v any) *float64 {
	switch t := v.(type) {
	case float64:
		return &t
	case int:
		f := float64(t)
		return &f
	case int64:
		f := float64(t)
		return &f
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return &f
		}
	case map[string]any:
		if val, ok := t["val"]; ok {
			return money(val)
		}
	}
	return nil
}

func fmtUSD(v *float64) string {
	if v == nil {
		return "$0.00"
	}
	return fmt.Sprintf("$%.2f", *v)
}

func floatOf(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case *float64:
		if t == nil {
			return 0
		}
		return *t
	default:
		return 0
	}
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func quotaTransport() http.RoundTripper {
	// Prefer explicit env proxy; DefaultTransport already honors HTTP(S)_PROXY.
	proxyURL := firstNonEmpty(
		os.Getenv("GROK2API_XAI_PROXY"),
		os.Getenv("GROK2API_PROXY"),
		os.Getenv("HTTPS_PROXY"),
		os.Getenv("HTTP_PROXY"),
		os.Getenv("https_proxy"),
		os.Getenv("http_proxy"),
	)
	base, _ := http.DefaultTransport.(*http.Transport)
	if base == nil {
		return http.DefaultTransport
	}
	tr := base.Clone()
	if proxyURL == "" {
		return tr
	}
	u, err := url.Parse(proxyURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return tr
	}
	tr.Proxy = http.ProxyURL(u)
	return tr
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
