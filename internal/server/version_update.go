package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/hm2899/grokcli-2api/internal/buildinfo"
)

var (
	updateTagPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	updateImagePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$`)
)

const (
	defaultGHReleaseRepo = "HM2899/grokcli-2api"
	defaultGHCRImage     = "ghcr.io/hm2899/grokcli-2api"
	versionCacheTTL      = 10 * time.Minute
	updateRequestFile    = "update.request"
	updateStatusFile     = "update.status"
	// Default in-container hot-update entry (compose/docker.sock based).
	defaultInContainerUpdateScript = "/app/scripts/g2a-hot-update-incontainer.sh"
)

type versionCheckResult struct {
	OK              bool   `json:"ok"`
	Current         string `json:"current"`
	Latest          string `json:"latest"`
	UpdateAvailable bool   `json:"update_available"`
	Image           string `json:"image"`
	ReleaseURL      string `json:"release_url,omitempty"`
	ReleaseName     string `json:"release_name,omitempty"`
	ReleaseNotes    string `json:"release_notes,omitempty"`
	CheckedAt       int64  `json:"checked_at"`
	Source          string `json:"source"` // github_releases | cache | error
	Error           string `json:"error,omitempty"`
	// Update plumbing
	UpdateMode      string `json:"update_mode"` // docker | cmd | request_file | disabled
	UpdateSupported bool   `json:"update_supported"`
	UpdateHint      string `json:"update_hint,omitempty"`
}

type updateRequest struct {
	Tag         string `json:"tag"`
	Image       string `json:"image"`
	RequestedAt int64  `json:"requested_at"`
	By          string `json:"by,omitempty"`
	Status      string `json:"status"` // pending | running | done | error
	Message     string `json:"message,omitempty"`
	FinishedAt  int64  `json:"finished_at,omitempty"`
	Mode        string `json:"mode,omitempty"`
}

var (
	versionCacheMu     sync.Mutex
	versionCacheAt     time.Time
	versionCacheResult versionCheckResult
)

func ghReleaseRepo() string {
	if v := strings.TrimSpace(os.Getenv("GROK2API_RELEASE_REPO")); v != "" {
		return strings.TrimPrefix(v, "https://github.com/")
	}
	return defaultGHReleaseRepo
}

func ghcrImage() string {
	if v := strings.TrimSpace(os.Getenv("GROK2API_GHCR_IMAGE")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return defaultGHCRImage
}

func dataDir(options Options) string {
	if v := strings.TrimSpace(os.Getenv("GROK2API_DATA_DIR")); v != "" {
		return v
	}
	// Common compose mount.
	if _, err := os.Stat("/app/data"); err == nil {
		return "/app/data"
	}
	return "data"
}

func dockerSockPath() string {
	host := strings.TrimSpace(os.Getenv("DOCKER_HOST"))
	if host == "" {
		return "/var/run/docker.sock"
	}
	if strings.HasPrefix(host, "unix://") {
		return strings.TrimPrefix(host, "unix://")
	}
	// tcp://... — still treat as available if docker CLI works; sock check only for unix.
	return ""
}

func dockerCLIAvailable() bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	return true
}

func dockerSockPresent() bool {
	p := dockerSockPath()
	if p == "" {
		// Non-unix DOCKER_HOST (tcp) — assume operator configured remote docker.
		return strings.TrimSpace(os.Getenv("DOCKER_HOST")) != ""
	}
	st, err := os.Stat(p)
	if err != nil {
		return false
	}
	// Prefer socket; also accept if path exists (some CI mounts present as file).
	return st.Mode()&os.ModeSocket != 0 || !st.IsDir()
}

func inContainerUpdateScript() string {
	if v := strings.TrimSpace(os.Getenv("GROK2API_HOT_UPDATE_SCRIPT")); v != "" {
		return v
	}
	// Prefer installed path inside image; fall back to repo-relative for local runs.
	for _, p := range []string{
		defaultInContainerUpdateScript,
		"scripts/g2a-hot-update-incontainer.sh",
		filepath.Join(".", "scripts", "g2a-hot-update-incontainer.sh"),
	} {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return defaultInContainerUpdateScript
}

// updateMode decides how admin "热更新" runs.
// Preference:
//  1. GROK2API_HOT_UPDATE_CMD — explicit operator command (always wins)
//  2. docker mode — in-container docker CLI + docker.sock (default when available)
//  3. request_file — legacy host watcher fallback (opt-in via GROK2API_HOT_UPDATE_MODE=request_file)
//  4. disabled
func updateMode() (mode string, supported bool, hint string) {
	if forced := strings.ToLower(strings.TrimSpace(os.Getenv("GROK2API_HOT_UPDATE_MODE"))); forced != "" {
		switch forced {
		case "cmd":
			if cmd := strings.TrimSpace(os.Getenv("GROK2API_HOT_UPDATE_CMD")); cmd != "" {
				return "cmd", true, "将在容器内执行 GROK2API_HOT_UPDATE_CMD（{{TAG}} / {{IMAGE}} 可替换）。"
			}
			return "disabled", false, "GROK2API_HOT_UPDATE_MODE=cmd 但未设置 GROK2API_HOT_UPDATE_CMD"
		case "docker", "incontainer", "container":
			if !dockerCLIAvailable() {
				return "disabled", false, "容器内未找到 docker CLI（镜像应内置；或挂载 docker 客户端）"
			}
			if !dockerSockPresent() {
				return "disabled", false, "未检测到 /var/run/docker.sock，请在 compose 中挂载 docker.sock 后重试"
			}
			return "docker", true, "容器内热更新：docker pull + compose force-recreate（无需宿主机 watcher）"
		case "request_file", "file", "watcher":
			return "request_file", true, "兼容模式：写入 data/update.request，由宿主机 g2a-update-watcher 执行 pull/recreate"
		case "off", "disabled", "0", "false", "no":
			return "disabled", false, "热更新已通过 GROK2API_HOT_UPDATE_MODE 关闭"
		}
	}

	// Explicit custom command always preferred when set.
	if cmd := strings.TrimSpace(os.Getenv("GROK2API_HOT_UPDATE_CMD")); cmd != "" {
		return "cmd", true, "将在容器内执行 GROK2API_HOT_UPDATE_CMD（{{TAG}} / {{IMAGE}} 可替换）。"
	}

	// Default: in-container docker when sock + CLI are present.
	if dockerCLIAvailable() && dockerSockPresent() {
		return "docker", true, "容器内热更新：docker pull 目标镜像，并通过 compose/容器自重建完成重启（无需宿主机 watcher）。"
	}

	// Legacy opt-in only — no longer the default.
	if truthyEnv("GROK2API_HOT_UPDATE_ALLOW_REQUEST_FILE") {
		return "request_file", true, "兼容模式：写入 data/update.request；需宿主机 g2a-update-watcher。"
	}

	// Explain how to enable.
	missing := []string{}
	if !dockerCLIAvailable() {
		missing = append(missing, "docker CLI")
	}
	if !dockerSockPresent() {
		missing = append(missing, "/var/run/docker.sock")
	}
	hint = "容器内热更新未就绪（缺少 " + strings.Join(missing, " + ") + "）。" +
		"请在 compose 挂载 docker.sock，并确保镜像含 docker 客户端；或设置 GROK2API_HOT_UPDATE_CMD。"
	return "disabled", false, hint
}

func truthyEnv(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func serveVersionInfo(w http.ResponseWriter, r *http.Request, options Options) {
	if !requireAdminReadWrite(w, r, options, false) {
		return
	}
	force := r.URL.Query().Get("force") == "1" || r.URL.Query().Get("refresh") == "1"
	info := checkLatestVersion(r.Context(), force)
	mode, supported, hint := updateMode()
	info.UpdateMode = mode
	info.UpdateSupported = supported
	info.UpdateHint = hint
	// Attach pending update status if any.
	if st, err := readUpdateStatus(options); err == nil && st != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":               info.OK,
			"current":          info.Current,
			"latest":           info.Latest,
			"update_available": info.UpdateAvailable,
			"image":            info.Image,
			"release_url":      info.ReleaseURL,
			"release_name":     info.ReleaseName,
			"release_notes":    info.ReleaseNotes,
			"checked_at":       info.CheckedAt,
			"source":           info.Source,
			"error":            info.Error,
			"update_mode":      info.UpdateMode,
			"update_supported": info.UpdateSupported,
			"update_hint":      info.UpdateHint,
			"update_status":    st,
		})
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func serveVersionCheck(w http.ResponseWriter, r *http.Request, options Options) {
	if !requireAdminReadWrite(w, r, options, false) {
		return
	}
	info := checkLatestVersion(r.Context(), true)
	mode, supported, hint := updateMode()
	info.UpdateMode = mode
	info.UpdateSupported = supported
	info.UpdateHint = hint
	writeJSON(w, http.StatusOK, info)
}

func serveVersionUpdate(w http.ResponseWriter, r *http.Request, options Options) {
	if !requireAdminReadWrite(w, r, options, true) {
		return
	}
	var body map[string]any
	if err := decodeJSONRequest(w, r, &body, maxAdminJSONBodyBytes); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, statusForDecodeError(err), map[string]any{"ok": false, "detail": err.Error()})
		return
	}
	if body == nil {
		body = map[string]any{}
	}
	tag := strings.TrimSpace(stringValue(body["tag"]))
	if tag == "" {
		// default: latest from check
		info := checkLatestVersion(r.Context(), true)
		if info.Latest != "" {
			tag = info.Latest
		} else {
			tag = "latest"
		}
	}
	tag = strings.TrimPrefix(tag, "v")
	if tag == "" {
		tag = "latest"
	}
	image := strings.TrimSpace(stringValue(body["image"]))
	if image == "" {
		image = ghcrImage()
	}
	if err := validateUpdateTarget(tag, image); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "detail": err.Error()})
		return
	}
	mode, supported, hint := updateMode()
	if !supported {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":     false,
			"detail": "version hot-update is disabled",
			"hint":   hint,
			"mode":   mode,
		})
		return
	}

	req := updateRequest{
		Tag:         tag,
		Image:       image,
		RequestedAt: time.Now().Unix(),
		Status:      "pending",
		Message:     "update requested",
		Mode:        mode,
	}

	switch mode {
	case "docker":
		req.Status = "running"
		req.Message = "container docker pull + recreate"
		if err := writeUpdateStatus(options, req); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "detail": err.Error()})
			return
		}
		go runInContainerDockerUpdate(options, req)
		writeJSON(w, http.StatusAccepted, map[string]any{
			"ok":      true,
			"mode":    mode,
			"message": "热更新已在容器内启动（docker pull + force-recreate）。服务将自动重启，无需宿主机 watcher。",
			"request": req,
			"hint":    hint,
		})
	case "cmd":
		req.Status = "running"
		req.Message = "executing GROK2API_HOT_UPDATE_CMD"
		if err := writeUpdateStatus(options, req); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "detail": err.Error()})
			return
		}
		go runHotUpdateCmd(options, req)
		writeJSON(w, http.StatusAccepted, map[string]any{
			"ok":      true,
			"mode":    mode,
			"message": "热更新已启动（执行 GROK2API_HOT_UPDATE_CMD：pull + recreate）。服务将自动重启。",
			"request": req,
			"hint":    hint,
		})
	default: // request_file (legacy)
		if err := writeUpdateRequest(options, req); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "detail": err.Error()})
			return
		}
		_ = writeUpdateStatus(options, req)
		writeJSON(w, http.StatusAccepted, map[string]any{
			"ok":      true,
			"mode":    mode,
			"message": "已写入 data/update.request（兼容宿主机 watcher 模式）。",
			"request": req,
			"hint":    hint,
			"file":    filepath.Join(dataDir(options), updateRequestFile),
		})
	}
}

func validateUpdateTarget(tag, image string) error {
	tag = strings.TrimSpace(strings.TrimPrefix(tag, "v"))
	image = strings.TrimSpace(image)
	configuredImage := ghcrImage()
	if !updateTagPattern.MatchString(tag) {
		return errors.New("invalid update tag")
	}
	if !updateImagePattern.MatchString(image) {
		return errors.New("invalid update image")
	}
	if image != configuredImage {
		return errors.New("update image override is not allowed")
	}
	return nil
}

func checkLatestVersion(ctx context.Context, force bool) versionCheckResult {
	versionCacheMu.Lock()
	defer versionCacheMu.Unlock()
	now := time.Now()
	if !force && !versionCacheAt.IsZero() && now.Sub(versionCacheAt) < versionCacheTTL && versionCacheResult.Latest != "" {
		out := versionCacheResult
		out.Source = "cache"
		return out
	}
	out := versionCheckResult{
		OK:      true,
		Current: strings.TrimPrefix(buildinfo.Version, "v"),
		Image:   ghcrImage(),
		Source:  "github_releases",
	}
	latest, name, notes, url, err := fetchGitHubLatestRelease(ctx, ghReleaseRepo())
	out.CheckedAt = now.Unix()
	if err != nil {
		out.OK = false
		out.Error = err.Error()
		out.Source = "error"
		// keep last good cache if any
		if versionCacheResult.Latest != "" {
			cached := versionCacheResult
			cached.Error = out.Error
			cached.CheckedAt = out.CheckedAt
			cached.Source = "cache_stale"
			return cached
		}
		return out
	}
	out.Latest = strings.TrimPrefix(latest, "v")
	out.ReleaseName = name
	out.ReleaseNotes = truncateRunes(notes, 800)
	out.ReleaseURL = url
	out.UpdateAvailable = versionLess(out.Current, out.Latest)
	versionCacheAt = now
	versionCacheResult = out
	return out
}

func fetchGitHubLatestRelease(ctx context.Context, repo string) (tag, name, notes, htmlURL string, err error) {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return "", "", "", "", errors.New("empty release repo")
	}
	api := "https://api.github.com/repos/" + repo + "/releases/latest"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, api, nil)
	if err != nil {
		return "", "", "", "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "grokcli-2api/"+buildinfo.Version)
	if tok := strings.TrimSpace(os.Getenv("GITHUB_TOKEN")); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	} else if tok := strings.TrimSpace(os.Getenv("GROK2API_GITHUB_TOKEN")); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", "", "", fmt.Errorf("github releases %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", "", "", err
	}
	tag = strings.TrimSpace(stringValue(payload["tag_name"]))
	name = strings.TrimSpace(stringValue(payload["name"]))
	notes = stringValue(payload["body"])
	htmlURL = strings.TrimSpace(stringValue(payload["html_url"]))
	if tag == "" {
		return "", "", "", "", errors.New("github release missing tag_name")
	}
	return tag, name, notes, htmlURL, nil
}

// versionLess reports whether current is strictly older than latest (semver-ish x.y.z).
func versionLess(current, latest string) bool {
	c := parseSemver(current)
	l := parseSemver(latest)
	for i := 0; i < 3; i++ {
		if c[i] < l[i] {
			return true
		}
		if c[i] > l[i] {
			return false
		}
	}
	return false
}

func parseSemver(v string) [3]int {
	v = strings.TrimSpace(strings.TrimPrefix(v, "v"))
	// drop pre-release / build
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	var out [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		n := 0
		for _, ch := range parts[i] {
			if ch < '0' || ch > '9' {
				break
			}
			n = n*10 + int(ch-'0')
		}
		out[i] = n
	}
	return out
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

func writeUpdateRequest(options Options, req updateRequest) error {
	dir := dataDir(options)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, updateRequestFile)
	raw, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func writeUpdateStatus(options Options, req updateRequest) error {
	dir := dataDir(options)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, updateStatusFile)
	raw, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func readUpdateStatus(options Options) (*updateRequest, error) {
	path := filepath.Join(dataDir(options), updateStatusFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var req updateRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func runInContainerDockerUpdate(options Options, req updateRequest) {
	script := inContainerUpdateScript()
	// Prefer script; if missing, synthesize a minimal docker pull + compose command.
	var cmd *exec.Cmd
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	env := append(os.Environ(),
		"GROK2API_UPDATE_TAG="+req.Tag,
		"GROK2API_UPDATE_IMAGE="+req.Image,
		"GROK2API_GHCR_IMAGE="+ghcrImage(),
	)
	if st, err := os.Stat(script); err == nil && !st.IsDir() {
		cmd = exec.CommandContext(ctx, "sh", script, req.Tag, req.Image)
	} else {
		// Inline fallback when script not packaged.
		full := strings.TrimRight(req.Image, "/") + ":" + strings.TrimPrefix(req.Tag, "v")
		if strings.Contains(req.Image, ":") && !strings.Contains(req.Image, "://") {
			full = req.Image
		}
		composeDir := strings.TrimSpace(os.Getenv("GROK2API_COMPOSE_DIR"))
		if composeDir == "" {
			composeDir = "/compose"
		}
		service := strings.TrimSpace(os.Getenv("GROK2API_DOCKER_SERVICE"))
		if service == "" {
			service = "grokcli-2api"
		}
		shell := fmt.Sprintf(
			`set -e; docker pull %q; `+
				`if [ -f %q/docker-compose.yml ]; then `+
				`cd %q; `+
				`printf 'services:\n  %s:\n    image: %s\n    pull_policy: always\n' > docker-compose.hot-update.yml; `+
				`docker compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.hot-update.yml pull %q; `+
				`docker compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.hot-update.yml up -d --force-recreate --remove-orphans %q; `+
				`else echo "no compose project; pull only — restart container externally"; fi`,
			full, composeDir, composeDir, service, full, service, service,
		)
		cmd = exec.CommandContext(ctx, "sh", "-c", shell)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	msg := strings.TrimSpace(string(out))
	if len(msg) > 2000 {
		msg = msg[len(msg)-2000:]
	}
	req.FinishedAt = time.Now().Unix()
	req.Mode = "docker"
	if err != nil {
		req.Status = "error"
		req.Message = fmt.Sprintf("%v: %s", err, msg)
	} else {
		req.Status = "done"
		if msg == "" {
			msg = "in-container docker update finished"
		}
		req.Message = msg
	}
	_ = writeUpdateStatus(options, req)
	if err == nil {
		_ = os.Remove(filepath.Join(dataDir(options), updateRequestFile))
	}
}

func runHotUpdateCmd(options Options, req updateRequest) {
	cmdLine := strings.TrimSpace(os.Getenv("GROK2API_HOT_UPDATE_CMD"))
	if cmdLine == "" {
		return
	}
	// Expand simple placeholders.
	cmdLine = strings.ReplaceAll(cmdLine, "{{TAG}}", req.Tag)
	cmdLine = strings.ReplaceAll(cmdLine, "{{IMAGE}}", req.Image)
	cmdLine = strings.ReplaceAll(cmdLine, "{{VERSION}}", req.Tag)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdLine)
	cmd.Env = append(os.Environ(),
		"GROK2API_UPDATE_TAG="+req.Tag,
		"GROK2API_UPDATE_IMAGE="+req.Image,
	)
	out, err := cmd.CombinedOutput()
	msg := strings.TrimSpace(string(out))
	if len(msg) > 2000 {
		msg = msg[len(msg)-2000:]
	}
	req.FinishedAt = time.Now().Unix()
	req.Mode = "cmd"
	if err != nil {
		req.Status = "error"
		req.Message = fmt.Sprintf("%v: %s", err, msg)
	} else {
		req.Status = "done"
		if msg == "" {
			msg = "update command finished"
		}
		req.Message = msg
	}
	_ = writeUpdateStatus(options, req)
	// Also clear request file on success.
	if err == nil {
		_ = os.Remove(filepath.Join(dataDir(options), updateRequestFile))
	}
}
