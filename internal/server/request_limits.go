package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

var errTooManyAuthReaders = errors.New("too many concurrent authentication requests")

const (
	maxPublicRequestBodyBytes = 32 << 20
	maxAdminAuthBodyBytes     = 8 << 10
	maxAdminJSONBodyBytes     = 2 << 20
	maxAdminUploadBodyBytes   = 128 << 20
)

func withRequestBodyLimits(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.Method != http.MethodGet && r.Method != http.MethodHead {
			limit := int64(maxPublicRequestBodyBytes)
			switch r.URL.Path {
			case "/admin/api/login", "/admin/api/setup", "/admin/api/settings/password":
				limit = maxAdminAuthBodyBytes
			case "/admin/api/accounts/import-file", "/admin/api/accounts/import-files":
				limit = maxAdminUploadBodyBytes
			}
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next.ServeHTTP(w, r)
	})
}

func decodeJSONRequest(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	if limit <= 0 {
		limit = maxAdminJSONBodyBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	return decoder.Decode(dst)
}

func decodeAdminAuthRequest(w http.ResponseWriter, r *http.Request, dst any) error {
	select {
	case adminAuthReadSlots <- struct{}{}:
		defer func() { <-adminAuthReadSlots }()
	default:
		return errTooManyAuthReaders
	}
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = controller.SetReadDeadline(time.Time{}) }()
	return decodeJSONRequest(w, r, dst, maxAdminAuthBodyBytes)
}

func statusForDecodeError(err error) int {
	if errors.Is(err, errTooManyAuthReaders) {
		return http.StatusTooManyRequests
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}
