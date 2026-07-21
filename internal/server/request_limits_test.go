package server

import (
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeJSONRequestRejectsOversizedBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/admin/api/login", strings.NewReader(`{"password":"`+strings.Repeat("x", 64)+`"}`))
	rec := httptest.NewRecorder()
	var body map[string]any
	err := decodeJSONRequest(rec, req, &body, 32)
	var tooLarge *http.MaxBytesError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("error=%v, want http.MaxBytesError", err)
	}
	if statusForDecodeError(err) != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d", statusForDecodeError(err))
	}
}

func TestDecodeJSONRequestAcceptsBoundedBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/admin/api/login", strings.NewReader(`{"password":"bounded"}`))
	rec := httptest.NewRecorder()
	var body map[string]any
	if err := decodeJSONRequest(rec, req, &body, 128); err != nil {
		t.Fatal(err)
	}
	if body["password"] != "bounded" {
		t.Fatalf("body=%v", body)
	}
}

func TestSecurityHeadersApplied(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://admin.example.com/health", nil)
	req.TLS = &tls.ConnectionState{}
	rec := httptest.NewRecorder()
	NewMigrationMux(func() bool { return true }).ServeHTTP(rec, req)
	for name, want := range map[string]string{
		"X-Content-Type-Options":    "nosniff",
		"X-Frame-Options":           "DENY",
		"Referrer-Policy":           "no-referrer",
		"Strict-Transport-Security": "max-age=31536000; includeSubDomains",
	} {
		if got := rec.Header().Get(name); got != want {
			t.Errorf("%s=%q want %q", name, got, want)
		}
	}
	if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Fatalf("missing CSP frame protection: %q", rec.Header().Get("Content-Security-Policy"))
	}
}
