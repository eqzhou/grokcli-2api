package server

import (
	"net/http"
	"testing"
	"time"
)

func TestRegistrationStartHTTPUsesDedicatedLongHeaderTimeout(t *testing.T) {
	pollClient := sharedRegistrationHTTP()
	pollTransport, ok := pollClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("poll transport type = %T", pollClient.Transport)
	}

	startClient := registrationStartHTTP()
	startTransport, ok := startClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("start transport type = %T", startClient.Transport)
	}
	if startTransport == pollTransport {
		t.Fatal("start requests must not share the poll transport timeout")
	}
	secondStartClient := registrationStartHTTP()
	if secondStartClient != startClient {
		t.Fatal("start requests must reuse one dedicated client")
	}
	if secondStartClient.Transport != startTransport {
		t.Fatal("start requests must reuse one dedicated transport")
	}
	if startClient.Timeout < 60*time.Second {
		t.Fatalf("start timeout = %s, want at least 60s", startClient.Timeout)
	}
	if startTransport.ResponseHeaderTimeout < 55*time.Second {
		t.Fatalf("response header timeout = %s, want at least 55s", startTransport.ResponseHeaderTimeout)
	}
	if pollTransport.ResponseHeaderTimeout > time.Second {
		t.Fatalf("poll response header timeout changed to %s", pollTransport.ResponseHeaderTimeout)
	}
}
