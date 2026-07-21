package postgres

import "testing"

func TestAPIKeyPublicMapNeverReturnsStoredSecret(t *testing.T) {
	legacy := "sk-g2a-legacy-plaintext"
	out := (APIKeyRecord{ID: "key-1", Prefix: "sk-g2a-legac", Secret: &legacy}).PublicMap()
	if _, leaked := out["secret"]; leaked {
		t.Fatalf("public map leaked API key: %#v", out)
	}
}
