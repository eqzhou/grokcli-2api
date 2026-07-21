package auth

import "testing"

func TestPythonCompatibilityVector(t *testing.T) {
	const salt = "0123456789abcdef0123456789abcdef"
	const hash = "76fb0ab5903effdab16fe4509d3dfe16ed37b77f08be080b3195e3682b772af2"
	if !VerifyPassword("secret", hash, salt) {
		t.Fatal("failed to verify Python settings_store.py vector")
	}
}

func TestPasswordRoundTrip(t *testing.T) {
	hash, salt, err := NewPassword("a-strong-password")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword("a-strong-password", hash, salt) || VerifyPassword("wrong", hash, salt) {
		t.Fatal("password verification mismatch")
	}
}

func TestNewPasswordRejectsWeakPassword(t *testing.T) {
	if _, _, err := NewPassword("short"); err == nil {
		t.Fatal("weak password accepted")
	}
}

func TestNewPasswordCountsCharactersAndCapsBytes(t *testing.T) {
	if _, _, err := NewPassword("密码密码密码密码"); err == nil {
		t.Fatal("four multibyte characters bypassed minimum length")
	}
	if _, _, err := NewPassword("密码密码密码密码密码密码密码密码密码密码密码密码"); err != nil {
		t.Fatalf("twelve characters rejected: %v", err)
	}
	if _, _, err := NewPassword(string(make([]byte, 257))); err == nil {
		t.Fatal("oversized password accepted")
	}
}
