package auth

import (
	"testing"
)

func TestDeriveAndVerifyKeys(t *testing.T) {
	salt := GenerateSalt()
	storedKey, serverKey := DeriveKeys("mypassword", salt, 10000)
	if len(storedKey) != 32 {
		t.Errorf("stored key len: %d", len(storedKey))
	}
	if len(serverKey) != 32 {
		t.Errorf("server key len: %d", len(serverKey))
	}
	// Same password and salt should give same keys
	sk2, svk2 := DeriveKeys("mypassword", salt, 10000)
	if string(storedKey) != string(sk2) {
		t.Error("stored key mismatch")
	}
	if string(serverKey) != string(svk2) {
		t.Error("server key mismatch")
	}
}
