package auth

import (
	"encoding/base64"
	"testing"
)

func TestDeriveKeysConsistency(t *testing.T) {
	salt := []byte("testsalt12345678")
	sk1, srv1 := DeriveKeys("password123", salt, 10000)
	sk2, srv2 := DeriveKeys("password123", salt, 10000)
	if len(sk1) != 32 {
		t.Errorf("stored key length: %d", len(sk1))
	}
	if string(sk1) != string(sk2) {
		t.Error("keys not deterministic")
	}
	if string(srv1) != string(srv2) {
		t.Error("keys not deterministic")
	}
}

func TestDeriveKeysDifferentPasswords(t *testing.T) {
	salt := []byte("testsalt12345678")
	sk1, _ := DeriveKeys("password1", salt, 10000)
	sk2, _ := DeriveKeys("password2", salt, 10000)
	if string(sk1) == string(sk2) {
		t.Error("different passwords should produce different keys")
	}
}

func TestSCRAMFullExchange(t *testing.T) {
	salt := GenerateSalt()
	storedKey, serverKey := DeriveKeys("testpass", salt, defaultIterations)

	fakeStore := &UserStore{
		getUserFn: func(username string) (*UserRecord, error) {
			return &UserRecord{
				Username:   "testuser",
				Salt:       salt,
				Iterations: defaultIterations,
				StoredKey:  storedKey,
				ServerKey:  serverKey,
			}, nil
		},
	}

	session, result, err := StartSCRAM(fakeStore, "testuser", "n,,n=testuser,r=clientnonce123")
	if err != nil {
		t.Fatalf("StartSCRAM: %v", err)
	}
	if result.Done {
		t.Error("should not be done after first step")
	}

	// Compute client proof
	serverNonce := session.serverNonce
	clientFirstBare := "n=testuser,r=clientnonce123"
	clientFinalBare := "c=biws,r=" + serverNonce
	authMessage := clientFirstBare + "," + result.Data + "," + clientFinalBare

	saltedPassword := pbkdf2SHA256([]byte("testpass"), salt, defaultIterations)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	clientSig := hmacSHA256(storedKey, []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSig[i]
	}

	clientFinal := "c=biws,r=" + serverNonce + ",p=" + base64.StdEncoding.EncodeToString(proof)
	finalResult, err := session.Continue(clientFinal)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if !finalResult.Done {
		t.Error("should be done after final step")
	}
}

func TestSCRAMWrongPassword(t *testing.T) {
	salt := GenerateSalt()
	storedKey, serverKey := DeriveKeys("correctpass", salt, defaultIterations)

	fakeStore := &UserStore{
		getUserFn: func(username string) (*UserRecord, error) {
			return &UserRecord{
				Username:   "testuser",
				Salt:       salt,
				Iterations: defaultIterations,
				StoredKey:  storedKey,
				ServerKey:  serverKey,
			}, nil
		},
	}

	session, _, err := StartSCRAM(fakeStore, "testuser", "n,,n=testuser,r=clientnonce456")
	if err != nil {
		t.Fatal(err)
	}

	// Compute proof with WRONG password
	serverNonce := session.serverNonce
	clientFirstBare := "n=testuser,r=clientnonce456"
	clientFinalBare := "c=biws,r=" + serverNonce
	authMessage := clientFirstBare + "," + session.serverFirst + "," + clientFinalBare

	wrongSaltedPassword := pbkdf2SHA256([]byte("wrongpass"), salt, defaultIterations)
	wrongClientKey := hmacSHA256(wrongSaltedPassword, []byte("Client Key"))
	clientSig := hmacSHA256(storedKey, []byte(authMessage))
	proof := make([]byte, len(wrongClientKey))
	for i := range wrongClientKey {
		proof[i] = wrongClientKey[i] ^ clientSig[i]
	}

	clientFinal := "c=biws,r=" + serverNonce + ",p=" + base64.StdEncoding.EncodeToString(proof)
	_, err = session.Continue(clientFinal)
	if err == nil {
		t.Error("should fail with wrong password")
	}
}
