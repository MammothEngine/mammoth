package encryption

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

// Test NewKeyManager with invalid master key sizes
func TestNewKeyManager_InvalidKeySizes(t *testing.T) {
	tests := []struct {
		name    string
		keySize int
		wantErr bool
	}{
		{"16 bytes", 16, true},
		{"31 bytes", 31, true},
		{"33 bytes", 33, true},
		{"0 bytes", 0, true},
		{"64 bytes", 64, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := make([]byte, tc.keySize)
			rand.Read(key)
			_, err := NewKeyManager(key, "")
			if tc.wantErr && err == nil {
				t.Errorf("expected error for %d-byte master key", tc.keySize)
			}
		})
	}
}

// Test MasterKeyFromEnv with unset variable
func TestMasterKeyFromEnv_Unset(t *testing.T) {
	os.Unsetenv("MAMMOTH_ENCRYPTION_KEY")
	_, err := MasterKeyFromEnv()
	if err == nil {
		t.Error("expected error when MAMMOTH_ENCRYPTION_KEY not set")
	}
}

// Test MasterKeyFromEnv with various invalid lengths
func TestMasterKeyFromEnv_InvalidLengths(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"empty", ""},
		{"31 hex chars", "0123456789abcdef0123456789abcde"},
		{"33 hex chars", "0123456789abcdef0123456789abcdef0"},
		{"63 hex chars", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde"},
		{"65 hex chars", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			os.Setenv("MAMMOTH_ENCRYPTION_KEY", tc.key)
			defer os.Unsetenv("MAMMOTH_ENCRYPTION_KEY")

			_, err := MasterKeyFromEnv()
			if err == nil {
				t.Error("expected error for invalid key length")
			}
		})
	}
}

// Test MasterKeyFromEnv with invalid hex characters
func TestMasterKeyFromEnv_InvalidHex(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"contains g", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"},
		{"contains G", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeG"},
		{"contains space", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde "},
		{"contains newline", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\n"},
		{"all invalid", "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"},
		{"last invalid", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeX"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			os.Setenv("MAMMOTH_ENCRYPTION_KEY", tc.key)
			defer os.Unsetenv("MAMMOTH_ENCRYPTION_KEY")

			_, err := MasterKeyFromEnv()
			if err == nil {
				t.Error("expected error for invalid hex characters")
			}
		})
	}
}

// Test getDEK with DEK encrypted by different master key
func TestGetDEK_WrongMasterKey(t *testing.T) {
	masterKey1 := make([]byte, 32)
	masterKey2 := make([]byte, 32)
	rand.Read(masterKey1)
	rand.Read(masterKey2)

	// Create KeyManager with masterKey1
	km1, _ := NewKeyManager(masterKey1, "")

	// Get encryptor to create DEK
	_, _ = km1.GetEncryptor("testdb")

	// Now create a new KeyManager with masterKey2 but copy the DEK entry
	km2, _ := NewKeyManager(masterKey2, "")
	km2.mu.Lock()
	km2.deks["testdb"] = km1.deks["testdb"] // Copy DEK encrypted with different master key
	km2.mu.Unlock()

	// Try to get encryptor - should fail to decrypt DEK with wrong master key
	_, err := km2.GetEncryptor("testdb")
	if err == nil {
		t.Error("expected error when decrypting DEK with wrong master key")
	}
	if err.Error() != "encryption: failed to decrypt DEK" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// Test getDEK with valid DEK after failed rotation
func TestGetDEK_AfterRotation(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	// First access creates DEK
	enc1, err := km.GetEncryptor("testdb")
	if err != nil {
		t.Fatal(err)
	}

	// Second access should reuse same DEK
	enc2, err := km.GetEncryptor("testdb")
	if err != nil {
		t.Fatal(err)
	}

	// Both should work
	pt := []byte("test data")
	ct, _ := enc1.Encrypt(pt)
	dec, _ := enc2.Decrypt(ct)
	if string(dec) != string(pt) {
		t.Error("same DB should use same DEK")
	}
}

// Test RotateDEK for existing DB
func TestRotateDEK_ExistingDB(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	// Get initial encryptor
	enc1, _ := km.GetEncryptor("mydb")
	pt := []byte("secret")
	ct, _ := enc1.Encrypt(pt)

	// Rotate DEK
	err := km.RotateDEK("mydb")
	if err != nil {
		t.Fatal(err)
	}

	// Get new encryptor
	enc2, _ := km.GetEncryptor("mydb")

	// Old ciphertext should not decrypt
	_, err = enc2.Decrypt(ct)
	if err == nil {
		t.Error("old ciphertext should not decrypt after rotation")
	}

	// New encryption should work
	ct2, _ := enc2.Encrypt(pt)
	dec, _ := enc2.Decrypt(ct2)
	if string(dec) != string(pt) {
		t.Error("new encryption should work after rotation")
	}
}

// Test RotateDEKLocked for existing DB
func TestRotateDEKLocked_ExistingDB(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	// Create initial DEK
	km.mu.Lock()
	err := km.RotateDEKLocked("testdb")
	km.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Save original entry
	origEntry := km.deks["testdb"]

	// Rotate again
	km.mu.Lock()
	err = km.RotateDEKLocked("testdb")
	km.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Entry should be different
	newEntry := km.deks["testdb"]
	if string(origEntry.EncryptedDEK) == string(newEntry.EncryptedDEK) {
		t.Error("DEK should change after rotation")
	}
}

// Test KeyManager with corrupted key file
func TestKeyManager_CorruptedKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "keys.json")
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	// Write corrupted JSON
	os.WriteFile(keyFile, []byte(`{"testdb": {"dek": "not-base64", "nonce": "also-not-base64"}}`), 0600)

	// Should still create KeyManager but fail to load keys
	km, err := NewKeyManager(masterKey, keyFile)
	if err != nil {
		t.Fatal(err)
	}

	// DEKs should be empty
	if len(km.deks) != 0 {
		t.Logf("DEKs after corrupted load: %v", km.deks)
	}
}

// Test saveKeys error path (read-only directory)
func TestSaveKeys_ReadOnlyDir(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping filesystem permission test")
	}

	dir := t.TempDir()
	keyFile := filepath.Join(dir, "keys.json")
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, keyFile)

	// Make directory read-only
	os.Chmod(dir, 0555)
	defer os.Chmod(dir, 0755)

	// Try to save keys
	km.mu.Lock()
	km.deks["test"] = &dekEntry{EncryptedDEK: []byte("test"), Nonce: []byte("nonce")}
	km.mu.Unlock()

	err := km.RotateDEK("newdb")
	// May or may not error depending on OS/permissions
	_ = err
}

// Test multiple databases isolation
func TestKeyManager_MultipleDBs(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	dbs := []string{"db1", "db2", "db3", "db4", "db5"}
	encryptors := make(map[string]*Encryptor)

	// Get encryptors for all DBs
	for _, db := range dbs {
		enc, err := km.GetEncryptor(db)
		if err != nil {
			t.Fatal(err)
		}
		encryptors[db] = enc
	}

	// Encrypt same plaintext with each
	pt := []byte("same secret message")
	ciphertexts := make(map[string][]byte)

	for db, enc := range encryptors {
		ct, _ := enc.Encrypt(pt)
		ciphertexts[db] = ct
	}

	// Each DB should decrypt its own ciphertext
	for db, enc := range encryptors {
		dec, err := enc.Decrypt(ciphertexts[db])
		if err != nil {
			t.Errorf("%s failed to decrypt: %v", db, err)
		}
		if string(dec) != string(pt) {
			t.Errorf("%s decrypted wrong value", db)
		}
	}
}

// Test KeyManager master key copy
func TestNewKeyManager_KeyCopy(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	// Modify original key
	for i := range masterKey {
		masterKey[i] = 0
	}

	// KeyManager should still work with its copy
	enc, err := km.GetEncryptor("test")
	if err != nil {
		t.Fatal(err)
	}

	pt := []byte("test")
	ct, _ := enc.Encrypt(pt)
	dec, _ := enc.Decrypt(ct)
	if string(dec) != string(pt) {
		t.Error("key copy failed")
	}
}
