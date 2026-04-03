package encryption

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}

	plaintext := []byte("Hello, Encryption!")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("round-trip mismatch: got %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptDecryptEmpty(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)

	ct, err := enc.Encrypt([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	pt, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}
	if len(pt) != 0 {
		t.Errorf("expected empty, got %d bytes", len(pt))
	}
}

func TestEncryptDecryptLargeData(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)

	data := make([]byte, 65536)
	rand.Read(data)

	ct, err := enc.Encrypt(data)
	if err != nil {
		t.Fatal(err)
	}
	pt, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pt, data) {
		t.Error("large data round-trip mismatch")
	}
}

func TestWrongKeyFails(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	enc1, _ := NewEncryptor(key1)
	enc2, _ := NewEncryptor(key2)

	ct, _ := enc1.Encrypt([]byte("secret"))
	_, err := enc2.Decrypt(ct)
	if err == nil {
		t.Error("expected error decrypting with wrong key")
	}
}

func TestInvalidKeySize(t *testing.T) {
	_, err := NewEncryptor([]byte("short"))
	if err == nil {
		t.Error("expected error for short key")
	}
}

func TestCiphertextTooShort(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)
	_, err := enc.Decrypt([]byte{0x01, 0x02})
	if err == nil {
		t.Error("expected error for short ciphertext")
	}
}

func TestCiphertextNotModified(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)

	ct, _ := enc.Encrypt([]byte("test"))
	ct[0]++ // corrupt nonce
	_, err := enc.Decrypt(ct)
	if err == nil {
		t.Error("expected error for corrupted ciphertext")
	}
}

func TestOverhead(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)
	overhead := enc.Overhead()
	if overhead <= 0 {
		t.Errorf("expected positive overhead, got %d", overhead)
	}

	pt := []byte("hello")
	ct, _ := enc.Encrypt(pt)
	if len(ct) != len(pt)+overhead {
		t.Errorf("ciphertext size: got %d, expected %d+%d=%d", len(ct), len(pt), overhead, len(pt)+overhead)
	}
}

func TestKeyManagerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "keys.json")
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, err := NewKeyManager(masterKey, keyFile)
	if err != nil {
		t.Fatal(err)
	}

	enc, err := km.GetEncryptor("testdb")
	if err != nil {
		t.Fatal(err)
	}

	pt := []byte("secret data")
	ct, err := enc.Encrypt(pt)
	if err != nil {
		t.Fatal(err)
	}

	dec, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dec, pt) {
		t.Error("key manager encryptor round-trip failed")
	}

	// Verify keys persisted
	if _, err := os.Stat(keyFile); err != nil {
		t.Errorf("key file not created: %v", err)
	}

	// Reload and verify
	km2, err := NewKeyManager(masterKey, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	enc2, err := km2.GetEncryptor("testdb")
	if err != nil {
		t.Fatal(err)
	}
	dec2, err := enc2.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dec2, pt) {
		t.Error("reloaded key manager decrypt failed")
	}
}

func TestKeyManagerRotation(t *testing.T) {
	dir := t.TempDir()
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, filepath.Join(dir, "keys.json"))
	enc1, _ := km.GetEncryptor("mydb")
	ct, _ := enc1.Encrypt([]byte("data"))

	// Rotate
	if err := km.RotateDEK("mydb"); err != nil {
		t.Fatal(err)
	}

	enc2, _ := km.GetEncryptor("mydb")
	// Old ciphertext should NOT decrypt with new DEK
	_, err := enc2.Decrypt(ct)
	if err == nil {
		t.Error("old ciphertext should not decrypt after rotation")
	}
}

func TestMasterKeyFromEnv(t *testing.T) {
	// Set a valid hex key
	os.Setenv("MAMMOTH_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	defer os.Unsetenv("MAMMOTH_ENCRYPTION_KEY")

	key, err := MasterKeyFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(key))
	}
}

func TestMasterKeyFromEnvInvalid(t *testing.T) {
	os.Setenv("MAMMOTH_ENCRYPTION_KEY", "tooshort")
	defer os.Unsetenv("MAMMOTH_ENCRYPTION_KEY")

	_, err := MasterKeyFromEnv()
	if err == nil {
		t.Error("expected error for short env key")
	}
}

func TestMasterKeyFromFile(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "master.key")
	key := make([]byte, 32)
	rand.Read(key)
	os.WriteFile(keyFile, key, 0600)

	loaded, err := MasterKeyFromFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loaded, key) {
		t.Error("key mismatch")
	}
}

// Test hexVal function
func TestHexVal(t *testing.T) {
	tests := []struct {
		input    byte
		expected int
	}{
		{'0', 0},
		{'1', 1},
		{'5', 5},
		{'9', 9},
		{'a', 10},
		{'b', 11},
		{'f', 15},
		{'A', 10},
		{'B', 11},
		{'F', 15},
		{'g', -1},    // invalid
		{'z', -1},    // invalid
		{'!', -1},    // invalid
		{' ', -1},    // invalid
		{0x00, -1},   // invalid
		{0xFF, -1},   // invalid
	}

	for _, tt := range tests {
		result := hexVal(tt.input)
		if result != tt.expected {
			t.Errorf("hexVal(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

// Test hexByte function
func TestHexByte(t *testing.T) {
	tests := []struct {
		h1       byte
		h2       byte
		expected int
	}{
		{'0', '0', 0},
		{'0', 'F', 15},
		{'F', '0', 240},
		{'F', 'F', 255},
		{'1', '2', 18},  // 0x12 = 18
		{'a', 'b', 171}, // 0xAB = 171
		{'A', 'B', 171}, // 0xAB = 171
	}

	for _, tt := range tests {
		result := hexByte(tt.h1, tt.h2)
		if result != tt.expected {
			t.Errorf("hexByte(%q, %q) = %d, want %d", tt.h1, tt.h2, result, tt.expected)
		}
	}
}

// Test MasterKeyFromFile with non-existent file
func TestMasterKeyFromFile_NotFound(t *testing.T) {
	_, err := MasterKeyFromFile("/nonexistent/path/to/key")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

// Test MasterKeyFromFile with wrong size key
func TestMasterKeyFromFile_WrongSize(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "master.key")

	// Write 16 bytes instead of 32
	os.WriteFile(keyFile, []byte("shortkey12345678"), 0600)

	_, err := MasterKeyFromFile(keyFile)
	if err == nil {
		t.Error("expected error for wrong size key")
	}

	// Write 33 bytes instead of 32
	os.WriteFile(keyFile, []byte("longkey012345678901234567890123456789"), 0600)

	_, err = MasterKeyFromFile(keyFile)
	if err == nil {
		t.Error("expected error for wrong size key")
	}
}

// Test KeyManager without key file (in-memory only)
func TestKeyManager_NoKeyFile(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	// No key file - in-memory only
	km, err := NewKeyManager(masterKey, "")
	if err != nil {
		t.Fatal(err)
	}

	// Should still work
	enc, err := km.GetEncryptor("testdb")
	if err != nil {
		t.Fatal(err)
	}

	pt := []byte("secret data")
	ct, err := enc.Encrypt(pt)
	if err != nil {
		t.Fatal(err)
	}

	dec, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(dec, pt) {
		t.Error("in-memory key manager failed")
	}
}

// Test saveKeys when keyFile is empty
func TestKeyManager_SaveKeys_NoFile(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	// saveKeys should return nil when no keyFile
	err := km.saveKeys()
	if err != nil {
		t.Errorf("saveKeys should return nil when no keyFile: %v", err)
	}
}

// Test loadKeys with invalid JSON
func TestKeyManager_LoadKeys_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "keys.json")
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	// Write invalid JSON
	os.WriteFile(keyFile, []byte("not valid json"), 0600)

	// Should still create KeyManager but loadKeys will fail silently
	km, err := NewKeyManager(masterKey, keyFile)
	if err != nil {
		t.Fatal(err)
	}

	// DEKs should be empty (failed to load)
	if len(km.deks) != 0 {
		t.Error("DEKs should be empty after failed load")
	}
}

// Test loadKeys with non-existent file
func TestKeyManager_LoadKeys_NoFile(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "nonexistent.json")
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	// File doesn't exist - should create empty KeyManager
	km, err := NewKeyManager(masterKey, keyFile)
	if err != nil {
		t.Fatal(err)
	}

	// Should work normally
	enc, err := km.GetEncryptor("testdb")
	if err != nil {
		t.Fatal(err)
	}

	pt := []byte("test")
	ct, _ := enc.Encrypt(pt)
	dec, _ := enc.Decrypt(ct)
	if !bytes.Equal(dec, pt) {
		t.Error("encryption failed after load from non-existent file")
	}
}

// Test GetEncryptor for new database creates DEK
func TestKeyManager_GetEncryptor_NewDB(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	// Get encryptor for new DB
	enc1, err := km.GetEncryptor("db1")
	if err != nil {
		t.Fatal(err)
	}

	// Get encryptor for another new DB
	enc2, err := km.GetEncryptor("db2")
	if err != nil {
		t.Fatal(err)
	}

	// Should be different encryptors (different DEKs)
	pt := []byte("test")
	ct1, _ := enc1.Encrypt(pt)
	ct2, _ := enc2.Encrypt(pt)

	// Same plaintext should encrypt to different ciphertext
	if bytes.Equal(ct1, ct2) {
		t.Error("different DBs should have different ciphertexts")
	}

	// But each should decrypt correctly
	dec1, _ := enc1.Decrypt(ct1)
	dec2, _ := enc2.Decrypt(ct2)

	if !bytes.Equal(dec1, pt) || !bytes.Equal(dec2, pt) {
		t.Error("decryption failed")
	}
}

// Test RotateDEKLocked directly
func TestRotateDEKLocked(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "keys.json")
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, keyFile)

	// Rotate for new DB
	err := km.RotateDEKLocked("testdb")
	if err != nil {
		t.Fatal(err)
	}

	// Verify DEK was created
	if _, ok := km.deks["testdb"]; !ok {
		t.Error("DEK should be created")
	}

	// Verify keys were persisted
	if _, err := os.Stat(keyFile); err != nil {
		t.Error("key file should be created after RotateDEKLocked")
	}
}

// Test RotateDEK for non-existent DB
func TestRotateDEK_NewDB(t *testing.T) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	km, _ := NewKeyManager(masterKey, "")

	// Rotate for DB that doesn't have a DEK yet
	err := km.RotateDEK("newdb")
	if err != nil {
		t.Fatal(err)
	}

	// Should be able to get encryptor
	enc, err := km.GetEncryptor("newdb")
	if err != nil {
		t.Fatal(err)
	}

	// Should work
	pt := []byte("test")
	ct, _ := enc.Encrypt(pt)
	dec, _ := enc.Decrypt(ct)
	if !bytes.Equal(dec, pt) {
		t.Error("encryption after rotation failed")
	}
}
