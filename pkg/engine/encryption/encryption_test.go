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
