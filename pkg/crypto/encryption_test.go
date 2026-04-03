package crypto

import (
	"bytes"
	"testing"
)

func TestProvider_EncryptDecrypt(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	provider, err := NewProvider(EncryptionConfig{
		Key:              key,
		EnableEncryption: true,
	})
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	tests := []struct {
		name      string
		plaintext string
	}{
		{"simple", "hello world"},
		{"empty", ""},
		{"unicode", "日本語テキスト"},
		{"binary-like", "\x00\x01\x02\xff\xfe"},
		{"long", string(make([]byte, 10000))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plaintext := []byte(tt.plaintext)

			// Encrypt
			ciphertext, err := provider.Encrypt(plaintext)
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}

			// Ciphertext should be different from plaintext
			if len(plaintext) > 0 && bytes.Equal(ciphertext, plaintext) {
				t.Error("Ciphertext should differ from plaintext")
			}

			// Decrypt
			decrypted, err := provider.Decrypt(ciphertext)
			if err != nil {
				t.Fatalf("Decrypt failed: %v", err)
			}

			// Should match original
			if !bytes.Equal(decrypted, plaintext) {
				t.Error("Decrypted text doesn't match original")
			}
		})
	}
}

func TestProvider_Disabled(t *testing.T) {
	provider, err := NewProvider(EncryptionConfig{
		EnableEncryption: false,
	})
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	plaintext := []byte("test data")

	// When disabled, Encrypt should return plaintext
	ciphertext, err := provider.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if !bytes.Equal(ciphertext, plaintext) {
		t.Error("When disabled, Encrypt should return plaintext")
	}

	// Decrypt should also return plaintext
	decrypted, err := provider.Decrypt(plaintext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("When disabled, Decrypt should return input")
	}
}

func TestProvider_StringEncryption(t *testing.T) {
	key, _ := GenerateKey()
	provider, _ := NewProvider(EncryptionConfig{
		Key:              key,
		EnableEncryption: true,
	})

	plaintext := "secret message"

	// Encrypt
	ciphertext, err := provider.EncryptString(plaintext)
	if err != nil {
		t.Fatalf("EncryptString failed: %v", err)
	}

	// Decrypt
	decrypted, err := provider.DecryptString(ciphertext)
	if err != nil {
		t.Fatalf("DecryptString failed: %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("Expected %q, got %q", plaintext, decrypted)
	}
}

func TestGenerateKey(t *testing.T) {
	key1, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	if len(key1) != 32 {
		t.Errorf("Expected 32 bytes, got %d", len(key1))
	}

	key2, _ := GenerateKey()

	// Keys should be random (different each time)
	if bytes.Equal(key1, key2) {
		t.Error("Generated keys should be random")
	}
}

func TestSecureCompare(t *testing.T) {
	a := []byte("secret")
	b := []byte("secret")
	c := []byte("different")

	if !SecureCompare(a, b) {
		t.Error("SecureCompare should return true for equal slices")
	}

	if SecureCompare(a, c) {
		t.Error("SecureCompare should return false for different slices")
	}

	// Empty slices
	if !SecureCompare([]byte{}, []byte{}) {
		t.Error("SecureCompare should handle empty slices")
	}
}

func TestEncryptionConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  EncryptionConfig
		wantErr bool
	}{
		{
			name: "valid 32-byte key",
			config: EncryptionConfig{
				Key:              make([]byte, 32),
				EnableEncryption: true,
			},
			wantErr: false,
		},
		{
			name: "disabled with nil key",
			config: EncryptionConfig{
				EnableEncryption: false,
			},
			wantErr: false,
		},
		{
			name: "invalid 16-byte key",
			config: EncryptionConfig{
				Key:              make([]byte, 16),
				EnableEncryption: true,
			},
			wantErr: true,
		},
		{
			name: "invalid 64-byte key",
			config: EncryptionConfig{
				Key:              make([]byte, 64),
				EnableEncryption: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestProvider_IsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		config   EncryptionConfig
		expected bool
	}{
		{
			name:     "enabled",
			config:   EncryptionConfig{EnableEncryption: true, Key: make([]byte, 32)},
			expected: true,
		},
		{
			name:     "disabled",
			config:   EncryptionConfig{EnableEncryption: false},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := NewProvider(tt.config)
			if err != nil {
				t.Fatalf("NewProvider failed: %v", err)
			}

			if provider.IsEnabled() != tt.expected {
				t.Errorf("IsEnabled() = %v, want %v", provider.IsEnabled(), tt.expected)
			}
		})
	}
}

func TestEncryptWithKey_DecryptWithKey(t *testing.T) {
	key, _ := GenerateKey()
	plaintext := []byte("secret data for key-specific encryption")

	// Encrypt with specific key
	ciphertext, err := EncryptWithKey(plaintext, key)
	if err != nil {
		t.Fatalf("EncryptWithKey failed: %v", err)
	}

	// Ciphertext should be different from plaintext
	if bytes.Equal(ciphertext, plaintext) {
		t.Error("Ciphertext should differ from plaintext")
	}

	// Decrypt with same key
	decrypted, err := DecryptWithKey(ciphertext, key)
	if err != nil {
		t.Fatalf("DecryptWithKey failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("Decrypted data doesn't match original")
	}
}

func TestEncryptWithKey_InvalidKey(t *testing.T) {
	plaintext := []byte("test data")

	// Test with short key
	_, err := EncryptWithKey(plaintext, make([]byte, 16))
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey, got %v", err)
	}

	// Test with long key
	_, err = EncryptWithKey(plaintext, make([]byte, 64))
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey, got %v", err)
	}
}

func TestDecryptWithKey_InvalidKey(t *testing.T) {
	ciphertext := []byte("test data")

	// Test with short key
	_, err := DecryptWithKey(ciphertext, make([]byte, 16))
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey, got %v", err)
	}
}

func TestDecryptWithKey_CorruptData(t *testing.T) {
	key, _ := GenerateKey()

	// Test with data too short to contain nonce
	_, err := DecryptWithKey([]byte("short"), key)
	if err != ErrDecryptionFailed {
		t.Errorf("Expected ErrDecryptionFailed for short data, got %v", err)
	}

	// Test with corrupted ciphertext (valid size but wrong content)
	corruptData := make([]byte, 32) // Large enough for nonce + some data
	_, err = DecryptWithKey(corruptData, key)
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestDecryptWithKey_WrongKey(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()
	plaintext := []byte("secret data")

	// Encrypt with key1
	ciphertext, _ := EncryptWithKey(plaintext, key1)

	// Try to decrypt with key2
	_, err := DecryptWithKey(ciphertext, key2)
	if err == nil {
		t.Error("Expected error when decrypting with wrong key")
	}
}

func BenchmarkEncrypt(b *testing.B) {
	key, _ := GenerateKey()
	provider, _ := NewProvider(EncryptionConfig{
		Key:              key,
		EnableEncryption: true,
	})

	plaintext := []byte("benchmark data for encryption testing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		provider.Encrypt(plaintext)
	}
}

func BenchmarkDecrypt(b *testing.B) {
	key, _ := GenerateKey()
	provider, _ := NewProvider(EncryptionConfig{
		Key:              key,
		EnableEncryption: true,
	})

	plaintext := []byte("benchmark data for encryption testing")
	ciphertext, _ := provider.Encrypt(plaintext)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		provider.Decrypt(ciphertext)
	}
}
