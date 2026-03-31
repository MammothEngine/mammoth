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
