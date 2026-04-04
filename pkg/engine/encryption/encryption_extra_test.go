package encryption

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// Test NewEncryptor with various key sizes
func TestNewEncryptor_KeySizes(t *testing.T) {
	tests := []struct {
		name    string
		keySize int
		wantErr bool
	}{
		{"16 bytes", 16, true},
		{"24 bytes", 24, true},
		{"31 bytes", 31, true},
		{"32 bytes", 32, false},
		{"33 bytes", 33, true},
		{"64 bytes", 64, true},
		{"0 bytes", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := make([]byte, tc.keySize)
			rand.Read(key)
			_, err := NewEncryptor(key)
			if tc.wantErr && err == nil {
				t.Errorf("expected error for %d-byte key", tc.keySize)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for 32-byte key: %v", err)
			}
		})
	}
}

// Test Encrypt with empty plaintext
func TestEncrypt_Empty(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	// Empty plaintext
	ct, err := enc.Encrypt([]byte{})
	if err != nil {
		t.Fatalf("Encrypt empty: %v", err)
	}
	if len(ct) != enc.Overhead() {
		t.Errorf("expected ciphertext size %d, got %d", enc.Overhead(), len(ct))
	}

	// Verify decryption
	pt, err := enc.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if len(pt) != 0 {
		t.Errorf("expected empty plaintext, got %d bytes", len(pt))
	}
}

// Test Decrypt with corrupted tag
func TestDecrypt_CorruptedTag(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	pt := []byte("secret data")
	ct, _ := enc.Encrypt(pt)

	// Corrupt the last byte (part of tag)
	ct[len(ct)-1] ^= 0xFF

	_, err := enc.Decrypt(ct)
	if err == nil {
		t.Error("expected error for corrupted tag")
	}
}

// Test Decrypt with corrupted ciphertext
func TestDecrypt_CorruptedCiphertext(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	pt := []byte("secret data that is long enough")
	ct, _ := enc.Encrypt(pt)

	// Corrupt middle byte (ciphertext, not nonce)
	ct[20] ^= 0xFF

	_, err := enc.Decrypt(ct)
	if err == nil {
		t.Error("expected error for corrupted ciphertext")
	}
}

// Test Encrypt/Decrypt with various sizes
func TestEncryptDecrypt_Sizes(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	sizes := []int{1, 16, 32, 100, 1024, 4096, 10000, 65536}

	for _, size := range sizes {
		pt := make([]byte, size)
		rand.Read(pt)

		ct, err := enc.Encrypt(pt)
		if err != nil {
			t.Fatalf("Encrypt %d bytes: %v", size, err)
		}

		decrypted, err := enc.Decrypt(ct)
		if err != nil {
			t.Fatalf("Decrypt %d bytes: %v", size, err)
		}

		if !bytes.Equal(decrypted, pt) {
			t.Errorf("round-trip failed for %d bytes", size)
		}
	}
}

// Test multiple Encrypt calls produce different ciphertexts (nonce uniqueness)
func TestEncrypt_NonceUniqueness(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	pt := []byte("same plaintext")
	ct1, _ := enc.Encrypt(pt)
	ct2, _ := enc.Encrypt(pt)
	ct3, _ := enc.Encrypt(pt)

	// All ciphertexts should be different due to random nonces
	if bytes.Equal(ct1, ct2) {
		t.Error("ct1 and ct2 should differ")
	}
	if bytes.Equal(ct1, ct3) {
		t.Error("ct1 and ct3 should differ")
	}
	if bytes.Equal(ct2, ct3) {
		t.Error("ct2 and ct3 should differ")
	}

	// But all should decrypt to same plaintext
	for i, ct := range [][]byte{ct1, ct2, ct3} {
		decrypted, err := enc.Decrypt(ct)
		if err != nil {
			t.Fatalf("decrypt ct%d: %v", i+1, err)
		}
		if !bytes.Equal(decrypted, pt) {
			t.Errorf("ct%d decrypted incorrectly", i+1)
		}
	}
}

// Test Overhead consistency
func TestOverhead_Consistency(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	overhead := enc.Overhead()

	// Test with various plaintext sizes
	for _, size := range []int{0, 1, 100, 1000} {
		pt := make([]byte, size)
		ct, _ := enc.Encrypt(pt)
		if len(ct) != size+overhead {
			t.Errorf("size %d: expected ct len %d, got %d", size, size+overhead, len(ct))
		}
	}
}
