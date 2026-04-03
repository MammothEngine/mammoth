package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrInvalidKey is returned when the encryption key is invalid
	ErrInvalidKey = errors.New("crypto: invalid encryption key")
	// ErrDecryptionFailed is returned when decryption fails
	ErrDecryptionFailed = errors.New("crypto: decryption failed")
)

// EncryptionConfig configures the encryption provider.
type EncryptionConfig struct {
	// Key is the AES-256 key (32 bytes)
	Key []byte
	// EnableEncryption determines if encryption is active
	EnableEncryption bool
	// KeyRotationInterval is the interval for key rotation (0 = disabled)
	KeyRotationInterval int64
}

// Validate validates the encryption configuration.
func (c *EncryptionConfig) Validate() error {
	if !c.EnableEncryption {
		return nil
	}
	if len(c.Key) != 32 {
		return fmt.Errorf("%w: expected 32 bytes, got %d", ErrInvalidKey, len(c.Key))
	}
	return nil
}

// Provider provides encryption/decryption services.
type Provider struct {
	config EncryptionConfig
	block  cipher.Block
}

// NewProvider creates a new encryption provider.
func NewProvider(config EncryptionConfig) (*Provider, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !config.EnableEncryption {
		return &Provider{config: config}, nil
	}

	block, err := aes.NewCipher(config.Key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}

	return &Provider{
		config: config,
		block:  block,
	}, nil
}

// IsEnabled returns true if encryption is enabled.
func (p *Provider) IsEnabled() bool {
	return p.config.EnableEncryption
}

// Encrypt encrypts plaintext using AES-GCM.
func (p *Provider) Encrypt(plaintext []byte) ([]byte, error) {
	if !p.config.EnableEncryption {
		return plaintext, nil
	}

	if len(plaintext) == 0 {
		return plaintext, nil
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(p.block)
	if err != nil {
		return nil, fmt.Errorf("crypto: failed to create GCM: %w", err)
	}

	// Generate nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: failed to generate nonce: %w", err)
	}

	// Encrypt and append tag
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts ciphertext using AES-GCM.
func (p *Provider) Decrypt(ciphertext []byte) ([]byte, error) {
	if !p.config.EnableEncryption {
		return ciphertext, nil
	}

	if len(ciphertext) == 0 {
		return ciphertext, nil
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(p.block)
	if err != nil {
		return nil, fmt.Errorf("crypto: failed to create GCM: %w", err)
	}

	// Check minimum size
	if len(ciphertext) < gcm.NonceSize() {
		return nil, ErrDecryptionFailed
	}

	// Extract nonce
	nonce, ciphertext := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]

	// Decrypt
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecryptionFailed, err)
	}

	return plaintext, nil
}

// EncryptString encrypts a string and returns base64 encoded result.
func (p *Provider) EncryptString(plaintext string) (string, error) {
	ciphertext, err := p.Encrypt([]byte(plaintext))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// DecryptString decrypts a base64 encoded ciphertext string.
func (p *Provider) DecryptString(ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("crypto: failed to decode base64: %w", err)
	}

	plaintext, err := p.Decrypt(data)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// GenerateKey generates a random 32-byte encryption key.
func GenerateKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("crypto: failed to generate key: %w", err)
	}
	return key, nil
}

// SecureCompare performs constant-time comparison to prevent timing attacks.
func SecureCompare(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// EncryptWithKey encrypts data with a specific key (for key rotation).
func EncryptWithKey(plaintext, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: failed to generate nonce: %w", err)
	}

	// Prepend key version (0 = no rotation)
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// DecryptWithKey decrypts data with a specific key.
func DecryptWithKey(ciphertext, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: failed to create GCM: %w", err)
	}

	if len(ciphertext) < gcm.NonceSize() {
		return nil, ErrDecryptionFailed
	}

	nonce, ciphertext := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecryptionFailed, err)
	}

	return plaintext, nil
}
