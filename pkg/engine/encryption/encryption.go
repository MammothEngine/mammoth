// Package encryption provides AES-256-GCM encryption for data at rest.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// Encryptor provides AES-256-GCM encryption/decryption.
type Encryptor struct {
	aead cipher.AEAD
}

// NewEncryptor creates a new AES-256-GCM encryptor from a 32-byte key.
func NewEncryptor(key []byte) (*Encryptor, error) {
	if len(key) != 32 {
		return nil, errors.New("encryption: key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Encryptor{aead: aead}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
// Output format: [nonce(12)][ciphertext+tag].
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	// Seal appends ciphertext+tag to nonce
	return e.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts AES-256-GCM ciphertext.
// Expects format: [nonce(12)][ciphertext+tag].
func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := e.aead.NonceSize()
	if len(ciphertext) < nonceSize+e.aead.Overhead() {
		return nil, errors.New("encryption: ciphertext too short")
	}
	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return e.aead.Open(nil, nonce, ct, nil)
}

// Overhead returns the number of bytes added by encryption (nonce + tag).
func (e *Encryptor) Overhead() int {
	return e.aead.NonceSize() + e.aead.Overhead()
}
