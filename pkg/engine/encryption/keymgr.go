package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

// KeyManager manages master and data encryption keys.
type KeyManager struct {
	mu       sync.Mutex
	masterKey []byte
	deks     map[string]*dekEntry // per-database DEKs
	keyFile  string
}

type dekEntry struct {
	EncryptedDEK []byte `json:"dek"`
	Nonce        []byte `json:"nonce"`
}

// NewKeyManager creates a key manager with the given master key.
// The master key must be 32 bytes.
func NewKeyManager(masterKey []byte, keyFile string) (*KeyManager, error) {
	if len(masterKey) != 32 {
		return nil, errors.New("encryption: master key must be 32 bytes")
	}
	km := &KeyManager{
		masterKey: make([]byte, 32),
		deks:      make(map[string]*dekEntry),
		keyFile:   keyFile,
	}
	copy(km.masterKey, masterKey)

	if keyFile != "" {
		km.loadKeys()
	}
	return km, nil
}

// MasterKeyFromEnv reads the master key from the MAMMOTH_ENCRYPTION_KEY env variable.
// The key must be a hex-encoded 32-byte (64 character) string.
func MasterKeyFromEnv() ([]byte, error) {
	hexKey := os.Getenv("MAMMOTH_ENCRYPTION_KEY")
	if hexKey == "" {
		return nil, errors.New("encryption: MAMMOTH_ENCRYPTION_KEY not set")
	}
	if len(hexKey) != 64 {
		return nil, errors.New("encryption: MAMMOTH_ENCRYPTION_KEY must be 64 hex characters (32 bytes)")
	}
	key := make([]byte, 32)
	for i := 0; i < 32; i++ {
		b := hexByte(hexKey[i*2], hexKey[i*2+1])
		if b == -1 {
			return nil, errors.New("encryption: invalid hex in MAMMOTH_ENCRYPTION_KEY")
		}
		key[i] = byte(b)
	}
	return key, nil
}

// MasterKeyFromFile reads a 32-byte key from a file.
func MasterKeyFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) != 32 {
		return nil, errors.New("encryption: key file must contain exactly 32 bytes")
	}
	return data, nil
}

// GetEncryptor returns an Encryptor for the given database namespace.
// Creates a new DEK if one doesn't exist.
func (km *KeyManager) GetEncryptor(db string) (*Encryptor, error) {
	km.mu.Lock()
	defer km.mu.Unlock()

	dek, err := km.getDEK(db)
	if err != nil {
		return nil, err
	}
	return NewEncryptor(dek)
}

// RotateDEK generates a new DEK for the given database and re-encrypts it.
func (km *KeyManager) RotateDEK(db string) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	// Generate new DEK
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return err
	}

	// Encrypt with master key
	block, err := aes.NewCipher(km.masterKey)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	encryptedDEK := aead.Seal(nil, nonce, dek, nil)

	km.deks[db] = &dekEntry{
		EncryptedDEK: encryptedDEK,
		Nonce:        nonce,
	}

	return km.saveKeys()
}

func (km *KeyManager) getDEK(db string) ([]byte, error) {
	entry, ok := km.deks[db]
	if !ok {
		// Generate new DEK
		if err := km.RotateDEKLocked(db); err != nil {
			return nil, err
		}
		entry = km.deks[db]
	}

	// Decrypt DEK with master key
	block, err := aes.NewCipher(km.masterKey)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	dek, err := aead.Open(nil, entry.Nonce, entry.EncryptedDEK, nil)
	if err != nil {
		return nil, errors.New("encryption: failed to decrypt DEK")
	}
	return dek, nil
}

// RotateDEKLocked generates a new DEK (caller must hold lock).
func (km *KeyManager) RotateDEKLocked(db string) error {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return err
	}

	block, err := aes.NewCipher(km.masterKey)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	encryptedDEK := aead.Seal(nil, nonce, dek, nil)

	km.deks[db] = &dekEntry{
		EncryptedDEK: encryptedDEK,
		Nonce:        nonce,
	}
	return km.saveKeys()
}

func (km *KeyManager) loadKeys() {
	if km.keyFile == "" {
		return
	}
	data, err := os.ReadFile(km.keyFile)
	if err != nil {
		return
	}
	var entries map[string]*dekEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return
	}
	km.deks = entries
}

func (km *KeyManager) saveKeys() error {
	if km.keyFile == "" {
		return nil
	}
	data, err := json.Marshal(km.deks)
	if err != nil {
		return err
	}
	return os.WriteFile(km.keyFile, data, 0600)
}

func hexByte(h1, h2 byte) int {
	return hexVal(h1)<<4 | hexVal(h2)
}

func hexVal(h byte) int {
	switch {
	case h >= '0' && h <= '9':
		return int(h - '0')
	case h >= 'a' && h <= 'f':
		return int(h - 'a' + 10)
	case h >= 'A' && h <= 'F':
		return int(h - 'A' + 10)
	default:
		return -1
	}
}
