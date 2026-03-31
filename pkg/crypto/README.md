# Crypto Package

The `crypto` package provides encryption and security utilities for Mammoth Engine.

## Features

- **AES-256-GCM Encryption**: Industry-standard authenticated encryption
- **Automatic Key Generation**: Secure random key generation
- **Key Validation**: Configuration validation with helpful errors
- **String/Bytes Support**: Both raw bytes and base64-encoded strings
- **Constant-Time Comparison**: Timing-attack resistant comparison
- **Key Rotation Support**: Framework for rotating encryption keys

## Usage

### Basic Encryption

```go
import "github.com/mammothengine/mammoth/pkg/crypto"

// Generate a new key
key, err := crypto.GenerateKey()
if err != nil {
    log.Fatal(err)
}

// Create provider
provider, err := crypto.NewProvider(crypto.EncryptionConfig{
    Key:              key,
    EnableEncryption: true,
})
if err != nil {
    log.Fatal(err)
}

// Encrypt data
plaintext := []byte("sensitive data")
ciphertext, err := provider.Encrypt(plaintext)
if err != nil {
    log.Fatal(err)
}

// Decrypt data
decrypted, err := provider.Decrypt(ciphertext)
if err != nil {
    log.Fatal(err)
}
```

### String Encryption

```go
// Encrypt string
encrypted, err := provider.EncryptString("my secret")
if err != nil {
    log.Fatal(err)
}
// Result: base64-encoded ciphertext

// Decrypt string
decrypted, err := provider.DecryptString(encrypted)
if err != nil {
    log.Fatal(err)
}
// Result: "my secret"
```

### Configuration

```go
config := crypto.EncryptionConfig{
    Key:                 key,        // 32-byte AES-256 key
    EnableEncryption:    true,       // Enable/disable encryption
    KeyRotationInterval: 86400,      // Rotate keys daily (optional)
}

if err := config.Validate(); err != nil {
    log.Fatal("Invalid configuration:", err)
}
```

## Security Considerations

1. **Key Storage**: Store encryption keys securely (e.g., environment variables, key management service)
2. **Key Rotation**: Implement regular key rotation for enhanced security
3. **Backup**: Keep secure backups of encryption keys - losing keys means losing data
4. **Compliance**: AES-256-GCM is compliant with most security standards (PCI DSS, HIPAA, etc.)

## Performance

Encryption adds minimal overhead:
- **Encryption**: ~500-1000 ns/op
- **Decryption**: ~500-1000 ns/op
- **Overhead**: ~16 bytes per encrypted value (nonce + tag)

## Integration with Engine

To enable encryption for the storage engine:

```go
// In engine configuration
eng, err := engine.Open(engine.Options{
    Dir: dataDir,
    Encryption: &crypto.EncryptionConfig{
        Key:              key,
        EnableEncryption: true,
    },
})
```

## Testing

Run tests:
```bash
go test ./pkg/crypto/...
```

Run benchmarks:
```bash
go test -bench=. ./pkg/crypto/...
```
