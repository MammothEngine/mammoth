package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
)

const defaultIterations = 10000

// SCRAMStepResult holds the result of a SCRAM authentication step.
type SCRAMStepResult struct {
	Data string
	Done bool
}

// SCRAMSession tracks an in-progress SCRAM authentication exchange.
type SCRAMSession struct {
	username        string
	clientFirstBare string
	serverFirst     string
	serverNonce     string
	storedKey       []byte
	serverKey       []byte
}

// Username returns the authenticating username.
func (s *SCRAMSession) Username() string { return s.username }

// StartSCRAM begins a SCRAM-SHA-256 authentication.
func StartSCRAM(us *UserStore, username, clientFirst string) (*SCRAMSession, *SCRAMStepResult, error) {
	// Strip GS2 header ("n,," etc.)
	clientFirstBare := clientFirst
	if idx := strings.Index(clientFirst, ",,"); idx >= 0 {
		clientFirstBare = clientFirst[idx+2:]
	}

	var clientNonce string
	for _, part := range strings.Split(clientFirstBare, ",") {
		if strings.HasPrefix(part, "r=") {
			clientNonce = part[2:]
		}
	}
	if clientNonce == "" {
		return nil, nil, fmt.Errorf("missing client nonce")
	}

	user, err := us.GetUser(username)
	if err != nil {
		return nil, nil, fmt.Errorf("user not found")
	}

	suffix := make([]byte, 18)
	rand.Read(suffix)
	serverNonce := clientNonce + base64.StdEncoding.EncodeToString(suffix)

	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d",
		serverNonce,
		base64.StdEncoding.EncodeToString(user.Salt),
		user.Iterations,
	)

	session := &SCRAMSession{
		username:        username,
		clientFirstBare: clientFirstBare,
		serverFirst:     serverFirst,
		serverNonce:     serverNonce,
		storedKey:       user.StoredKey,
		serverKey:       user.ServerKey,
	}

	return session, &SCRAMStepResult{Data: serverFirst}, nil
}

// Continue processes the client-final-message and verifies the proof.
func (s *SCRAMSession) Continue(clientFinal string) (*SCRAMStepResult, error) {
	var proof string
	var bareParts []string

	for _, part := range strings.Split(clientFinal, ",") {
		if strings.HasPrefix(part, "p=") {
			proof = part[2:]
		} else {
			bareParts = append(bareParts, part)
		}
	}
	if proof == "" {
		return nil, fmt.Errorf("missing client proof")
	}

	clientFinalBare := strings.Join(bareParts, ",")
	authMessage := s.clientFirstBare + "," + s.serverFirst + "," + clientFinalBare

	clientSig := hmacSHA256(s.storedKey, []byte(authMessage))

	proofBytes, err := base64.StdEncoding.DecodeString(proof)
	if err != nil {
		return nil, fmt.Errorf("invalid proof encoding: %w", err)
	}
	if len(proofBytes) != len(clientSig) {
		return nil, fmt.Errorf("authentication failed")
	}

	clientKey := make([]byte, len(proofBytes))
	for i := range proofBytes {
		clientKey[i] = proofBytes[i] ^ clientSig[i]
	}

	computed := sha256.Sum256(clientKey)
	if !hmac.Equal(computed[:], s.storedKey) {
		return nil, fmt.Errorf("authentication failed")
	}

	serverSig := hmacSHA256(s.serverKey, []byte(authMessage))
	return &SCRAMStepResult{
		Data: fmt.Sprintf("v=%s", base64.StdEncoding.EncodeToString(serverSig)),
		Done: true,
	}, nil
}

// DeriveKeys derives StoredKey and ServerKey from a password.
func DeriveKeys(password string, salt []byte, iterations int) (storedKey, serverKey []byte) {
	saltedPassword := pbkdf2SHA256([]byte(password), salt, iterations)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	sk := sha256.Sum256(clientKey)
	srvKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	return sk[:], srvKey
}

// GenerateSalt creates a random 16-byte salt.
func GenerateSalt() []byte {
	salt := make([]byte, 16)
	rand.Read(salt)
	return salt
}

func pbkdf2SHA256(password, salt []byte, iterations int) []byte {
	h := hmac.New(sha256.New, password)
	dkLen := h.Size()
	result := make([]byte, 0, dkLen)
	blockIndex := uint32(1)

	for len(result) < dkLen {
		h.Reset()
		h.Write(salt)
		var bi [4]byte
		bi[0] = byte(blockIndex >> 24)
		bi[1] = byte(blockIndex >> 16)
		bi[2] = byte(blockIndex >> 8)
		bi[3] = byte(blockIndex)
		h.Write(bi[:])
		u := h.Sum(nil)
		block := make([]byte, len(u))
		copy(block, u)

		for i := 1; i < iterations; i++ {
			h.Reset()
			h.Write(u)
			u = h.Sum(nil)
			for j := range block {
				block[j] ^= u[j]
			}
		}
		result = append(result, block...)
		blockIndex++
	}
	return result[:dkLen]
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
