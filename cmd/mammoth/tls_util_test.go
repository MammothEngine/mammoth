package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "test.crt")
	keyFile := filepath.Join(dir, "test.key")

	err := GenerateSelfSignedCert(certFile, keyFile)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert failed: %v", err)
	}

	// Verify files exist
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		t.Error("cert file was not created")
	}
	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		t.Error("key file was not created")
	}

	// Load the generated cert
	config, err := loadTLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}
	if config == nil {
		t.Error("TLS config is nil")
	}
	if len(config.Certificates) != 1 {
		t.Error("expected 1 certificate")
	}
}

func TestLoadTLSConfigInvalidFiles(t *testing.T) {
	_, err := loadTLSConfig("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Error("expected error for invalid files")
	}
}

func TestLoadTLSConfigInvalidPath(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "test.crt")
	keyFile := filepath.Join(dir, "test.key")

	// Create invalid cert file
	os.WriteFile(certFile, []byte("invalid cert"), 0644)
	os.WriteFile(keyFile, []byte("invalid key"), 0600)

	_, err := loadTLSConfig(certFile, keyFile)
	if err == nil {
		t.Error("expected error for invalid cert content")
	}
}
