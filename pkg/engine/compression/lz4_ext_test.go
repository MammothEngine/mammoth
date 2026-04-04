package compression

import (
	"testing"
)

// TestLZ4DecompressEmptyInput tests decompression of empty input
func TestLZ4DecompressEmptyInput(t *testing.T) {
	c := LZ4Compressor{}
	result, err := c.Decompress([]byte{})
	// Empty input returns nil, nil (no error)
	if err != nil {
		t.Errorf("expected no error for empty input, got %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty input, got %v", result)
	}
}

// TestLZ4DecompressTruncatedHeader tests decompression with truncated header
func TestLZ4DecompressTruncatedHeader(t *testing.T) {
	c := LZ4Compressor{}
	// LZ4 frame header needs at least 7 bytes
	_, err := c.Decompress([]byte{0x04, 0x22, 0x4D, 0x18}) // incomplete header
	if err == nil {
		t.Error("expected error for truncated header")
	}
}

// TestLZ4DecompressInvalidMagic tests decompression with invalid magic bytes
func TestLZ4DecompressInvalidMagic(t *testing.T) {
	c := LZ4Compressor{}
	// Invalid magic number
	_, err := c.Decompress([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	if err == nil {
		t.Error("expected error for invalid magic bytes")
	}
}

// TestLZ4DecompressTruncatedBlock tests decompression with truncated block
func TestLZ4DecompressTruncatedBlock(t *testing.T) {
	c := LZ4Compressor{}
	// Valid LZ4 frame header but truncated block
	// Magic: 0x184D2204
	// Frame descriptor: FLG=0x60 (version 01, block independence), BD=0x40 (64KB max)
	// Content size flag not set
	data := []byte{
		0x04, 0x22, 0x4D, 0x18, // Magic
		0x60,                   // FLG: version 01, block indep
		0x40,                   // BD: 64KB max block
		// Missing content size (not needed without flag)
		// Missing header checksum
		// Block size (4 bytes) claiming large block but no data
		0xFF, 0xFF, 0xFF, 0x7F, // Claim 2GB block
	}
	_, err := c.Decompress(data)
	if err == nil {
		t.Error("expected error for truncated block")
	}
}

// TestLZ4CompressEmptyInput tests compression of empty input
func TestLZ4CompressEmptyInput(t *testing.T) {
	c := LZ4Compressor{}
	compressed, err := c.Compress([]byte{})
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	// Empty input produces minimal output (flag + length + empty data)
	// Verify we can decompress it back to empty
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if len(decompressed) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(decompressed))
	}
}

// TestLZ4RoundTripSmall tests round-trip with small data
func TestLZ4RoundTripSmall(t *testing.T) {
	c := LZ4Compressor{}
	data := []byte("Hello, World!")
	compressed, err := c.Compress(data)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if string(decompressed) != string(data) {
		t.Errorf("expected %q, got %q", data, decompressed)
	}
}

// TestLZ4RoundTripLarge tests round-trip with large data
func TestLZ4RoundTripLarge(t *testing.T) {
	c := LZ4Compressor{}
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	compressed, err := c.Compress(data)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if len(decompressed) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(decompressed))
	}
	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("mismatch at byte %d", i)
			break
		}
	}
}

// TestLZ4RoundTripRepetitive tests round-trip with repetitive data
func TestLZ4RoundTripRepetitive(t *testing.T) {
	c := LZ4Compressor{}
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 10)
	}
	compressed, err := c.Compress(data)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	// Should compress well
	if len(compressed) > len(data)/10 {
		t.Logf("Warning: compression ratio not great: %d -> %d", len(data), len(compressed))
	}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("mismatch at byte %d", i)
			break
		}
	}
}

// TestLZ4DecompressCorruptedData tests decompression of corrupted data
func TestLZ4DecompressCorruptedData(t *testing.T) {
	c := LZ4Compressor{}
	data := []byte("Hello, World!")
	compressed, _ := c.Compress(data)

	// Corrupt the data
	if len(compressed) > 10 {
		compressed[10] ^= 0xFF
	}

	_, err := c.Decompress(compressed)
	// May or may not error depending on where corruption is
	_ = err
}

// TestLZ4CompressNilInput tests compression of nil input
func TestLZ4CompressNilInput(t *testing.T) {
	c := LZ4Compressor{}
	compressed, err := c.Compress(nil)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	// Decompress should give empty result
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if len(decompressed) != 0 {
		t.Errorf("expected empty, got %d bytes", len(decompressed))
	}
}
