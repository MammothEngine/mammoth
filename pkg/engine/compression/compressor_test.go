package compression

import (
	"testing"
)

func TestGetCompressorReturnsCorrectType(t *testing.T) {
	// Types that are currently implemented.
	implemented := []CompressionType{CompressionNone, CompressionSnappy}
	for _, want := range implemented {
		c := GetCompressor(want)
		if c == nil {
			t.Errorf("GetCompressor(%v) returned nil", want)
			continue
		}
		got := c.Type()
		if got != want {
			t.Errorf("GetCompressor(%v).Type() = %v, want %v", want, got, want)
		}
	}

	// Unimplemented types should fall back to NoneCompressor.
	for _, ct := range []CompressionType{CompressionLZ4, CompressionZstd} {
		c := GetCompressor(ct)
		if c.Type() != CompressionNone {
			t.Errorf("GetCompressor(%v).Type() = %v, expected fallback to CompressionNone", ct, c.Type())
		}
	}
}

func TestGetCompressorUnknownFallsBackToNone(t *testing.T) {
	c := GetCompressor(CompressionType(99))
	if c.Type() != CompressionNone {
		t.Errorf("unknown type should fall back to NoneCompressor, got %v", c.Type())
	}
}

func TestNoneCompressorRoundTrip(t *testing.T) {
	c := GetCompressor(CompressionNone)
	tests := [][]byte{
		nil,
		{},
		{0x00},
		{0x01, 0x02, 0x03},
		make([]byte, 4096),
	}
	for i, input := range tests {
		compressed, err := c.Compress(input)
		if err != nil {
			t.Fatalf("test %d: Compress error: %v", i, err)
		}
		decompressed, err := c.Decompress(compressed)
		if err != nil {
			t.Fatalf("test %d: Decompress error: %v", i, err)
		}
		if input == nil {
			if decompressed != nil {
				t.Errorf("test %d: expected nil, got %v", i, decompressed)
			}
			continue
		}
		if len(decompressed) != len(input) {
			t.Errorf("test %d: length mismatch: got %d, want %d", i, len(decompressed), len(input))
			continue
		}
		for j := range input {
			if decompressed[j] != input[j] {
				t.Errorf("test %d: byte %d mismatch: got %02x, want %02x", i, j, decompressed[j], input[j])
				break
			}
		}
	}
}

func TestNoneCompressorAllocatesCopy(t *testing.T) {
	c := GetCompressor(CompressionNone)
	input := []byte{0xAA, 0xBB, 0xCC}
	compressed, _ := c.Compress(input)
	compressed[0] = 0xFF // modify the copy
	decompressed, _ := c.Decompress(compressed)
	if decompressed[0] == 0xAA {
		t.Error("NoneCompressor should return independent copies")
	}
}
