package compression

import (
	"bytes"
	"fmt"
	"testing"
)

func TestZstdRoundTripEmpty(t *testing.T) {
	zstdRoundTrip(t, []byte{})
}

func TestZstdRoundTripSingleByte(t *testing.T) {
	zstdRoundTrip(t, []byte{0x42})
}

func TestZstdRoundTripShortString(t *testing.T) {
	zstdRoundTrip(t, []byte("Hello, Zstd!"))
}

func TestZstdRoundTripLongRepetitive(t *testing.T) {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 7)
	}
	zstdRoundTrip(t, data)
}

func TestZstdRoundTripAllSameBytes(t *testing.T) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = 0xAA
	}
	zstdRoundTrip(t, data)
}

func TestZstdRoundTripRandom(t *testing.T) {
	for _, size := range []int{100, 1024, 10 * 1024, 100 * 1024} {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			zstdRoundTrip(t, randBytes(t, size))
		})
	}
}

func TestZstdType(t *testing.T) {
	c := ZstdCompressor{}
	if c.Type() != CompressionZstd {
		t.Errorf("expected CompressionZstd, got %d", c.Type())
	}
}

func TestZstdGetCompressor(t *testing.T) {
	c := GetCompressor(CompressionZstd)
	if _, ok := c.(ZstdCompressor); !ok {
		t.Errorf("expected ZstdCompressor, got %T", c)
	}
}

func TestZstdDecompressCorrupt(t *testing.T) {
	c := ZstdCompressor{}
	_, err := c.Decompress([]byte{0x01}) // too short, no magic
	if err == nil {
		t.Error("expected error for corrupt input")
	}
}

func TestZstdDecompressBadMagic(t *testing.T) {
	c := ZstdCompressor{}
	_, err := c.Decompress([]byte{0x00, 0x00, 0x00, 0x00, 0x00})
	if err == nil {
		t.Error("expected error for bad magic number")
	}
}

func zstdRoundTrip(t *testing.T, input []byte) {
	t.Helper()
	c := ZstdCompressor{}
	compressed, err := c.Compress(input)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	if !bytes.Equal(decompressed, input) {
		t.Errorf("round-trip mismatch: input len=%d, output len=%d", len(input), len(decompressed))
	}
}
