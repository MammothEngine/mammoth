package compression

import (
	"bytes"
	"fmt"
	"testing"
)

func TestLZ4RoundTripEmpty(t *testing.T) {
	lz4RoundTrip(t, []byte{})
}

func TestLZ4RoundTripSingleByte(t *testing.T) {
	lz4RoundTrip(t, []byte{0x42})
}

func TestLZ4RoundTripShortString(t *testing.T) {
	lz4RoundTrip(t, []byte("Hello, LZ4!"))
}

func TestLZ4RoundTripLongRepetitive(t *testing.T) {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 7)
	}
	lz4RoundTrip(t, data)
}

func TestLZ4RoundTripAllSameBytes(t *testing.T) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = 0xAA
	}
	lz4RoundTrip(t, data)
}

func TestLZ4RoundTripRandom(t *testing.T) {
	for _, size := range []int{100, 1024, 10 * 1024, 100 * 1024} {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			lz4RoundTrip(t, randBytes(t, size))
		})
	}
}

func TestLZ4CompressionRatio(t *testing.T) {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = 0x55
	}
	c := LZ4Compressor{}
	compressed, err := c.Compress(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) >= len(data) {
		// For highly repetitive data, should compress
		ratio := float64(len(compressed)) / float64(len(data))
		t.Errorf("all-same-bytes compression ratio = %.2f, expected < 1.0", ratio)
	}
}

func TestLZ4Type(t *testing.T) {
	c := LZ4Compressor{}
	if c.Type() != CompressionLZ4 {
		t.Errorf("expected CompressionLZ4, got %d", c.Type())
	}
}

func TestLZ4GetCompressor(t *testing.T) {
	c := GetCompressor(CompressionLZ4)
	if _, ok := c.(LZ4Compressor); !ok {
		t.Errorf("expected LZ4Compressor, got %T", c)
	}
}

func TestLZ4DecompressCorrupt(t *testing.T) {
	c := LZ4Compressor{}
	_, err := c.Decompress([]byte{0x01}) // too short
	if err == nil {
		t.Error("expected error for corrupt input")
	}
}

func lz4RoundTrip(t *testing.T, input []byte) {
	t.Helper()
	c := LZ4Compressor{}
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
