package compression

import (
	"bytes"
	"math/rand"
	"testing"
)

// ---------------------------------------------------------------------------
// Round-trip tests
// ---------------------------------------------------------------------------

func TestSnappyRoundTripEmpty(t *testing.T) {
	roundTrip(t, []byte{})
}

func TestSnappyRoundTripSingleByte(t *testing.T) {
	roundTrip(t, []byte{0x42})
}

func TestSnappyRoundTripShortString(t *testing.T) {
	roundTrip(t, []byte("Hello, Snappy!"))
}

func TestSnappyRoundTripLongRepetitive(t *testing.T) {
	// Pattern that compresses very well.
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 7)
	}
	roundTrip(t, data)
}

func TestSnappyRoundTripAllSameBytes(t *testing.T) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = 0xAA
	}
	roundTrip(t, data)
}

func TestSnappyRoundTripRandom100(t *testing.T) {
	roundTrip(t, randBytes(t, 100))
}

func TestSnappyRoundTripRandom1K(t *testing.T) {
	roundTrip(t, randBytes(t, 1024))
}

func TestSnappyRoundTripRandom10K(t *testing.T) {
	roundTrip(t, randBytes(t, 10*1024))
}

func TestSnappyRoundTripRandom100K(t *testing.T) {
	roundTrip(t, randBytes(t, 100*1024))
}

func TestSnappyRoundTripRandom1MB(t *testing.T) {
	roundTrip(t, randBytes(t, 1024*1024))
}

func TestSnappyRoundTripTwoLiteralBlocks(t *testing.T) {
	// 200 bytes of random data forces a literal > 60 bytes.
	roundTrip(t, randBytes(t, 200))
}

func TestSnappyRoundTripLargeLiteral(t *testing.T) {
	// 300 bytes of random data forces a 2-byte literal length.
	roundTrip(t, randBytes(t, 300))
}

func TestSnappyRoundTripVeryLongLiteral(t *testing.T) {
	// 70000 bytes random data forces a multi-byte literal length header.
	roundTrip(t, randBytes(t, 70000))
}

// ---------------------------------------------------------------------------
// Known test vectors
// ---------------------------------------------------------------------------

func TestSnappyDecompressKnownVector(t *testing.T) {
	// Hand-crafted Snappy data: varint(6) + literal(6 bytes "foobar")
	// varint(6) = 0x06
	// literal tag: (len-1)<<2 = (6-1)<<2 = 20 = 0x14, literal type = 00 => tag = 0x14
	// Then the 6 literal bytes: 'f','o','o','b','a','r'
	compressed := []byte{
		0x06,       // varint: uncompressed length = 6
		0x14,       // literal tag: (6-1)<<2 | 0x00 = 20
		'f', 'o', 'o', 'b', 'a', 'r',
	}

	c := SnappyCompressor{}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	want := []byte("foobar")
	if !bytes.Equal(decompressed, want) {
		t.Errorf("got %q, want %q", decompressed, want)
	}
}

func TestSnappyDecompressCopy1Vector(t *testing.T) {
	// "abcabcabc": "abc" literal then copy-1 back 3 bytes, length 6
	// Uncompressed length = 9
	// Literal tag for 3 bytes: (3-1)<<2 = 0x08
	// Literal: 'a','b','c'
	// Copy-1: length=6 (len-4=2, bits[4:2]=2), offset=3
	//   tag = 0x01 | (2<<2) | ((3>>8)<<5) = 0x01 | 0x08 | 0x00 = 0x09
	//   next byte: 0x03
	compressed := []byte{
		0x09,                     // varint: uncompressed length = 9
		0x08,                     // literal tag: (3-1)<<2 = 8
		'a', 'b', 'c',            // literal data
		0x09,                     // copy-1 tag: type=01, length-4=2, offset high=0
		0x03,                     // offset low = 3
	}
	c := SnappyCompressor{}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	want := []byte("abcabcabc")
	if !bytes.Equal(decompressed, want) {
		t.Errorf("got %q, want %q", decompressed, want)
	}
}

func TestSnappyDecompressCopy2Vector(t *testing.T) {
	// "abcdefgh" literal then copy-2 back 8 bytes, length 4 => "abcdefghabcd"
	// Uncompressed length = 12
	// Literal tag for 8 bytes: (8-1)<<2 = 28 = 0x1C
	// Copy-2: length=4 => (len-1)<<2 = 12, offset=8
	//   tag = 0x02 | 12 = 0x0E
	//   offset LE: 0x08, 0x00
	compressed := []byte{
		0x0C,                               // varint: uncompressed length = 12
		0x1C,                               // literal tag: (8-1)<<2 = 28
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
		0x0E,                               // copy-2 tag: (4-1)<<2 | 0x02 = 14
		0x08, 0x00,                         // offset = 8 (little-endian)
	}
	c := SnappyCompressor{}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	want := []byte("abcdefghabcd")
	if !bytes.Equal(decompressed, want) {
		t.Errorf("got %q, want %q", decompressed, want)
	}
}

func TestSnappyDecompressOverlappingCopy(t *testing.T) {
	// "a" literal then copy-1 back 1 byte, length 7 => "aaaaaaaa"
	// Uncompressed length = 8
	// Literal tag for 1 byte: (1-1)<<2 = 0x00
	// Copy-1: length=7 (len-4=3, bits[4:2]=3), offset=1
	//   tag = 0x01 | (3<<2) | ((1>>8)<<5) = 0x01 | 0x0C | 0x00 = 0x0D
	//   next byte: 0x01
	compressed := []byte{
		0x08,       // varint: uncompressed length = 8
		0x00,       // literal tag: (1-1)<<2 = 0
		'a',        // literal data
		0x0D,       // copy-1 tag: type=01, length-4=3, offset high=0
		0x01,       // offset low = 1
	}
	c := SnappyCompressor{}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	want := []byte("aaaaaaaa")
	if !bytes.Equal(decompressed, want) {
		t.Errorf("got %q, want %q", decompressed, want)
	}
}

func TestSnappyRoundTripRepeatsPattern(t *testing.T) {
	// Pattern: "abcde" repeated many times — should compress well.
	var buf []byte
	for i := 0; i < 2000; i++ {
		buf = append(buf, "abcde"...)
	}
	roundTrip(t, buf)

	// Verify we actually compress.
	c := SnappyCompressor{}
	compressed, err := c.Compress(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) >= len(buf) {
		t.Errorf("repetitive data should compress: got %d bytes output for %d input", len(compressed), len(buf))
	}
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestSnappyDecompressTruncatedVarint(t *testing.T) {
	c := SnappyCompressor{}
	_, err := c.Decompress([]byte{0x80}) // continuation bit set but no more bytes
	if err == nil {
		t.Error("expected error for truncated varint")
	}
}

func TestSnappyDecompressTruncatedAfterVarint(t *testing.T) {
	c := SnappyCompressor{}
	_, err := c.Decompress([]byte{0x05, 0x14}) // length=5, literal tag says 3 bytes, but data truncated
	if err == nil {
		t.Error("expected error for truncated literal data")
	}
}

func TestSnappyDecompressOffsetOutOfBounds(t *testing.T) {
	// Literal "a" then copy-1 with offset 10 (out of bounds).
	compressed := []byte{
		0x05,       // uncompressed length = 5
		0x00,       // literal: 1 byte
		'a',
		0x01,       // copy-1: length=4, offset high=0
		0x0A,       // offset low = 10 (out of bounds, only 1 byte decoded)
	}
	c := SnappyCompressor{}
	_, err := c.Decompress(compressed)
	if err == nil {
		t.Error("expected error for offset out of bounds")
	}
}

func TestSnappyDecompressZeroOffset(t *testing.T) {
	compressed := []byte{
		0x05,       // uncompressed length = 5
		0x00,       // literal: 1 byte
		'a',
		0x01,       // copy-1 tag: length=4, offset high=0
		0x00,       // offset = 0 (invalid)
	}
	c := SnappyCompressor{}
	_, err := c.Decompress(compressed)
	if err == nil {
		t.Error("expected error for zero offset")
	}
}

func TestSnappyDecompressEmptyInput(t *testing.T) {
	c := SnappyCompressor{}
	_, err := c.Decompress([]byte{})
	if err == nil {
		t.Error("expected error for empty input")
	}
}

func TestSnappyDecompressOutputTooLong(t *testing.T) {
	// Claim uncompressed length is 2, but we'll produce more.
	compressed := []byte{
		0x02,       // uncompressed length = 2
		0x00,       // literal: 1 byte
		'a',
		0x01,       // copy-1: length=4, offset = 1
		0x01,
	}
	c := SnappyCompressor{}
	_, err := c.Decompress(compressed)
	if err == nil {
		t.Error("expected error for output exceeding declared length")
	}
}

func TestSnappyDecompressLengthMismatch(t *testing.T) {
	// Claim uncompressed length 10 but only provide "abc".
	compressed := []byte{
		0x0A,       // uncompressed length = 10
		0x08,       // literal: 3 bytes
		'a', 'b', 'c',
	}
	c := SnappyCompressor{}
	_, err := c.Decompress(compressed)
	if err == nil {
		t.Error("expected error for output length mismatch")
	}
}

func TestSnappyDecompressCopy2Truncated(t *testing.T) {
	compressed := []byte{
		0x0A,       // uncompressed length = 10
		0x08,       // literal: 3 bytes
		'a', 'b', 'c',
		0x06,       // copy-2 tag
		0x01,       // missing second offset byte
	}
	c := SnappyCompressor{}
	_, err := c.Decompress(compressed)
	if err == nil {
		t.Error("expected error for truncated copy-2 offset")
	}
}

func TestSnappyDecompressCopy4Truncated(t *testing.T) {
	compressed := []byte{
		0x0A,       // uncompressed length = 10
		0x08,       // literal: 3 bytes
		'a', 'b', 'c',
		0x03,       // copy-4 tag
		0x01, 0x00, // only 2 of 4 offset bytes
	}
	c := SnappyCompressor{}
	_, err := c.Decompress(compressed)
	if err == nil {
		t.Error("expected error for truncated copy-4 offset")
	}
}

// ---------------------------------------------------------------------------
// Compression ratio check
// ---------------------------------------------------------------------------

func TestSnappyCompressionRatio(t *testing.T) {
	// All same bytes should compress significantly.
	data := make([]byte, 10000)
	for i := range data {
		data[i] = 0x55
	}
	c := SnappyCompressor{}
	compressed, err := c.Compress(data)
	if err != nil {
		t.Fatal(err)
	}
	ratio := float64(len(compressed)) / float64(len(data))
	if ratio > 0.1 {
		t.Errorf("all-same-bytes compression ratio = %.2f, expected < 0.1", ratio)
	}
}

func TestSnappyIncompressibleData(t *testing.T) {
	// Random data should not expand too much.
	data := randBytes(t, 10000)
	c := SnappyCompressor{}
	compressed, err := c.Compress(data)
	if err != nil {
		t.Fatal(err)
	}
	// Snappy overhead for incompressible data should be modest.
	if len(compressed) > len(data)+len(data)/10+100 {
		t.Errorf("compressed %d bytes to %d bytes (too much expansion)", len(data), len(compressed))
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func benchmarkSnappyCompress(b *testing.B, size int) {
	data := make([]byte, size)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	c := SnappyCompressor{}
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compress(data)
	}
}

func benchmarkSnappyDecompress(b *testing.B, size int) {
	data := make([]byte, size)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	c := SnappyCompressor{}
	compressed, err := c.Compress(data)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Decompress(compressed)
	}
}

func BenchmarkSnappyCompress100(b *testing.B)   { benchmarkSnappyCompress(b, 100) }
func BenchmarkSnappyCompress1K(b *testing.B)    { benchmarkSnappyCompress(b, 1024) }
func BenchmarkSnappyCompress10K(b *testing.B)   { benchmarkSnappyCompress(b, 10*1024) }
func BenchmarkSnappyCompress100K(b *testing.B)  { benchmarkSnappyCompress(b, 100*1024) }
func BenchmarkSnappyCompress1MB(b *testing.B)   { benchmarkSnappyCompress(b, 1024*1024) }

func BenchmarkSnappyDecompress100(b *testing.B)  { benchmarkSnappyDecompress(b, 100) }
func BenchmarkSnappyDecompress1K(b *testing.B)   { benchmarkSnappyDecompress(b, 1024) }
func BenchmarkSnappyDecompress10K(b *testing.B)  { benchmarkSnappyDecompress(b, 10*1024) }
func BenchmarkSnappyDecompress100K(b *testing.B) { benchmarkSnappyDecompress(b, 100*1024) }
func BenchmarkSnappyDecompress1MB(b *testing.B)  { benchmarkSnappyDecompress(b, 1024*1024) }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func roundTrip(t *testing.T, input []byte) {
	t.Helper()
	c := SnappyCompressor{}
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
		if len(decompressed) < 256 && len(input) < 256 {
			t.Errorf("  input:  %x", input)
			t.Errorf("  output: %x", decompressed)
		}
	}
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(int64(n)))
	data := make([]byte, n)
	_, err := rng.Read(data)
	if err != nil {
		t.Fatalf("rand.Read error: %v", err)
	}
	return data
}
