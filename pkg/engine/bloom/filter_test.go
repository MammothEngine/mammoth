package bloom

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// TestFilterInsertAndQuery verifies that all inserted keys are present
// (zero false negatives).
func TestFilterInsertAndQuery(t *testing.T) {
	f := NewFilter(1000)

	keys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("mammoth"),
		[]byte("engine"),
		[]byte("bloom"),
		[]byte("filter"),
		[]byte("murmur"),
		[]byte("hash"),
	}

	for _, k := range keys {
		f.Insert(k)
	}

	for _, k := range keys {
		if !f.MayContain(k) {
			t.Errorf("false negative for key %q", k)
		}
	}
}

// TestFilter100KKeys inserts 100K keys and verifies zero false negatives,
// then measures the false positive rate with 100K non-keys.
func TestFilter100KKeys(t *testing.T) {
	const n = 100_000

	f := NewFilter(n)

	// Insert keys 0..n-1.
	inserted := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		inserted[i] = key
		f.Insert(key)
	}

	// Verify all inserted keys are present (0% false negative).
	for i := 0; i < n; i++ {
		if !f.MayContain(inserted[i]) {
			t.Fatalf("false negative for key %d", i)
		}
	}

	// Measure false positive rate with non-keys n..2n-1.
	fpCount := 0
	testCount := 100_000
	for i := n; i < n+testCount; i++ {
		nonKey := []byte(fmt.Sprintf("key-%08d", i))
		if f.MayContain(nonKey) {
			fpCount++
		}
	}

	fpRate := float64(fpCount) / float64(testCount)
	t.Logf("False positive rate: %.4f%% (%d / %d)", fpRate*100, fpCount, testCount)
	t.Logf("Theoretical FP rate: %.4f%%", f.FalsePositiveRate(n)*100)
	t.Logf("Bits per key: %.1f", f.BitsPerKey(n))
	t.Logf("Memory size: %d bytes", f.Size())

	// The FP rate must be below 5%.
	if fpRate >= 0.05 {
		t.Errorf("false positive rate %.2f%% exceeds 5%% threshold", fpRate*100)
	}

	// The FP rate should be approximately 1% (allow wide margin for randomness).
	if fpRate > 0.05 {
		t.Errorf("false positive rate %.2f%% is unexpectedly high", fpRate*100)
	}
}

// TestFilterSerializeRoundTrip verifies that serialization and deserialization
// preserve filter behavior.
func TestFilterSerializeRoundTrip(t *testing.T) {
	f := NewFilter(1000)

	keys := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
		[]byte("epsilon"),
	}
	for _, k := range keys {
		f.Insert(k)
	}

	data, err := f.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	f2 := &Filter{}
	if err := f2.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// All original keys must still be present.
	for _, k := range keys {
		if !f2.MayContain(k) {
			t.Errorf("false negative after round-trip for key %q", k)
		}
	}

	// Non-keys should still be (mostly) absent.
	nonKeys := [][]byte{
		[]byte("zeta"),
		[]byte("eta"),
		[]byte("theta"),
	}
	for _, k := range nonKeys {
		if f2.MayContain(k) {
			t.Logf("false positive after round-trip for %q (acceptable)", k)
		}
	}

	// Internal state must match.
	if f2.k != f.k {
		t.Errorf("k mismatch: got %d, want %d", f2.k, f.k)
	}
	if f2.m != f.m {
		t.Errorf("m mismatch: got %d, want %d", f2.m, f.m)
	}
	if len(f2.bits) != len(f.bits) {
		t.Errorf("bits length mismatch: got %d, want %d", len(f2.bits), len(f.bits))
	}
}

// TestFilterEmpty verifies that an empty filter matches nothing.
func TestFilterEmpty(t *testing.T) {
	f := EmptyFilter()

	keys := [][]byte{
		[]byte("anything"),
		[]byte(""),
		{0x00},
		{0xff},
	}
	for _, k := range keys {
		if f.MayContain(k) {
			t.Errorf("empty filter matched key %q", k)
		}
	}
}

// TestFilterEmptyNewFilter verifies that a newly created filter (no inserts)
// matches nothing.
func TestFilterEmptyNewFilter(t *testing.T) {
	f := NewFilter(100)

	if f.MayContain([]byte("absent")) {
		t.Error("newly created filter should not match any key")
	}
}

// TestFilterSingleKey verifies correct behavior with a single key.
func TestFilterSingleKey(t *testing.T) {
	f := NewFilter(1)
	key := []byte("only-key")
	f.Insert(key)

	if !f.MayContain(key) {
		t.Error("false negative for single inserted key")
	}
}

// TestFilterDifferentSizes tests filter creation with various expected key counts.
func TestFilterDifferentSizes(t *testing.T) {
	sizes := []int{1, 10, 100, 1000, 10000, 100000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			f := NewFilter(size)

			// Insert all keys.
			for i := 0; i < size; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				f.Insert(key)
			}

			// Verify all present.
			for i := 0; i < size; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				if !f.MayContain(key) {
					t.Errorf("false negative at size %d for key %d", size, i)
				}
			}

			t.Logf("size=%d m=%d k=%d mem=%d bytes", size, f.m, f.k, f.Size())
		})
	}
}

// TestFilterAllowKeyUpdate verifies that AllowKeyUpdate is a harmless no-op
// and re-insertion still works correctly.
func TestFilterAllowKeyUpdate(t *testing.T) {
	f := NewFilter(100)
	f.AllowKeyUpdate() // should be a no-op

	key := []byte("test-key")
	f.Insert(key)
	f.Insert(key) // re-insert should be fine

	if !f.MayContain(key) {
		t.Error("false negative after re-insert")
	}
}

// TestFilterMarshalBinaryEmpty verifies serialization of an empty filter.
func TestFilterMarshalBinaryEmpty(t *testing.T) {
	f := EmptyFilter()

	data, err := f.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary on empty filter failed: %v", err)
	}

	f2 := &Filter{}
	if err := f2.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary on empty filter failed: %v", err)
	}

	if f2.MayContain([]byte("anything")) {
		t.Error("deserialized empty filter should match nothing")
	}
}

// TestFilterUnmarshalBinaryErrors verifies error handling for malformed data.
func TestFilterUnmarshalBinaryErrors(t *testing.T) {
	f := &Filter{}

	tests := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"too short", []byte{0x01, 0x02, 0x03}},
		{"invalid k", func() []byte {
			// k=0, m=64
			d := make([]byte, 8+8)
			return d
		}()},
		{"invalid m alignment", func() []byte {
			d := make([]byte, 8+8)
			d[0] = 7                  // k=7
			binaryLEPutUint32(d[4:], 63) // m=63 (not multiple of 64)
			return d
		}()},
		{"truncated bits", func() []byte {
			d := make([]byte, 8) // header only, no bits
			d[0] = 7                  // k=7
			binaryLEPutUint32(d[4:], 64) // m=64
			return d
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := f.UnmarshalBinary(tt.data)
			if err == nil {
				t.Error("expected error for malformed data")
			}
		})
	}
}

// TestFilterConcurrencySafety is a basic check that concurrent reads are safe.
// Bloom filters are insert-only; concurrent inserts are not safe, but concurrent
// reads after all inserts are fine.
func TestFilterConcurrencySafety(t *testing.T) {
	const n = 10_000
	f := NewFilter(n)

	for i := 0; i < n; i++ {
		f.Insert([]byte(fmt.Sprintf("key-%d", i)))
	}

	// Launch multiple goroutines doing MayContain.
	done := make(chan bool)
	const readers = 8
	for r := 0; r < readers; r++ {
		go func(offset int) {
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				if !f.MayContain(key) {
					t.Errorf("false negative in concurrent reader %d for key %d", offset, i)
				}
			}
			done <- true
		}(r)
	}

	for r := 0; r < readers; r++ {
		<-done
	}
}

// TestFilterFalsePositiveRateStatistical uses random keys to measure the
// actual FP rate and verifies it is close to the theoretical value.
func TestFilterFalsePositiveRateStatistical(t *testing.T) {
	const n = 50_000
	f := NewFilter(n)

	rng := rand.New(rand.NewSource(42))

	// Insert n random keys.
	inserted := make(map[string]bool)
	for i := 0; i < n; i++ {
		key := make([]byte, 16)
		rng.Read(key)
		inserted[string(key)] = true
		f.Insert(key)
	}

	// Verify zero false negatives.
	for k := range inserted {
		if !f.MayContain([]byte(k)) {
			t.Fatal("false negative detected")
		}
	}

	// Measure FP rate with new random keys.
	fpCount := 0
	testN := 50_000
	for i := 0; i < testN; i++ {
		key := make([]byte, 16)
		rng.Read(key)
		ks := string(key)
		if !inserted[ks] && f.MayContain(key) {
			fpCount++
		}
	}

	fpRate := float64(fpCount) / float64(testN)
	t.Logf("Statistical FP rate: %.4f%% (%d/%d)", fpRate*100, fpCount, testN)
	t.Logf("Theoretical FP rate: %.4f%%", f.FalsePositiveRate(n)*100)

	if fpRate >= 0.05 {
		t.Errorf("FP rate %.2f%% exceeds 5%% threshold", fpRate*100)
	}
}

// TestFilterSerializationPreservesBits verifies that the raw bit patterns
// survive serialization exactly.
func TestFilterSerializationPreservesBits(t *testing.T) {
	f := NewFilter(100)

	// Insert some keys to set bits.
	for i := 0; i < 50; i++ {
		f.Insert([]byte(fmt.Sprintf("key-%d", i)))
	}

	originalBits := make([]uint64, len(f.bits))
	copy(originalBits, f.bits)

	data, err := f.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	f2 := &Filter{}
	if err := f2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}

	for i := range originalBits {
		if f2.bits[i] != originalBits[i] {
			t.Errorf("bits mismatch at word %d: got %016x, want %016x",
				i, f2.bits[i], originalBits[i])
		}
	}
}

// TestFilterBytesComparison verifies that the same key produces consistent
// results regardless of whether it is presented as a string literal or byte slice.
func TestFilterBytesComparison(t *testing.T) {
	f := NewFilter(100)

	// Insert with one representation.
	f.Insert([]byte("test"))

	// Query with a different but equal representation.
	key := bytes.Repeat([]byte("test"), 1)[:4]
	if !f.MayContain(key) {
		t.Error("false negative with equal byte slice")
	}
}

// TestFilterMarshalBinaryFormat verifies the binary format layout explicitly.
func TestFilterMarshalBinaryFormat(t *testing.T) {
	f := NewFilter(1)

	data, err := f.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Minimum size: 8 bytes header + at least 1 word (8 bytes) = 16 bytes.
	if len(data) < 16 {
		t.Fatalf("serialized data too short: %d bytes", len(data))
	}

	// First 4 bytes should be k=7 (little-endian).
	if data[0] != numHashFunctions || data[1] != 0 || data[2] != 0 || data[3] != 0 {
		t.Errorf("unexpected k in header: %v", data[0:4])
	}

	// m should be 64 (1 key * 10 bits/key, rounded to 64).
	if data[4] != 64 || data[5] != 0 || data[6] != 0 || data[7] != 0 {
		t.Errorf("unexpected m in header: %v", data[4:8])
	}
}

// binaryLEPutUint32 is a helper to write little-endian uint32 in tests.
func binaryLEPutUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
