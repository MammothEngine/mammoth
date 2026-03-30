package bloom

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	// bitsPerKey is the target number of bits per key, yielding ~1% FP rate
	// with 7 hash functions.
	bitsPerKey = 10

	// numHashFunctions is the number of hash functions used.
	// With 10 bits/key and 7 hash functions the theoretical FP rate is ~0.82%.
	numHashFunctions = 7

	// defaultSeed is the hash seed used for Murmur3.
	defaultSeed uint32 = 0xbc9f1d34
)

// Filter is a space-efficient probabilistic data structure that tests whether
// an element is possibly a member of a set. False positives are possible;
// false negatives are not.
//
// The filter uses a bit array backed by a uint64 slice and derives k hash
// positions from a single MurmurHash3 128-bit computation.
type Filter struct {
	bits []uint64 // bit array stored as uint64 words
	k    int      // number of hash functions
	m    uint     // total number of bits
}

// NewFilter creates a Bloom filter optimized for the given expected number of
// keys. It uses ~10 bits per key and 7 hash functions, achieving a theoretical
// false positive rate of approximately 1%.
//
// If expectedKeys is zero or negative, a minimal filter is created.
func NewFilter(expectedKeys int) *Filter {
	if expectedKeys <= 0 {
		expectedKeys = 1
	}

	m := uint(expectedKeys) * bitsPerKey
	// Round up to nearest multiple of 64 so we use whole uint64 words.
	m = ((m + 63) / 64) * 64

	nWords := m / 64
	return &Filter{
		bits: make([]uint64, nWords),
		k:    numHashFunctions,
		m:    m,
	}
}

// EmptyFilter returns a filter that matches nothing. It has a single uint64
// word (all zeros) so MayContain always returns false.
func EmptyFilter() *Filter {
	return &Filter{
		bits: make([]uint64, 1),
		k:    numHashFunctions,
		m:    64,
	}
}

// Insert adds a key to the Bloom filter. After insertion, MayContain will
// always return true for this key.
func (f *Filter) Insert(key []byte) {
	h1, h2 := Murmur3Sum128(key, defaultSeed)
	for i := 0; i < f.k; i++ {
		// Double hashing: h_i = (h1 + i * h2) mod m
		pos := uint((h1 + uint64(i)*h2) % uint64(f.m))
		f.setBit(pos)
	}
}

// MayContain returns true if the key might have been inserted into the filter,
// and false if the key was definitely not inserted. False positives are
// possible; false negatives are never possible.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := Murmur3Sum128(key, defaultSeed)
	for i := 0; i < f.k; i++ {
		pos := uint((h1 + uint64(i)*h2) % uint64(f.m))
		if !f.getBit(pos) {
			return false
		}
	}
	return true
}

// setBit sets the bit at the given position in the bit array.
func (f *Filter) setBit(pos uint) {
	word := pos / 64
	bit := pos % 64
	f.bits[word] |= 1 << bit
}

// getBit returns true if the bit at the given position is set.
func (f *Filter) getBit(pos uint) bool {
	word := pos / 64
	bit := pos % 64
	return f.bits[word]&(1<<bit) != 0
}

// MarshalBinary serializes the Bloom filter into a byte slice.
//
// Binary layout:
//
//	[0:4]  k   (uint32 little-endian)
//	[4:8]  m   (uint32 little-endian)
//	[8:]   bits (raw uint64 words, little-endian)
func (f *Filter) MarshalBinary() ([]byte, error) {
	nWords := len(f.bits)
	buf := make([]byte, 8+nWords*8)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(f.k))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(f.m))

	for i, word := range f.bits {
		binary.LittleEndian.PutUint64(buf[8+i*8:], word)
	}

	return buf, nil
}

// UnmarshalBinary deserializes a Bloom filter from a byte slice produced by
// MarshalBinary. It overwrites the receiver's state entirely.
func (f *Filter) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return errors.New("bloom: data too short to deserialize")
	}

	k := int(binary.LittleEndian.Uint32(data[0:4]))
	m := uint(binary.LittleEndian.Uint32(data[4:8]))

	if k <= 0 || k > 64 {
		return errors.New("bloom: invalid number of hash functions")
	}
	if m == 0 || m%64 != 0 {
		return errors.New("bloom: invalid bit count")
	}

	expectedLen := 8 + int(m/64)*8
	if len(data) < expectedLen {
		return errors.New("bloom: data truncated")
	}

	nWords := m / 64
	bits := make([]uint64, nWords)
	for i := 0; i < int(nWords); i++ {
		bits[i] = binary.LittleEndian.Uint64(data[8+i*8:])
	}

	f.k = k
	f.m = m
	f.bits = bits
	return nil
}

// AllowKeyUpdate enables re-insertion of keys for testing purposes. This is
// equivalent to calling Insert; all previously inserted keys remain.
func (f *Filter) AllowKeyUpdate() {
	// Bloom filters are inherently updatable. This method exists as a
	// testing hook and is a no-op. Keys can always be re-inserted.
}

// Size returns the approximate memory usage of the Bloom filter in bytes.
func (f *Filter) Size() int {
	// bits slice + struct overhead (3 fields ~ 48 bytes on 64-bit).
	return len(f.bits)*8 + 48
}

// FalsePositiveRate returns the theoretical false positive rate for the given
// number of inserted keys based on the current filter parameters.
func (f *Filter) FalsePositiveRate(numKeys int) float64 {
	if f.m == 0 || numKeys == 0 {
		return 0
	}
	// FP rate = (1 - e^(-k*n/m))^k
	exponent := -float64(f.k) * float64(numKeys) / float64(f.m)
	return math.Pow(1-math.Exp(exponent), float64(f.k))
}

// BitsPerKey returns the actual bits per key for the given number of keys.
func (f *Filter) BitsPerKey(numKeys int) float64 {
	if numKeys == 0 {
		return float64(f.m)
	}
	return float64(f.m) / float64(numKeys)
}
