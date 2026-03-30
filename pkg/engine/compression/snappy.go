package compression

import "errors"

// SnappyCompressor implements the Snappy block compression format.
// Pure Go, zero external dependencies, interoperable with the standard Snappy format.
type SnappyCompressor struct{}

func (SnappyCompressor) Type() CompressionType { return CompressionSnappy }

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	snappyHashTableBits = 14
	snappyHashTableSize = 1 << snappyHashTableBits // 16384 entries
	snappyMinMatchLen   = 4
	snappyMaxOffset     = 65535 // maximum backward offset for copy elements
)

// ---------------------------------------------------------------------------
// Compress
// ---------------------------------------------------------------------------

func (SnappyCompressor) Compress(data []byte) ([]byte, error) {
	n := len(data)
	if n == 0 {
		return appendVarInt(nil, 0), nil
	}

	// Allocate output buffer with a generous estimate.
	dst := make([]byte, 0, n+n/6+64)
	dst = appendVarInt(dst, uint64(n))

	// Hash table: stores positions as 1-based indices (0 means empty).
	table := make([]int, snappyHashTableSize)

	anchor := 0 // start of current literal run

	for pos := 0; pos < n; {
		// Need at least 4 bytes remaining for a hash match.
		if pos+snappyMinMatchLen > n {
			pos++
			continue
		}

		h := hash4(data, pos)
		candidateIdx := table[h]
		table[h] = pos + 1 // store 1-based

		if candidateIdx == 0 {
			pos++
			continue
		}

		candidate := candidateIdx - 1
		off := pos - candidate
		if off <= 0 || off > snappyMaxOffset || !equal4(data, candidate, pos) {
			pos++
			continue
		}

		// Found a match. Emit pending literals.
		if pos > anchor {
			dst = appendLiteral(dst, data[anchor:pos])
		}

		// Extend the match as far as possible.
		matchLen := extendMatch(data, pos, candidate)

		// Update hash table for the first position we skip.
		if pos+1+3 < n {
			table[hash4(data, pos+1)] = pos + 1 + 1
		}

		// Emit copy element(s).
		dst = emitCopy(dst, off, matchLen)

		pos += matchLen
		anchor = pos
	}

	// Emit final literal run.
	if anchor < n {
		dst = appendLiteral(dst, data[anchor:n])
	}

	return dst, nil
}

// hash4 computes a 14-bit hash from 4 bytes starting at pos.
func hash4(data []byte, pos int) uint32 {
	v := uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
	const prime = 0x1e35a7bd
	return (v * prime) >> (32 - snappyHashTableBits)
}

// equal4 checks if 4 bytes at positions a and b are equal.
func equal4(data []byte, a, b int) bool {
	return data[a] == data[b] && data[a+1] == data[b+1] &&
		data[a+2] == data[b+2] && data[a+3] == data[b+3]
}

// extendMatch extends the match starting at pos/candidate as far as possible.
func extendMatch(data []byte, pos, candidate int) int {
	n := len(data)
	i := snappyMinMatchLen
	for pos+i < n && data[candidate+i] == data[pos+i] {
		i++
	}
	return i
}

// emitCopy emits Snappy copy element(s) for the given offset and length.
// May emit multiple copy elements when length exceeds single-element limits.
// Strategy: prefer copy-2 (3 bytes, up to 64 length) for bulk output, then
// copy-1 (2 bytes, up to 11 length) for the remainder if it saves a byte.
func emitCopy(dst []byte, offset, length int) []byte {
	for length > 0 {
		// If we can use copy-1 and it saves bytes vs copy-2, use it.
		// copy-1: 2 bytes encodes 4-11 (best for remaining 4-11 bytes).
		// copy-2: 3 bytes encodes 1-64 (best for larger chunks).
		if offset < 2048 && length < 12 {
			// Use copy-1 for the remaining 4-11 bytes.
			cl := length
			if cl > 11 {
				cl = 11
			}
			dst = appendCopy1(dst, offset, cl)
			length -= cl
		} else {
			// Use copy-2 for up to 64 bytes per element.
			cl := length
			if cl > 64 {
				cl = 64
			}
			dst = appendCopy2(dst, offset, cl)
			length -= cl
		}
	}
	return dst
}

// appendCopy1 appends a Snappy copy-1 element (1-byte offset, length 4-11).
//
//	Tag byte: bits[1:0] = 01
//	          bits[4:2] = length - 4
//	          bits[7:5] = high 3 bits of offset
//	Next byte: low 8 bits of offset
func appendCopy1(dst []byte, offset, length int) []byte {
	tag := byte(0x01) | byte(length-4)<<2 | byte(offset>>8)<<5
	return append(dst, tag, byte(offset))
}

// appendCopy2 appends a Snappy copy-2 element (2-byte LE offset, length 1-64).
//
//	Tag byte: bits[1:0] = 10
//	          bits[7:2] = length - 1
//	Next 2 bytes: offset (little-endian)
func appendCopy2(dst []byte, offset, length int) []byte {
	tag := byte(0x02) | byte(length-1)<<2
	return append(dst, tag, byte(offset), byte(offset>>8))
}

// appendLiteral appends a Snappy literal element.
//
//	For length < 60:  tag = (length-1)<<2, 1 byte header.
//	For length 60-255: tag = 60<<2, then 1 byte = length-1.
//	For length 256-65535: tag = 61<<2, then 2 bytes LE = length-1.
//	Larger: tag = 62<<2, then 4 bytes LE = length-1.
func appendLiteral(dst, lit []byte) []byte {
	n := len(lit)
	if n == 0 {
		return dst
	}
	switch {
	case n < 60:
		dst = append(dst, byte(n-1)<<2)
	case n < 1<<8:
		dst = append(dst, 60<<2, byte(n-1))
	case n < 1<<16:
		dst = append(dst, 61<<2, byte(n-1), byte((n-1)>>8))
	default:
		dst = append(dst, 62<<2, byte(n-1), byte((n-1)>>8), byte((n-1)>>16), byte((n-1)>>24))
	}
	return append(dst, lit...)
}

// appendVarInt appends a varint-encoded value to dst.
func appendVarInt(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	dst = append(dst, byte(v))
	return dst
}

// ---------------------------------------------------------------------------
// Decompress
// ---------------------------------------------------------------------------

func (SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("compression: empty input")
	}

	// Read the varint-encoded uncompressed length.
	uncompressedLenU64, consumed, err := decodeVarInt(data)
	if err != nil {
		return nil, err
	}
	d := consumed
	if d > len(data) {
		return nil, ErrCorrupt
	}
	if uncompressedLenU64 == 0 {
		if d != len(data) {
			return nil, ErrCorrupt
		}
		return nil, nil
	}
	// On 32-bit platforms, reject impossibly large lengths.
	if uncompressedLenU64 > uint64(int(1<<31-1)) {
		return nil, ErrCorrupt
	}
	uncompressedLen := int(uncompressedLenU64)

	// Pre-allocate output.
	dst := make([]byte, 0, uncompressedLen)

	for d < len(data) {
		tag := data[d]
		d++

		elemType := tag & 0x03

		switch elemType {
		case 0x00:
			// Literal element.
			litLen, err := decodeLiteralLength(tag, data, &d)
			if err != nil {
				return nil, err
			}
			if d+litLen > len(data) {
				return nil, ErrCorrupt
			}
			if len(dst)+litLen > uncompressedLen {
				return nil, ErrCorrupt
			}
			dst = append(dst, data[d:d+litLen]...)
			d += litLen

		case 0x01:
			// Copy with 1-byte offset: length 4-11, offset 0-2047.
			if d >= len(data) {
				return nil, ErrCorrupt
			}
			length := (int(tag)>>2)&0x07 + 4
			offset := int(tag&0xE0)>>5<<8 | int(data[d])
			d++
			if err := copyMatch(dst, offset, length, uncompressedLen); err != nil {
				return nil, err
			}
			dst = appendCopyFrom(dst, offset, length)

		case 0x02:
			// Copy with 2-byte offset: length 1-64, offset 0-65535.
			if d+1 >= len(data) {
				return nil, ErrCorrupt
			}
			length := int(tag>>2) + 1
			offset := int(data[d]) | int(data[d+1])<<8
			d += 2
			if err := copyMatch(dst, offset, length, uncompressedLen); err != nil {
				return nil, err
			}
			dst = appendCopyFrom(dst, offset, length)

		case 0x03:
			// Copy with 4-byte offset: length 1-64, offset 0-2^32-1.
			if d+3 >= len(data) {
				return nil, ErrCorrupt
			}
			length := int(tag>>2) + 1
			offset := int(data[d]) | int(data[d+1])<<8 | int(data[d+2])<<16 | int(data[d+3])<<24
			d += 4
			if err := copyMatch(dst, offset, length, uncompressedLen); err != nil {
				return nil, err
			}
			dst = appendCopyFrom(dst, offset, length)
		}
	}

	if len(dst) != uncompressedLen {
		return nil, ErrCorrupt
	}
	return dst, nil
}

// decodeLiteralLength extracts the literal length from the tag and possibly
// following bytes. Advances *d past any consumed length bytes.
func decodeLiteralLength(tag byte, data []byte, d *int) (int, error) {
	x := tag >> 2
	switch {
	case x < 60:
		return int(x) + 1, nil
	case x == 60:
		if *d >= len(data) {
			return 0, ErrCorrupt
		}
		v := int(data[*d]) + 1
		*d++
		return v, nil
	case x == 61:
		if *d+1 >= len(data) {
			return 0, ErrCorrupt
		}
		v := int(data[*d]) | int(data[*d+1])<<8
		*d += 2
		return v + 1, nil
	case x == 62:
		if *d+3 >= len(data) {
			return 0, ErrCorrupt
		}
		v := int(data[*d]) | int(data[*d+1])<<8 | int(data[*d+2])<<16 | int(data[*d+3])<<24
		*d += 4
		return v + 1, nil
	default:
		return 0, ErrCorrupt
	}
}

// copyMatch validates a copy element's offset and length.
func copyMatch(dst []byte, offset, length, uncompressedLen int) error {
	if offset <= 0 || offset > len(dst) {
		return ErrCorrupt
	}
	if len(dst)+length > uncompressedLen {
		return ErrCorrupt
	}
	return nil
}

// appendCopyFrom copies length bytes from position len(dst)-offset in dst,
// handling overlapping copies (e.g., RLE patterns).
func appendCopyFrom(dst []byte, offset, length int) []byte {
	srcStart := len(dst) - offset
	for i := 0; i < length; i++ {
		dst = append(dst, dst[srcStart+i])
	}
	return dst
}

// decodeVarInt reads a varint from the beginning of data, returning the value
// and the number of bytes consumed.
func decodeVarInt(data []byte) (uint64, int, error) {
	var v uint64
	var s uint
	for i := 0; i < len(data); i++ {
		b := data[i]
		if b < 0x80 {
			if i > 9 || (i == 9 && b > 1) {
				return 0, 0, ErrCorrupt
			}
			return v | uint64(b)<<s, i + 1, nil
		}
		v |= uint64(b&0x7F) << s
		s += 7
	}
	return 0, 0, ErrCorrupt
}
