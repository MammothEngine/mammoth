package compression

// LZ4Compressor implements the LZ4 block compression format.
// Pure Go, zero external dependencies.
// Format: [flag(1)][origLen(4 LE)][data]
// flag=0: data is compressed LZ4 sequences
// flag=1: data is stored raw (uncompressed)
type LZ4Compressor struct{}

func (LZ4Compressor) Type() CompressionType { return CompressionLZ4 }

const (
	lz4HTBits   = 16
	lz4HTSize   = 1 << lz4HTBits
	lz4MinMatch = 4
	lz4MaxOff   = 65535
)

// Compress compresses data using LZ4 block format.
func (LZ4Compressor) Compress(data []byte) ([]byte, error) {
	n := len(data)
	if n == 0 {
		return nil, nil
	}

	// Reserve header: flag(1) + origLen(4)
	dst := make([]byte, 5, n+n/6+16)
	// origLen in header
	dst[1] = byte(n)
	dst[2] = byte(n >> 8)
	dst[3] = byte(n >> 16)
	dst[4] = byte(n >> 24)

	table := make([]int, lz4HTSize)
	anchor := 0

	for pos := 0; pos < n; {
		if pos+lz4MinMatch > n {
			break
		}

		h := lz4Hash(data, pos)
		candidate := table[h]
		table[h] = pos

		if candidate == 0 || pos-candidate > lz4MaxOff {
			pos++
			continue
		}

		if data[candidate] != data[pos] || data[candidate+1] != data[pos+1] ||
			data[candidate+2] != data[pos+2] || data[candidate+3] != data[pos+3] {
			pos++
			continue
		}

		ml := lz4MinMatch
		for pos+ml < n && data[candidate+ml] == data[pos+ml] {
			ml++
		}

		litLen := pos - anchor
		offset := pos - candidate

		// Token
		token := byte(0)
		if litLen >= 15 {
			token |= 0xF0
		} else {
			token |= byte(litLen << 4)
		}
		mc := ml - lz4MinMatch
		if mc >= 15 {
			token |= 0x0F
		} else {
			token |= byte(mc)
		}
		dst = append(dst, token)

		// Literal length extension
		if litLen >= 15 {
			rem := litLen - 15
			for rem >= 255 {
				dst = append(dst, 255)
				rem -= 255
			}
			dst = append(dst, byte(rem))
		}

		// Literals
		dst = append(dst, data[anchor:pos]...)

		// Offset (2 bytes LE)
		dst = append(dst, byte(offset), byte(offset>>8))

		// Match length extension
		if mc >= 15 {
			rem := mc - 15
			for rem >= 255 {
				dst = append(dst, 255)
				rem -= 255
			}
			dst = append(dst, byte(rem))
		}

		pos += ml
		anchor = pos
	}

	// Final literals (no match at end)
	if anchor < n {
		litLen := n - anchor
		token := byte(0)
		if litLen >= 15 {
			token = 0xF0
		} else {
			token = byte(litLen << 4)
		}
		dst = append(dst, token)

		if litLen >= 15 {
			rem := litLen - 15
			for rem >= 255 {
				dst = append(dst, 255)
				rem -= 255
			}
			dst = append(dst, byte(rem))
		}
		dst = append(dst, data[anchor:n]...)
	}

	// Check if compression helps
	if len(dst) >= n+5 {
		// Store uncompressed
		out := make([]byte, 5+n)
		out[0] = 1 // raw flag
		out[1] = byte(n)
		out[2] = byte(n >> 8)
		out[3] = byte(n >> 16)
		out[4] = byte(n >> 24)
		copy(out[5:], data)
		return out, nil
	}

	// Compressed
	dst[0] = 0 // compressed flag
	return dst, nil
}

// Decompress decompresses LZ4 block format data.
func (LZ4Compressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 5 {
		return nil, ErrCorrupt
	}

	flag := data[0]
	origLen := int(uint32(data[1]) | uint32(data[2])<<8 | uint32(data[3])<<16 | uint32(data[4])<<24)

	if flag == 1 {
		// Raw
		if origLen+5 != len(data) {
			return nil, ErrCorrupt
		}
		out := make([]byte, origLen)
		copy(out, data[5:])
		return out, nil
	}

	if flag != 0 {
		return nil, ErrCorrupt
	}

	dst := make([]byte, 0, origLen)
	pos := 5

	for pos < len(data) {
		token := data[pos]
		pos++

		// Literal length (high 4 bits)
		litLen := int(token >> 4)
		if litLen == 15 {
			for pos < len(data) {
				b := data[pos]
				pos++
				litLen += int(b)
				if b != 255 {
					break
				}
			}
		}

		if pos+litLen > len(data) {
			return nil, ErrCorrupt
		}
		dst = append(dst, data[pos:pos+litLen]...)
		pos += litLen

		// Match (last sequence has no match)
		if pos >= len(data) {
			break
		}

		// Offset (2 bytes LE)
		if pos+1 >= len(data) {
			return nil, ErrCorrupt
		}
		offset := int(data[pos]) | int(data[pos+1])<<8
		pos += 2
		if offset == 0 || offset > len(dst) {
			return nil, ErrCorrupt
		}

		// Match length (low 4 bits + 4)
		matchLen := int(token&0x0F) + lz4MinMatch
		if token&0x0F == 15 {
			for pos < len(data) {
				b := data[pos]
				pos++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}

		// Copy from back reference
		srcStart := len(dst) - offset
		for i := 0; i < matchLen; i++ {
			dst = append(dst, dst[srcStart+i])
		}
	}

	if len(dst) != origLen {
		return nil, ErrCorrupt
	}
	return dst, nil
}

func lz4Hash(data []byte, pos int) uint32 {
	v := uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
	return (v * 2654435761) >> (32 - lz4HTBits)
}
