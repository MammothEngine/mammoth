package compression

import "encoding/binary"

// ZstdCompressor implements a minimal Zstd-compatible compression format.
// Pure Go, zero external dependencies.
// Uses simplified internal format: [magic(4)][origLen(4 LE)][blocks...]
// Each block: [blockType(1)][blockLen(4 LE)][blockData]
// blockType: 0=raw, 1=compressed
type ZstdCompressor struct{}

func (ZstdCompressor) Type() CompressionType { return CompressionZstd }

const (
	zstdMagic     = 0xFD2FB528
	zstdBlockMax  = 128 * 1024
	zstdHTBits    = 16
	zstdHTSize    = 1 << zstdHTBits
	zstdMinMatch  = 3
	zstdMaxWindow = 1 << 16
)

// Compress compresses data.
func (ZstdCompressor) Compress(data []byte) ([]byte, error) {
	n := len(data)
	if n == 0 {
		return nil, nil
	}

	dst := make([]byte, 0, n+n/6+32)

	// Header: magic + origLen
	var magic [4]byte
	binary.LittleEndian.PutUint32(magic[:], zstdMagic)
	dst = append(dst, magic[:]...)
	var ol [4]byte
	binary.LittleEndian.PutUint32(ol[:], uint32(n))
	dst = append(dst, ol[:]...)

	// Compress in blocks
	for off := 0; off < n; {
		end := off + zstdBlockMax
		if end > n {
			end = n
		}
		block := data[off:end]
		off = end

		compressed := zstdCompressBlock(block)
		if len(compressed) == 0 || len(compressed) >= len(block) {
			// Raw block
			dst = append(dst, 0) // type=raw
			var bl [4]byte
			binary.LittleEndian.PutUint32(bl[:], uint32(len(block)))
			dst = append(dst, bl[:]...)
			dst = append(dst, block...)
		} else {
			// Compressed block
			dst = append(dst, 1) // type=compressed
			var bl [4]byte
			binary.LittleEndian.PutUint32(bl[:], uint32(len(compressed)))
			dst = append(dst, bl[:]...)
			dst = append(dst, compressed...)
		}
	}

	return dst, nil
}

// Decompress decompresses data.
func (ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 8 {
		return nil, ErrCorrupt
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != zstdMagic {
		return nil, ErrCorrupt
	}
	origLen := int(binary.LittleEndian.Uint32(data[4:8]))
	pos := 8

	dst := make([]byte, 0, origLen)

	for pos < len(data) {
		if pos >= len(data) {
			return nil, ErrCorrupt
		}
		blockType := data[pos]
		pos++

		if pos+3 >= len(data) {
			return nil, ErrCorrupt
		}
		blockLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+blockLen > len(data) {
			return nil, ErrCorrupt
		}
		blockData := data[pos : pos+blockLen]
		pos += blockLen

		switch blockType {
		case 0: // Raw
			dst = append(dst, blockData...)
		case 1: // Compressed
			dec, err := zstdDecompressBlock(blockData)
			if err != nil {
				return nil, err
			}
			dst = append(dst, dec...)
		default:
			return nil, ErrCorrupt
		}
	}

	if origLen > 0 && len(dst) != origLen {
		return nil, ErrCorrupt
	}
	return dst, nil
}

// zstdCompressBlock compresses a single block.
// Format: [litCount(4 LE)][literals...][numSeq(2 LE)][sequences...]
// Each sequence: [litLen(4 LE)][matchLen(4 LE)][offset(2 LE)]
// The literals for each sequence are interleaved.
func zstdCompressBlock(data []byte) []byte {
	n := len(data)
	if n == 0 {
		return nil
	}

	type seq struct {
		litLen  int
		matchLn int
		offset  int
	}

	table := make([]int, zstdHTSize)
	var seqs []seq
	anchor := 0

	for pos := 0; pos < n; {
		if pos+zstdMinMatch > n {
			break
		}

		h := zstdHash(data, pos)
		candidate := table[h]
		table[h] = pos

		if candidate == 0 || pos-candidate > zstdMaxWindow {
			pos++
			continue
		}

		if data[candidate] != data[pos] || data[candidate+1] != data[pos+1] ||
			data[candidate+2] != data[pos+2] {
			pos++
			continue
		}

		ml := zstdMinMatch
		for pos+ml < n && data[candidate+ml] == data[pos+ml] {
			ml++
		}

		seqs = append(seqs, seq{litLen: pos - anchor, matchLn: ml, offset: pos - candidate})
		pos += ml
		anchor = pos
	}

	if len(seqs) == 0 {
		return nil // fall back to raw
	}

	// Collect all literals
	var lits []byte
	cur := 0
	for _, s := range seqs {
		lits = append(lits, data[cur:cur+s.litLen]...)
		cur += s.litLen + s.matchLn
	}
	if cur < n {
		lits = append(lits, data[cur:]...)
	}

	// Encode
	estSize := 4 + len(lits) + 2 + len(seqs)*10
	if estSize >= n {
		return nil
	}

	out := make([]byte, 0, estSize)

	// Literal count + literals
	var lc [4]byte
	binary.LittleEndian.PutUint32(lc[:], uint32(len(lits)))
	out = append(out, lc[:]...)
	out = append(out, lits...)

	// Sequence count
	var sc [2]byte
	binary.LittleEndian.PutUint16(sc[:], uint16(len(seqs)))
	out = append(out, sc[:]...)

	// Sequences
	for _, s := range seqs {
		var ll [4]byte
		binary.LittleEndian.PutUint32(ll[:], uint32(s.litLen))
		out = append(out, ll[:]...)

		var ml [4]byte
		binary.LittleEndian.PutUint32(ml[:], uint32(s.matchLn))
		out = append(out, ml[:]...)

		out = append(out, byte(s.offset), byte(s.offset>>8))
	}

	if len(out) >= n {
		return nil
	}

	return out
}

func zstdDecompressBlock(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, ErrCorrupt
	}

	pos := 0
	litCount := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if pos+litCount > len(data) {
		return nil, ErrCorrupt
	}
	literals := data[pos : pos+litCount]
	pos += litCount

	if pos+1 >= len(data) {
		// No sequences, just literals
		return literals, nil
	}

	numSeq := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	var dst []byte
	litPos := 0

	for i := 0; i < numSeq; i++ {
		if pos+9 > len(data) {
			return nil, ErrCorrupt
		}

		ll := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		ml := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		offset := int(data[pos]) | int(data[pos+1])<<8
		pos += 2

		// Copy literals
		end := litPos + ll
		if end > len(literals) {
			end = len(literals)
		}
		dst = append(dst, literals[litPos:end]...)
		litPos = end

		// Copy match
		if offset == 0 || offset > len(dst) {
			return nil, ErrCorrupt
		}
		srcStart := len(dst) - offset
		for j := 0; j < ml; j++ {
			dst = append(dst, dst[srcStart+j])
		}
	}

	// Remaining literals
	if litPos < len(literals) {
		dst = append(dst, literals[litPos:]...)
	}

	return dst, nil
}

func zstdHash(data []byte, pos int) uint32 {
	v := uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16
	return (v * 70607) >> (32 - zstdHTBits)
}
