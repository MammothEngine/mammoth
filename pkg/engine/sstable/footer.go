package sstable

import "encoding/binary"

const (
	footerSize     = 48
	magicNumber    = 0x4D414D4D // "MAMM"
	magicBytesSize = 4
)

// Footer holds SSTable metadata stored at the end of the file.
// Layout: bloomOffset(8) + bloomLength(8) + indexOffset(8) + indexLength(8) + metaOffset(8) + metaLength(8)
// Total: 48 bytes (last 4 bytes are magic "MAMM")
type Footer struct {
	BloomOffset  uint64
	BloomLength  uint64
	IndexOffset  uint64
	IndexLength  uint64
	MetaOffset   uint64
	Magic        uint32
}

// Encode serializes the footer.
func (f *Footer) Encode() []byte {
	buf := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(buf[0:], f.BloomOffset)
	binary.LittleEndian.PutUint64(buf[8:], f.BloomLength)
	binary.LittleEndian.PutUint64(buf[16:], f.IndexOffset)
	binary.LittleEndian.PutUint64(buf[24:], f.IndexLength)
	binary.LittleEndian.PutUint64(buf[32:], f.MetaOffset)
	binary.LittleEndian.PutUint32(buf[40:], uint32(f.MetaOffset>>32)) // padding
	binary.LittleEndian.PutUint32(buf[44:], magicNumber)
	return buf
}

// DecodeFooter parses a footer from bytes.
func DecodeFooter(data []byte) (Footer, error) {
	if len(data) < footerSize {
		return Footer{}, errInvalidFooter
	}

	magic := binary.LittleEndian.Uint32(data[44:])
	if magic != magicNumber {
		return Footer{}, errInvalidFooter
	}

	f := Footer{
		BloomOffset: binary.LittleEndian.Uint64(data[0:]),
		BloomLength: binary.LittleEndian.Uint64(data[8:]),
		IndexOffset: binary.LittleEndian.Uint64(data[16:]),
		IndexLength: binary.LittleEndian.Uint64(data[24:]),
		MetaOffset:  binary.LittleEndian.Uint64(data[32:]),
	}
	return f, nil
}

func init() {
	_ = magicBytesSize
}
