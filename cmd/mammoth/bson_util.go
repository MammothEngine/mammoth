package main

import "encoding/binary"

// skipBSONValue advances past a BSON value of the given type.
// Returns the next position, or -1 if the buffer is too short.
func skipBSONValue(buf []byte, pos int, btype byte) int {
	switch btype {
	case 0x01: // double
		return pos + 8
	case 0x02: // string
		if pos+4 > len(buf) {
			return -1
		}
		strLen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		return pos + 4 + strLen
	case 0x03, 0x04: // document, array
		if pos+4 > len(buf) {
			return -1
		}
		docLen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		return pos + docLen
	case 0x05: // binary
		if pos+4 > len(buf) {
			return -1
		}
		binLen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		return pos + 5 + binLen
	case 0x07: // ObjectId
		return pos + 12
	case 0x08: // bool
		return pos + 1
	case 0x09: // datetime
		return pos + 8
	case 0x0A: // null
		return pos
	case 0x10: // int32
		return pos + 4
	case 0x11: // timestamp
		return pos + 8
	case 0x12: // int64
		return pos + 8
	case 0x13: // decimal128
		return pos + 16
	default:
		return -1
	}
}
