package wal

import (
	"encoding/binary"
	"hash/crc32"
)

// RecordType defines the type of a WAL record.
type RecordType uint8

const (
	RecordFull  RecordType = 1
	RecordFirst RecordType = 2
	RecordMiddle RecordType = 3
	RecordLast  RecordType = 4
)

// Record represents a single WAL record.
type Record struct {
	Type    RecordType
	SeqNum  uint64
	Payload []byte
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Encode encodes a record to bytes.
// Format: type(1) + seqNum(8) + payloadLen(4) + payload + crc32c(4)
func (r *Record) Encode() []byte {
	size := 1 + 8 + 4 + len(r.Payload) + 4
	buf := make([]byte, size)

	buf[0] = byte(r.Type)
	binary.LittleEndian.PutUint64(buf[1:], r.SeqNum)
	binary.LittleEndian.PutUint32(buf[9:], uint32(len(r.Payload)))
	copy(buf[13:], r.Payload)

	// CRC over type + seqNum + payload
	crc := crc32.Checksum(buf[:13+len(r.Payload)], crcTable)
	binary.LittleEndian.PutUint32(buf[13+len(r.Payload):], crc)

	return buf
}

// RecordSize returns the encoded size of a record.
func RecordSize(payloadLen int) int {
	return 1 + 8 + 4 + payloadLen + 4
}

// DecodeRecord decodes a record from bytes. Returns the record and bytes consumed.
func DecodeRecord(data []byte) (Record, int, error) {
	if len(data) < 13 {
		return Record{}, 0, errDataTooShort
	}

	rec := Record{
		Type:   RecordType(data[0]),
		SeqNum: binary.LittleEndian.Uint64(data[1:9]),
	}

	payloadLen := int(binary.LittleEndian.Uint32(data[9:13]))
	if len(data) < 13+payloadLen+4 {
		return Record{}, 0, errDataTooShort
	}

	rec.Payload = make([]byte, payloadLen)
	copy(rec.Payload, data[13:13+payloadLen])

	// Verify CRC
	expectedCRC := binary.LittleEndian.Uint32(data[13+payloadLen:])
	actualCRC := crc32.Checksum(data[:13+payloadLen], crcTable)
	if expectedCRC != actualCRC {
		return Record{}, 0, errCRCMismatch
	}

	return rec, 13 + payloadLen + 4, nil
}
