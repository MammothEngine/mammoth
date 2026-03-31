package main

import (
	"encoding/binary"
	"testing"
)

func TestSkipBSONValue(t *testing.T) {
	tests := []struct {
		name     string
		btype    byte
		setup    func([]byte)
		expected int
	}{
		{
			name:     "double",
			btype:    0x01,
			setup:    func(buf []byte) {},
			expected: 8,
		},
		{
			name:  "string",
			btype: 0x02,
			setup: func(buf []byte) {
				binary.LittleEndian.PutUint32(buf, 4) // len including null
				buf[4] = 'h'
				buf[5] = 'i'
				buf[6] = 0
			},
			expected: 8, // 4 + 4 (including the string "hi\0")
		},
		{
			name:  "document",
			btype: 0x03,
			setup: func(buf []byte) {
				binary.LittleEndian.PutUint32(buf, 10)
			},
			expected: 10,
		},
		{
			name:  "array",
			btype: 0x04,
			setup: func(buf []byte) {
				binary.LittleEndian.PutUint32(buf, 15)
			},
			expected: 15,
		},
		{
			name:  "binary",
			btype: 0x05,
			setup: func(buf []byte) {
				binary.LittleEndian.PutUint32(buf, 4)
				buf[4] = 0 // subtype
			},
			expected: 9, // 4 + 1 (subtype) + 4
		},
		{
			name:     "objectId",
			btype:    0x07,
			setup:    func(buf []byte) {},
			expected: 12,
		},
		{
			name:     "bool",
			btype:    0x08,
			setup:    func(buf []byte) {},
			expected: 1,
		},
		{
			name:     "datetime",
			btype:    0x09,
			setup:    func(buf []byte) {},
			expected: 8,
		},
		{
			name:     "null",
			btype:    0x0A,
			setup:    func(buf []byte) {},
			expected: 0,
		},
		{
			name:     "int32",
			btype:    0x10,
			setup:    func(buf []byte) {},
			expected: 4,
		},
		{
			name:     "timestamp",
			btype:    0x11,
			setup:    func(buf []byte) {},
			expected: 8,
		},
		{
			name:     "int64",
			btype:    0x12,
			setup:    func(buf []byte) {},
			expected: 8,
		},
		{
			name:     "decimal128",
			btype:    0x13,
			setup:    func(buf []byte) {},
			expected: 16,
		},
		{
			name:     "unknown type",
			btype:    0x99,
			setup:    func(buf []byte) {},
			expected: -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 32)
			tc.setup(buf)
			result := skipBSONValue(buf, 0, tc.btype)
			if result != tc.expected {
				t.Errorf("skipBSONValue(0, 0x%02x) = %d, want %d", tc.btype, result, tc.expected)
			}
		})
	}
}

func TestSkipBSONValueShortBuffer(t *testing.T) {
	// String type with short buffer
	buf := make([]byte, 2)
	result := skipBSONValue(buf, 0, 0x02)
	if result != -1 {
		t.Errorf("expected -1 for short buffer, got %d", result)
	}

	// Document type with short buffer
	buf = make([]byte, 3)
	result = skipBSONValue(buf, 0, 0x03)
	if result != -1 {
		t.Errorf("expected -1 for short buffer, got %d", result)
	}

	// Binary type with short buffer
	buf = make([]byte, 3)
	result = skipBSONValue(buf, 0, 0x05)
	if result != -1 {
		t.Errorf("expected -1 for short buffer, got %d", result)
	}
}

func TestSkipBSONValueNonZeroPos(t *testing.T) {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint32(buf[4:], 4) // len including null
	buf[8] = 'h'
	buf[9] = 'i'
	buf[10] = 0

	result := skipBSONValue(buf, 4, 0x02)
	if result != 12 { // 4 + 4 + 4
		t.Errorf("skipBSONValue with offset = %d, want 12", result)
	}
}
