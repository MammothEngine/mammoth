// Package compression provides block compression algorithms for the Mammoth Engine.
// All implementations are pure Go with zero external dependencies.
package compression

import "errors"

// CompressionType identifies the compression algorithm.
type CompressionType int

const (
	CompressionNone  CompressionType = 0
	CompressionSnappy CompressionType = 1
	CompressionLZ4    CompressionType = 2
	CompressionZstd   CompressionType = 3
)

// Compressor is the interface for compression/decompression of byte slices.
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Type() CompressionType
}

// GetCompressor returns the Compressor for the given CompressionType.
func GetCompressor(t CompressionType) Compressor {
	switch t {
	case CompressionNone:
		return NoneCompressor{}
	case CompressionSnappy:
		return SnappyCompressor{}
	default:
		return NoneCompressor{}
	}
}

// NoneCompressor is a pass-through compressor that returns input unchanged.
type NoneCompressor struct{}

func (NoneCompressor) Compress(data []byte) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (NoneCompressor) Decompress(data []byte) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (NoneCompressor) Type() CompressionType { return CompressionNone }

var (
	ErrCorrupt       = errors.New("compression: corrupt input")
	ErrUnsupported   = errors.New("compression: unsupported compression type")
)
