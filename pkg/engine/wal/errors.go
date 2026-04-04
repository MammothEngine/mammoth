package wal

import "errors"

var (
	errDataTooShort = errors.New("wal: data too short")
	errCRCMismatch  = errors.New("wal: CRC mismatch")
	errClosed       = errors.New("wal: closed")
)
