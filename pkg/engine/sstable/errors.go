package sstable

import "errors"

var (
	errInvalidFooter   = errors.New("sstable: invalid footer")
	errInvalidFormat   = errors.New("sstable: invalid format")
	errKeyNotFound     = errors.New("sstable: key not found")
	errCorruptBlock    = errors.New("sstable: corrupt block")
	errReaderClosed    = errors.New("sstable: reader closed")
)
