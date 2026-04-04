package sstable

import "errors"

var (
	errInvalidFooter = errors.New("sstable: invalid footer")
	errKeyNotFound   = errors.New("sstable: key not found")
)
