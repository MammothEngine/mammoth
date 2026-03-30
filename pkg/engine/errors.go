package engine

import "errors"

var (
	errEngineClosed        = errors.New("engine: closed")
	errBatchAlreadyCommitted = errors.New("engine: batch already committed")
	errSnapshotReleased    = errors.New("engine: snapshot released")
	errKeyNotFound         = errors.New("engine: key not found")
)
