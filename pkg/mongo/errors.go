package mongo

import "errors"

var (
	// ErrNotFound indicates the requested document or resource was not found.
	ErrNotFound = errors.New("mongo: not found")

	// ErrDuplicateKey indicates a unique constraint violation.
	ErrDuplicateKey = errors.New("mongo: duplicate key")

	// ErrNamespaceExists indicates the database or collection already exists.
	ErrNamespaceExists = errors.New("mongo: namespace already exists")

	// ErrNamespaceNotFound indicates the database or collection does not exist.
	ErrNamespaceNotFound = errors.New("mongo: namespace not found")
)
