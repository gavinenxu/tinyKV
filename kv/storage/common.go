package storage

import "github.com/pkg/errors"

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrEngineNotInitiate = errors.New("engine not initiate")
)
