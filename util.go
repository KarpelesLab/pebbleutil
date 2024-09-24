package pebbleutil

import (
	"io"
	"slices"
)

// GetDup allows duplicating the result of a get
func GetDup(b []byte, cl io.Closer, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	return slices.Clone(b), nil
}

// must panics if err is not nil
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
