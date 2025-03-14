// Package pebbleutil provides utility functions for working with Pebble databases.
package pebbleutil

import (
	"io"
	"slices"
)

// GetDup duplicates the result of a Pebble Get operation.
//
// This function is useful when handling Pebble DB Get() results which return a byte slice
// that is only valid until the Closer is closed. GetDup takes the byte slice, makes a copy
// using slices.Clone, and handles the Closer, allowing the returned byte slice to outlive
// the original database cursor.
//
// Parameters:
//   - b: Byte slice from a Pebble Get operation
//   - cl: Closer from the Pebble Get operation
//   - err: Error from the Pebble Get operation
//
// Returns:
//   - A cloned copy of the byte slice that can be safely used after this function returns
//   - The original error if one occurred
func GetDup(b []byte, cl io.Closer, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	return slices.Clone(b), nil
}

// must is a generic helper that panics if an error occurs.
//
// This utility function simplifies error handling in contexts where errors
// indicate a fatal problem and normal recovery is not possible or desired.
// It's commonly used inside iterator creation where errors are not expected
// and cannot be reasonably recovered from.
//
// Type parameter:
//   - T: The type of the value to be returned
//
// Parameters:
//   - v: The value to return if no error occurred
//   - err: The error to check
//
// Returns:
//   - The value v if err is nil
//
// Panics if err is not nil
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
