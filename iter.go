// Package pebbleutil provides utility functions for working with Pebble databases.
package pebbleutil

import (
	"slices"

	"github.com/cockroachdb/pebble"
)

// Prefix returns an iterator function that yields key-value pairs with the given prefix.
//
// This function allows using Go 1.23 style iterators with Pebble for a given subset of keys
// based on a prefix. It creates an iterator that iterates through all keys starting with the
// provided prefix, in ascending order.
//
// Parameters:
//   - db: A Pebble database reader interface
//   - pfx: The key prefix to iterate over (all keys starting with this prefix)
//   - seek: Optional starting point within the prefix range (can be nil to start at beginning)
//
// Returns:
//   - A range iterator function compatible with Go 1.23's range-over-func syntax
//
// The returned function takes a yield function that will be called for each key-value pair.
// If the yield function returns false, iteration stops immediately.
//
// Example usage:
//
//	for k, v := range pebbleutil.Prefix(db, []byte("users:"), nil) {
//	    // Process each key-value pair
//	}
func Prefix(db pebble.Reader, pfx, seek []byte) func(yield func(k, v []byte) bool) {
	return func(yield func(k, v []byte) bool) {
		// Create an iterator for the prefix range
		iter := must(PrefixIter(db, pfx))
		defer iter.Close()

		// Position the iterator at the starting point
		if seek != nil {
			// Start at or after the seek key
			iter.SeekGE(seek)
		} else {
			// Start at the first key in the prefix range
			iter.First()
		}

		// Iterate through all matching keys
		for ; iter.Valid(); iter.Next() {
			// Call the yield function with the current key-value pair
			// If it returns false, stop iteration
			if !yield(iter.Key(), iter.Value()) {
				return
			}
		}
	}
}

// ReversePrefix returns an iterator function that yields key-value pairs with the given prefix,
// but in reverse order (from highest to lowest key).
//
// Parameters:
//   - db: A Pebble database reader interface
//   - pfx: The key prefix to iterate over (all keys starting with this prefix)
//   - seek: Optional starting point within the prefix range (can be nil to start at end)
//     If provided, iteration starts at the key LESS THAN the seek key (unlike Prefix which
//     starts at or after the seek key)
//
// Returns:
//   - A range iterator function compatible with Go 1.23's range-over-func syntax
//
// Example usage:
//
//	for k, v := range pebbleutil.ReversePrefix(db, []byte("users:"), nil) {
//	    // Process each key-value pair in reverse order
//	}
func ReversePrefix(db pebble.Reader, pfx, seek []byte) func(yield func(k, v []byte) bool) {
	return func(yield func(k, v []byte) bool) {
		// Create an iterator for the prefix range
		iter := must(PrefixIter(db, pfx))
		defer iter.Close()

		// Position the iterator at the starting point
		if seek != nil {
			// Start at the key less than the seek key
			iter.SeekLT(seek)
		} else {
			// Start at the last key in the prefix range
			iter.Last()
		}

		// Iterate through all matching keys in reverse order
		for ; iter.Valid(); iter.Prev() {
			// Call the yield function with the current key-value pair
			// If it returns false, stop iteration
			if !yield(iter.Key(), iter.Value()) {
				return
			}
		}
	}
}

// All returns an iterator function that yields all key-value pairs in the database,
// optionally starting from a specific key.
//
// Parameters:
//   - db: A Pebble database reader interface
//   - seek: Optional starting point (can be nil to start at beginning)
//
// Returns:
//   - A range iterator function compatible with Go 1.23's range-over-func syntax
//
// Example usage:
//
//	for k, v := range pebbleutil.All(db, nil) {
//	    // Process each key-value pair in the entire database
//	}
func All(db pebble.Reader, seek []byte) func(yield func(k, v []byte) bool) {
	return func(yield func(k, v []byte) bool) {
		// Create an iterator with no bounds
		iter := must(db.NewIter(nil))
		defer iter.Close()

		// Position the iterator at the starting point
		if seek != nil {
			// Start at or after the seek key
			iter.SeekGE(seek)
		} else {
			// Start at the first key
			iter.First()
		}

		// Iterate through all keys
		for ; iter.Valid(); iter.Next() {
			// Call the yield function with the current key-value pair
			// If it returns false, stop iteration
			if !yield(iter.Key(), iter.Value()) {
				return
			}
		}
	}
}

// Range returns an iterator function that yields key-value pairs within a specific
// key range [start, end), where end is not included.
//
// Parameters:
//   - db: A Pebble database reader interface
//   - start: The inclusive lower bound of the range (can be nil for no lower bound)
//   - end: The exclusive upper bound of the range (can be nil for no upper bound)
//
// Returns:
//   - A range iterator function compatible with Go 1.23's range-over-func syntax
//
// Example usage:
//
//	for k, v := range pebbleutil.Range(db, []byte("users:1000"), []byte("users:2000")) {
//	    // Process user records with IDs from 1000 up to (but not including) 2000
//	}
func Range(db pebble.Reader, start, end []byte) func(yield func(k, v []byte) bool) {
	// Configure iterator options with range bounds
	opts := &pebble.IterOptions{
		LowerBound: start, // Inclusive lower bound
		UpperBound: end,   // Exclusive upper bound
	}

	return func(yield func(k, v []byte) bool) {
		// Create a bounded iterator
		iter := must(db.NewIter(opts))
		defer iter.Close()

		// Iterate through all keys in the range
		for iter.First(); iter.Valid(); iter.Next() {
			// Call the yield function with the current key-value pair
			// If it returns false, stop iteration
			if !yield(iter.Key(), iter.Value()) {
				return
			}
		}
	}
}

// IncrementBytesArray adds 1 to a byte array, treating it as a big-endian number.
// This is useful for creating upper bounds for key ranges in Pebble.
//
// The function adds 1 to the rightmost byte and handles carry properly:
//   - 123456 becomes 123457
//   - 1234ff becomes 123500
//   - ffffff becomes nil (indicating no upper bound is possible)
//
// Parameters:
//   - uppr: The byte array to increment (will be cloned, original is not modified)
//
// Returns:
//   - A new byte array with the value incremented by 1
//   - nil if the input is already at maximum value (all bytes are 0xff)
func IncrementBytesArray(uppr []byte) []byte {
	// Clone the input to avoid modifying the original
	uppr = slices.Clone(uppr)

	// Start from the least significant byte (rightmost)
	pos := len(uppr) - 1

	for {
		// Handle carry case
		if uppr[pos] == 0xff {
			if pos == 0 {
				// If we've carried through the entire array, there's no possible increment
				return nil
			}
			// Reset this byte to zero and carry to the next position
			uppr[pos] = 0
			pos -= 1
			continue
		}

		// Normal case: simply increment the current byte and return
		uppr[pos] += 1
		return uppr
	}
}

// PrefixIter returns a Pebble iterator configured to iterate over keys with the specified prefix.
//
// This is a lower-level function used by Prefix and ReversePrefix. It configures a Pebble iterator
// with proper bounds to only include keys starting with the given prefix.
//
// Parameters:
//   - db: A Pebble database reader interface
//   - pfx: The key prefix to iterate over
//
// Returns:
//   - A configured Pebble iterator
//   - Any error encountered when creating the iterator
func PrefixIter(db pebble.Reader, pfx []byte) (*pebble.Iterator, error) {
	// Configure the iterator to bound the range to exactly the prefix
	opts := &pebble.IterOptions{
		LowerBound: pfx,                      // Start at the prefix
		UpperBound: IncrementBytesArray(pfx), // End just after the prefix (exclusive)
	}
	return db.NewIter(opts)
}
