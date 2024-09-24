package pebbleutil

import (
	"slices"

	"github.com/cockroachdb/pebble"
)

// Prefix allows using go1.23 style iterators with pebble for a given subset of keys
// (based on prefix).
// Seek can be used to start at a specific key.
//
// for k, v := range pebbleutil.Prefix(db, pfx, nil) { ...
func Prefix(db *pebble.DB, pfx, seek []byte) func(yield func(k, v []byte) bool) {
	return func(yield func(k, v []byte) bool) {
		iter := must(PrefixIter(db, pfx))
		defer iter.Close()

		if seek != nil {
			iter.SeekGE(seek)
		} else {
			iter.First()
		}

		for ; iter.Valid(); iter.Next() {
			if !yield(iter.Key(), iter.Value()) {
				return
			}
		}
	}
}

// All will iterate over all entries in the database, optionally seeking at the requested
// location.
//
// for k, v := range pebbleutil.All(db, nil) { ...
func All(db *pebble.DB, seek []byte) func(yield func(k, v []byte) bool) {
	return func(yield func(k, v []byte) bool) {
		iter := must(db.NewIter(nil))
		defer iter.Close()

		if seek != nil {
			iter.SeekGE(seek)
		} else {
			iter.First()
		}

		for ; iter.Valid(); iter.Next() {
			if !yield(iter.Key(), iter.Value()) {
				return
			}
		}
	}
}

// Range will iterate over records in the range [start, end) (that is, end will not be included).
func Range(db *pebble.DB, start, end []byte) func(yield func(k, v []byte) bool) {
	opts := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	return func(yield func(k, v []byte) bool) {
		iter := must(db.NewIter(opts))
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			if !yield(iter.Key(), iter.Value()) {
				return
			}
		}
	}
}

// incrementBytesArray adds 1 to the right-most byte, handling carry
// 123456 becomes 123457
// 1234ff becomes 123500
// ffffff becomes nil
func incrementBytesArray(uppr []byte) []byte {
	uppr = slices.Clone(uppr)
	pos := len(uppr) - 1
	for {
		if uppr[pos] == 0xff {
			if pos == 0 {
				// no upper bound
				return nil
			}
			uppr[pos] = 0
			pos -= 1
			continue
		}
		uppr[pos] += 1
		return uppr
	}
}

// PrefixIter returns a [pebble.Iterator] configured to loop over the specified prefix.
func PrefixIter(db *pebble.DB, pfx []byte) (*pebble.Iterator, error) {
	opts := &pebble.IterOptions{
		LowerBound: pfx,
		UpperBound: incrementBytesArray(pfx),
	}
	return db.NewIter(opts)
}
