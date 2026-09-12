package friend

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// SliceKey represents an immutable sequence of bytes used as a routing key.
// Equivalent to com.databricks.dicer.external.SliceKey in Scala.
type SliceKey struct {
	data        []byte
	bytesPrefix uint64 // First 8 bytes stored as Big-Endian uint64 for fast prefix comparison
}

// MinSliceKey is the smallest possible key ("", empty bytes).
var MinSliceKey = NewSliceKey([]byte{})

// NewSliceKey constructs a SliceKey from raw bytes.
func NewSliceKey(b []byte) SliceKey {
	cp := make([]byte, len(b))
	copy(cp, b)

	var prefix uint64
	l := len(cp)
	for i := range 8 {
		var byteVal uint64
		if i < l {
			byteVal = uint64(cp[i])
		}
		prefix |= byteVal << (8 * (7 - i))
	}

	return SliceKey{
		data:        cp,
		bytesPrefix: prefix,
	}
}

// NewSliceKeyFromString constructs a SliceKey from a UTF-8 string.
func NewSliceKeyFromString(s string) SliceKey {
	return NewSliceKey([]byte(s))
}

// Bytes returns a defensive copy of the raw byte slice.
func (k SliceKey) Bytes() []byte {
	cp := make([]byte, len(k.data))
	copy(cp, k.data)
	return cp
}

// String returns a human-readable representation of the key.
func (k SliceKey) String() string {
	if len(k.data) == 8 {
		return fmt.Sprintf("0x%016x", k.bytesPrefix)
	}
	return string(k.data)
}

// Compare compares two SliceKeys lexicographically.
// Returns:
//
//	-1 if k < other
//	 0 if k == other
//	+1 if k > other
//
// Matches Scala Dicer optimization: compares the 8-byte prefix first. If distinct,
// byte-by-byte traversal is skipped entirely.
func (k SliceKey) Compare(other SliceKey) int {
	if k.bytesPrefix < other.bytesPrefix {
		return -1
	} else if k.bytesPrefix > other.bytesPrefix {
		return 1
	}

	// If prefixes match and both keys are <= 8 bytes, compare by length
	if len(k.data) <= 8 || len(other.data) <= 8 {
		if len(k.data) < len(other.data) {
			return -1
		} else if len(k.data) > len(other.data) {
			return 1
		}
		return 0
	}

	// Both are > 8 bytes with identical 8-byte prefix: compare full byte sequences
	return bytes.Compare(k.data, other.data)
}

// Less returns true if k < other.
func (k SliceKey) Less(other SliceKey) bool {
	return k.Compare(other) < 0
}

// Equal returns true if k == other.
func (k SliceKey) Equal(other SliceKey) bool {
	return k.bytesPrefix == other.bytesPrefix && bytes.Equal(k.data, other.data)
}

// HighSliceKey represents the upper bound of a Slice: either a finite SliceKey or Infinity (+∞).
// Equivalent to com.databricks.dicer.external.HighSliceKey in Scala.
type HighSliceKey struct {
	key        SliceKey
	isInfinity bool
}

// InfinityKey represents positive infinity (+∞).
var InfinityKey = HighSliceKey{isInfinity: true}

// NewHighSliceKey wraps a finite SliceKey as an upper bound.
func NewHighSliceKey(key SliceKey) HighSliceKey {
	return HighSliceKey{key: key, isInfinity: false}
}

// IsInfinity returns whether this upper bound represents positive infinity.
func (h HighSliceKey) IsInfinity() bool {
	return h.isInfinity
}

// Key returns the finite SliceKey and true, or (empty, false) if it is Infinity.
func (h HighSliceKey) Key() (SliceKey, bool) {
	if h.isInfinity {
		return SliceKey{}, false
	}
	return h.key, true
}

// CompareWithKey compares this HighSliceKey with a finite SliceKey.
// Any finite key is strictly less than Infinity (+∞).
func (h HighSliceKey) CompareWithKey(key SliceKey) int {
	if h.isInfinity {
		return 1
	}
	return h.key.Compare(key)
}

// CompareWithHigh compares two HighSliceKeys.
func (h HighSliceKey) CompareWithHigh(other HighSliceKey) int {
	if h.isInfinity && other.isInfinity {
		return 0
	}
	if h.isInfinity {
		return 1
	}
	if other.isInfinity {
		return -1
	}
	return h.key.Compare(other.key)
}

// String returns the string representation.
func (h HighSliceKey) String() string {
	if h.isInfinity {
		return "∞"
	}
	return h.key.String()
}

// Slice represents a half-open range of keys: [LowInclusive .. HighExclusive).
// Equivalent to com.databricks.dicer.external.Slice in Scala.
type Slice struct {
	LowInclusive  SliceKey
	HighExclusive HighSliceKey
}

// NewSlice constructs a new Slice requiring LowInclusive < HighExclusive.
func NewSlice(low SliceKey, high HighSliceKey) (Slice, error) {
	if high.CompareWithKey(low) <= 0 {
		return Slice{}, fmt.Errorf("low key must be strictly less than high key: %s >= %s", low, high)
	}
	return Slice{
		LowInclusive:  low,
		HighExclusive: high,
	}, nil
}

// MustNewSlice constructs a Slice and panics if bounds are invalid.
func MustNewSlice(low SliceKey, high HighSliceKey) Slice {
	s, err := NewSlice(low, high)
	if err != nil {
		panic(err)
	}
	return s
}

// FullSlice represents the whole key space: ["", +∞).
var FullSlice = MustNewSlice(MinSliceKey, InfinityKey)

// Contains returns whether key falls within [LowInclusive, HighExclusive).
func (s Slice) Contains(key SliceKey) bool {
	if key.Compare(s.LowInclusive) < 0 {
		return false
	}
	return s.HighExclusive.CompareWithKey(key) > 0
}

// ContainsSlice returns whether this slice completely covers the other slice.
func (s Slice) ContainsSlice(other Slice) bool {
	if other.LowInclusive.Compare(s.LowInclusive) < 0 {
		return false
	}
	return s.HighExclusive.CompareWithHigh(other.HighExclusive) >= 0
}

// Intersection computes the overlapping Slice between two Slices.
// Returns (intersection, true) if overlapping, or (empty, false) if disjoint.
func (s Slice) Intersection(other Slice) (Slice, bool) {
	// low = max(s.low, other.low)
	low := s.LowInclusive
	if other.LowInclusive.Compare(low) > 0 {
		low = other.LowInclusive
	}

	// high = min(s.high, other.high)
	high := s.HighExclusive
	if other.HighExclusive.CompareWithHigh(high) < 0 {
		high = other.HighExclusive
	}

	if high.CompareWithKey(low) <= 0 {
		return Slice{}, false
	}
	return MustNewSlice(low, high), true
}

// Equal returns true if both bounds are identical.
func (s Slice) Equal(other Slice) bool {
	return s.LowInclusive.Equal(other.LowInclusive) && s.HighExclusive.CompareWithHigh(other.HighExclusive) == 0
}

// String returns the bracketed string representation of the slice.
func (s Slice) String() string {
	return fmt.Sprintf("[%s .. %s)", s.LowInclusive, s.HighExclusive)
}

// Uint64Key builds an 8-byte SliceKey from uint64 (convenient for tests).
func Uint64Key(val uint64) SliceKey {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return NewSliceKey(buf)
}
