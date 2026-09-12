package friend

import (
	"fmt"
	"sort"
)

// HasSlice is an interface for any type associated with a Slice.
// Equivalent to trait HasSlice { def slice: Slice } in Scala.
type HasSlice interface {
	GetSlice() Slice
}

// SliceEntry directly wraps a Slice to act as a basic entry.
type SliceEntry struct {
	Slice Slice
}

func (e SliceEntry) GetSlice() Slice {
	return e.Slice
}

// SliceMap manages an ordered, disjoint list of entries that fully partition
// the key space from [MinSliceKey, InfinityKey).
// Equivalent to com.databricks.dicer.friend.SliceMap in Scala.
type SliceMap[T HasSlice] struct {
	entries []T
}

// NewSliceMap constructs a SliceMap and verifies completeness invariants.
func NewSliceMap[T HasSlice](entries []T) (*SliceMap[T], error) {
	if err := ValidateCompleteSlices(entries); err != nil {
		return nil, err
	}
	cp := make([]T, len(entries))
	copy(cp, entries)
	return &SliceMap[T]{entries: cp}, nil
}

// MustNewSliceMap constructs a SliceMap and panics if invariants are violated.
func MustNewSliceMap[T HasSlice](entries []T) *SliceMap[T] {
	sm, err := NewSliceMap(entries)
	if err != nil {
		panic(err)
	}
	return sm
}

// Entries returns a shallow copy of the underlying entries slice.
func (sm *SliceMap[T]) Entries() []T {
	cp := make([]T, len(sm.entries))
	copy(cp, sm.entries)
	return cp
}

// Size returns the total number of slices in this map.
func (sm *SliceMap[T]) Size() int {
	return len(sm.entries)
}

// LookUp finds the entry containing `key`. Guaranteed to succeed in O(log N)
// due to the invariant that entries partition the entire keyspace.
func (sm *SliceMap[T]) LookUp(key SliceKey) T {
	index := FindIndexInOrderedDisjointEntries(sm.entries, key)
	if index < 0 {
		panic(fmt.Sprintf("Key %s not found in complete SliceMap (invariant violation)", key))
	}
	return sm.entries[index]
}

// FindIndexInOrderedDisjointEntries performs binary search O(log N) to locate the slice containing `key`.
// Equivalent to SliceMap.findIndexInOrderedDisjointEntries in Scala.
func FindIndexInOrderedDisjointEntries[T HasSlice](entries []T, key SliceKey) int {
	n := len(entries)
	if n == 0 {
		return -1
	}

	// Binary search with sort.Search:
	// Find the smallest index i such that HighExclusive > key
	idx := sort.Search(n, func(i int) bool {
		return entries[i].GetSlice().HighExclusive.CompareWithKey(key) > 0
	})

	if idx < n {
		s := entries[idx].GetSlice()
		if s.LowInclusive.Compare(key) <= 0 {
			return idx
		}
	}

	return -1
}

// ValidateCompleteSlices checks the required class invariants:
// 1. entries must not be empty.
// 2. The first entry must start at MinSliceKey ("").
// 3. Each subsequent entry must start exactly where the previous entry ended (no gaps, no overlaps).
// 4. The last entry must end at InfinityKey (+∞).
// Equivalent to com.databricks.dicer.friend.SliceMap.validateCompleteSlices in Scala.
func ValidateCompleteSlices[T HasSlice](entries []T) error {
	if len(entries) == 0 {
		return fmt.Errorf("must not be empty")
	}

	firstSlice := entries[0].GetSlice()
	if !firstSlice.LowInclusive.Equal(MinSliceKey) {
		return fmt.Errorf("first entry must start at \"\": actual=%s", firstSlice)
	}

	prevSlice := firstSlice
	for i := 1; i < len(entries); i++ {
		curSlice := entries[i].GetSlice()

		prevHighKey, ok := prevSlice.HighExclusive.Key()
		if !ok {
			return fmt.Errorf("only last entry may have Infinity upper bound: prev=%s", prevSlice)
		}

		boundaryCmp := prevHighKey.Compare(curSlice.LowInclusive)
		if boundaryCmp < 0 {
			return fmt.Errorf("gap between successive entries: %s and %s", prevSlice, curSlice)
		}
		if boundaryCmp > 0 {
			return fmt.Errorf("overlap between successive entries: %s and %s", prevSlice, curSlice)
		}

		prevSlice = curSlice
	}

	if !prevSlice.HighExclusive.IsInfinity() {
		return fmt.Errorf("last entry must have Infinity upper bound: actual=%s", prevSlice)
	}

	return nil
}

// IntersectionEntry represents an entry resulting from intersecting two SliceMaps.
// Equivalent to SliceMap.IntersectionEntry[T, U] in Scala.
type IntersectionEntry[T HasSlice, U HasSlice] struct {
	Slice      Slice
	LeftEntry  T
	RightEntry U
}

func (ie IntersectionEntry[T, U]) GetSlice() Slice {
	return ie.Slice
}

// IntersectSlices calculates the intersection between two complete SliceMaps.
// Uses a 2-pointer scan O(N + M) identical to SliceMap.intersectSlices in Scala.
func IntersectSlices[T HasSlice, U HasSlice](
	left *SliceMap[T],
	right *SliceMap[U],
) *SliceMap[IntersectionEntry[T, U]] {
	leftEntries := left.entries
	rightEntries := right.entries

	var result []IntersectionEntry[T, U]

	leftIt := 0
	rightIt := 0

	for leftIt < len(leftEntries) && rightIt < len(rightEntries) {
		leftEntry := leftEntries[leftIt]
		rightEntry := rightEntries[rightIt]

		leftSlice := leftEntry.GetSlice()
		rightSlice := rightEntry.GetSlice()

		intersection, ok := leftSlice.Intersection(rightSlice)
		if !ok {
			panic(fmt.Sprintf("Slices must cover full space and intersect: left=%s, right=%s", leftSlice, rightSlice))
		}

		result = append(result, IntersectionEntry[T, U]{
			Slice:      intersection,
			LeftEntry:  leftEntry,
			RightEntry: rightEntry,
		})

		// Advance cursors if the upper bounds match the intersection upper bound
		if leftSlice.HighExclusive.CompareWithHigh(intersection.HighExclusive) == 0 {
			leftIt++
		}
		if rightSlice.HighExclusive.CompareWithHigh(intersection.HighExclusive) == 0 {
			rightIt++
		}
	}

	return MustNewSliceMap(result)
}

// CoalesceSlices returns a new SliceMap where adjacent entries with equal values are coalesced (merged).
// Equivalent to SliceMap.coalesceSlices in Scala.
//
// Example:
//
//	original:  [A .. B) -> Dest1, [B .. C) -> Dest1, [C .. D) -> Dest2
//	result:    [A .. C) -> Dest1, [C .. D) -> Dest2
func CoalesceSlices[T HasSlice](
	sliceMap *SliceMap[T],
	equalVal func(left, right T) bool,
	withSlice func(entry T, newSlice Slice) T,
) *SliceMap[T] {
	oldEntries := sliceMap.entries
	if len(oldEntries) <= 1 {
		return sliceMap
	}

	var newEntries []T
	currentEntry := oldEntries[0]

	for i := 1; i < len(oldEntries); i++ {
		nextEntry := oldEntries[i]
		if equalVal(currentEntry, nextEntry) {
			// Merge current and next into a single combined slice
			combinedSlice := MustNewSlice(
				currentEntry.GetSlice().LowInclusive,
				nextEntry.GetSlice().HighExclusive,
			)
			currentEntry = withSlice(currentEntry, combinedSlice)
		} else {
			newEntries = append(newEntries, currentEntry)
			currentEntry = nextEntry
		}
	}
	newEntries = append(newEntries, currentEntry)

	return MustNewSliceMap(newEntries)
}

// GapEntry represents an entry in a partition that is either defined (Some) or represents a gap (Gap).
// Equivalent to sealed trait GapEntry[+T] in Scala.
type GapEntry[T any] struct {
	Slice   Slice
	Value   T
	IsValue bool // true if Value is present, false if this represents an unassigned Gap
}

func (ge GapEntry[T]) GetSlice() Slice {
	return ge.Slice
}

// CreateFromOrderedDisjointEntries takes ordered disjoint entries that do NOT necessarily cover
// the entire key space, fills in any gaps with unassigned Gap entries, and produces a complete SliceMap.
// Equivalent to SliceMap.createFromOrderedDisjointEntries in Scala.
func CreateFromOrderedDisjointEntries[T HasSlice](
	entries []T,
) *SliceMap[GapEntry[T]] {
	var result []GapEntry[T]
	var cursor HighSliceKey = NewHighSliceKey(MinSliceKey)

	for _, entry := range entries {
		s := entry.GetSlice()

		// If there is a gap between the cursor and entry's low bound, fill with a Gap
		if cursorKey, ok := cursor.Key(); ok {
			if s.LowInclusive.Compare(cursorKey) > 0 {
				gapSlice := MustNewSlice(cursorKey, NewHighSliceKey(s.LowInclusive))
				result = append(result, GapEntry[T]{
					Slice:   gapSlice,
					IsValue: false,
				})
			}
		}

		// Append the actual entry
		result = append(result, GapEntry[T]{
			Slice:   s,
			Value:   entry,
			IsValue: true,
		})
		cursor = s.HighExclusive
	}

	// If there is a trailing gap up to Infinity (+∞), fill it
	if cursorKey, ok := cursor.Key(); ok {
		trailingGap := MustNewSlice(cursorKey, InfinityKey)
		result = append(result, GapEntry[T]{
			Slice:   trailingGap,
			IsValue: false,
		})
	}

	return MustNewSliceMap(result)
}
