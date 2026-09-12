package friend_test

import (
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/quangh33/godicer/pkg/friend"
)

// Helper to generate a random complete slice partition covering ["", ∞).
// Equivalent to createCompleteSlices in Scala's TestSliceUtils.
func createRandomCompleteSlices(size int, rng *rand.Rand) []friend.SliceEntry {
	if size == 1 {
		return []friend.SliceEntry{{Slice: friend.FullSlice}}
	}

	// Generate (size - 1) random cut points
	cutValues := make([]uint64, size-1)
	used := make(map[uint64]bool)
	for i := range size - 1 {
		for {
			v := rng.Uint64()
			if v > 0 && !used[v] {
				used[v] = true
				cutValues[i] = v
				break
			}
		}
	}
	sort.Slice(cutValues, func(i, j int) bool { return cutValues[i] < cutValues[j] })

	entries := make([]friend.SliceEntry, size)
	curLow := friend.MinSliceKey

	for i := range size - 1 {
		highKey := friend.Uint64Key(cutValues[i])
		entries[i] = friend.SliceEntry{
			Slice: friend.MustNewSlice(curLow, friend.NewHighSliceKey(highKey)),
		}
		curLow = highKey
	}

	// The last slice extends to Infinity (+∞)
	entries[size-1] = friend.SliceEntry{
		Slice: friend.MustNewSlice(curLow, friend.InfinityKey),
	}

	return entries
}

// --------------------------------------------------------------------------------------
// 1. Exact Port of "lookUp" tests from SliceMapSuite.scala
// --------------------------------------------------------------------------------------

func TestLookUp(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	// 100 trials matching Scala test suite
	for range 100 {
		size := rng.Intn(100) + 1
		entries := createRandomCompleteSlices(size, rng)
		sliceMap, err := friend.NewSliceMap(entries)
		if err != nil {
			t.Fatalf("Failed to create SliceMap: %v", err)
		}

		// Verify all boundary values
		for _, entry := range sliceMap.Entries() {
			lookedUp := sliceMap.LookUp(entry.Slice.LowInclusive)
			if !lookedUp.Slice.Equal(entry.Slice) {
				t.Fatalf("Boundary lookup failed: expected %s, got %s", entry.Slice, lookedUp.Slice)
			}
		}

		// Verify 1000 random keys per trial
		for range 1000 {
			key := friend.Uint64Key(rng.Uint64())
			actual := sliceMap.LookUp(key)

			if !actual.Slice.Contains(key) {
				t.Fatalf("Looked up slice %s does not contain key %s", actual.Slice, key)
			}
		}
	}
}

// --------------------------------------------------------------------------------------
// 2. Exact Port of "validate_complete_slices_test_cases" from slice_map_test_data.textproto
// --------------------------------------------------------------------------------------

func TestValidateCompleteSlicesGoldenCases(t *testing.T) {
	s := func(low string, high string) friend.SliceEntry {
		lowKey := friend.NewSliceKeyFromString(low)
		if high == "" || high == "∞" {
			return friend.SliceEntry{Slice: friend.MustNewSlice(lowKey, friend.InfinityKey)}
		}
		return friend.SliceEntry{Slice: friend.MustNewSlice(lowKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString(high)))}
	}

	testCases := []struct {
		name          string
		slices        []friend.SliceEntry
		expectedError string
	}{
		{
			name:          "Empty list",
			slices:        []friend.SliceEntry{},
			expectedError: "must not be empty",
		},
		{
			name: "Does not start at \"\"",
			slices: []friend.SliceEntry{
				s("Balin", "Fili"),
			},
			expectedError: "first entry must start at \"\"",
		},
		{
			name: "Gap between successive entries",
			slices: []friend.SliceEntry{
				s("", "Balin"),
				s("Fili", "∞"),
			},
			expectedError: "gap between successive entries",
		},
		{
			name: "Overlap between successive entries",
			slices: []friend.SliceEntry{
				s("", "Fili"),
				s("Balin", "∞"),
			},
			expectedError: "overlap between successive entries",
		},
		{
			name: "Last entry must have Infinity upper bound",
			slices: []friend.SliceEntry{
				s("", "Balin"),
			},
			expectedError: "last entry must have Infinity upper bound",
		},
		{
			name: "Only last entry may have Infinity upper bound",
			slices: []friend.SliceEntry{
				s("", "∞"),
				s("Fili", "∞"),
			},
			expectedError: "only last entry may have Infinity upper bound",
		},
		{
			name: "Valid complete slices",
			slices: []friend.SliceEntry{
				s("", "Balin"),
				s("Balin", "Fili"),
				s("Fili", "∞"),
			},
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := friend.ValidateCompleteSlices(tc.slices)
			if tc.expectedError == "" {
				if err != nil {
					t.Fatalf("Expected valid, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Fatalf("Expected error containing '%s', got nil", tc.expectedError)
				}
				if !strings.Contains(err.Error(), tc.expectedError) {
					t.Fatalf("Expected error containing '%s', got '%v'", tc.expectedError, err)
				}
			}
		})
	}
}

// --------------------------------------------------------------------------------------
// 3. Exact Port of "intersect_test_cases" from slice_map_test_data.textproto
// --------------------------------------------------------------------------------------

type ResourceEntry struct {
	Slice    friend.Slice
	Resource string
}

func (re ResourceEntry) GetSlice() friend.Slice {
	return re.Slice
}

type ValEntry struct {
	Slice friend.Slice
	Val   int
}

func (ve ValEntry) GetSlice() friend.Slice {
	return ve.Slice
}

func TestIntersectSlicesGoldenCases(t *testing.T) {
	s := func(low string, high string) friend.Slice {
		lowKey := friend.NewSliceKeyFromString(low)
		if high == "" || high == "∞" {
			return friend.MustNewSlice(lowKey, friend.InfinityKey)
		}
		return friend.MustNewSlice(lowKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString(high)))
	}

	// Case 1: Aligned slices
	t.Run("Aligned", func(t *testing.T) {
		left := friend.MustNewSliceMap([]ResourceEntry{
			{Slice: s("", "10"), Resource: "resource1"},
			{Slice: s("10", "20"), Resource: "resource2"},
			{Slice: s("20", "∞"), Resource: "resource1"},
		})
		right := friend.MustNewSliceMap([]ValEntry{
			{Slice: s("", "10"), Val: 0},
			{Slice: s("10", "20"), Val: 1},
			{Slice: s("20", "∞"), Val: 2},
		})

		intersected := friend.IntersectSlices(left, right)
		entries := intersected.Entries()
		if len(entries) != 3 {
			t.Fatalf("Expected 3 entries, got %d", len(entries))
		}
		if entries[0].LeftEntry.Resource != "resource1" || entries[0].RightEntry.Val != 0 {
			t.Errorf("Mismatch at 0: %+v", entries[0])
		}
		if entries[1].LeftEntry.Resource != "resource2" || entries[1].RightEntry.Val != 1 {
			t.Errorf("Mismatch at 1: %+v", entries[1])
		}
		if entries[2].LeftEntry.Resource != "resource1" || entries[2].RightEntry.Val != 2 {
			t.Errorf("Mismatch at 2: %+v", entries[2])
		}
	})

	// Case 2: Slices merged from left to right (first 2 on left merged on right)
	t.Run("Merged", func(t *testing.T) {
		left := friend.MustNewSliceMap([]ResourceEntry{
			{Slice: s("", "10"), Resource: "resource1"},
			{Slice: s("10", "20"), Resource: "resource2"},
			{Slice: s("20", "∞"), Resource: "resource1"},
		})
		right := friend.MustNewSliceMap([]ValEntry{
			{Slice: s("", "20"), Val: 0},
			{Slice: s("20", "∞"), Val: 1},
		})

		intersected := friend.IntersectSlices(left, right)
		entries := intersected.Entries()
		if len(entries) != 3 {
			t.Fatalf("Expected 3 entries, got %d", len(entries))
		}
		// [ "" .. 10 ): left 0, right 0
		if !entries[0].Slice.Equal(s("", "10")) || entries[0].LeftEntry.Resource != "resource1" || entries[0].RightEntry.Val != 0 {
			t.Errorf("Mismatch at 0: %+v", entries[0])
		}
		// [ 10 .. 20 ): left 1, right 0
		if !entries[1].Slice.Equal(s("10", "20")) || entries[1].LeftEntry.Resource != "resource2" || entries[1].RightEntry.Val != 0 {
			t.Errorf("Mismatch at 1: %+v", entries[1])
		}
		// [ 20 .. ∞ ): left 2, right 1
		if !entries[2].Slice.Equal(s("20", "∞")) || entries[2].LeftEntry.Resource != "resource1" || entries[2].RightEntry.Val != 1 {
			t.Errorf("Mismatch at 2: %+v", entries[2])
		}
	})

	// Case 3: Slices split from left to right (first slice on left split into 2 on right)
	t.Run("Split", func(t *testing.T) {
		left := friend.MustNewSliceMap([]ResourceEntry{
			{Slice: s("", "20"), Resource: "resource1"},
			{Slice: s("20", "∞"), Resource: "resource2"},
		})
		right := friend.MustNewSliceMap([]ValEntry{
			{Slice: s("", "10"), Val: 0},
			{Slice: s("10", "20"), Val: 1},
			{Slice: s("20", "∞"), Val: 2},
		})

		intersected := friend.IntersectSlices(left, right)
		entries := intersected.Entries()
		if len(entries) != 3 {
			t.Fatalf("Expected 3 entries, got %d", len(entries))
		}
		// [ "" .. 10 ): left 0, right 0
		if !entries[0].Slice.Equal(s("", "10")) || entries[0].LeftEntry.Resource != "resource1" || entries[0].RightEntry.Val != 0 {
			t.Errorf("Mismatch at 0: %+v", entries[0])
		}
		// [ 10 .. 20 ): left 0, right 1
		if !entries[1].Slice.Equal(s("10", "20")) || entries[1].LeftEntry.Resource != "resource1" || entries[1].RightEntry.Val != 1 {
			t.Errorf("Mismatch at 1: %+v", entries[1])
		}
		// [ 20 .. ∞ ): left 1, right 2
		if !entries[2].Slice.Equal(s("20", "∞")) || entries[2].LeftEntry.Resource != "resource2" || entries[2].RightEntry.Val != 2 {
			t.Errorf("Mismatch at 2: %+v", entries[2])
		}
	})

	// Case 4: Misaligned boundaries in left and right (from Dicer docs diagram)
	t.Run("Misaligned", func(t *testing.T) {
		left := friend.MustNewSliceMap([]ResourceEntry{
			{Slice: s("", "20"), Resource: "resource1"},
			{Slice: s("20", "30"), Resource: "resource2"},
			{Slice: s("30", "40"), Resource: "resource1"},
			{Slice: s("40", "∞"), Resource: "resource4"},
		})
		right := friend.MustNewSliceMap([]ValEntry{
			{Slice: s("", "10"), Val: 0},
			{Slice: s("10", "50"), Val: 1},
			{Slice: s("50", "60"), Val: 2},
			{Slice: s("60", "∞"), Val: 3},
		})

		intersected := friend.IntersectSlices(left, right)
		entries := intersected.Entries()
		if len(entries) != 7 {
			t.Fatalf("Expected 7 entries, got %d", len(entries))
		}

		expectedSlices := []struct {
			s        friend.Slice
			leftRes  string
			rightVal int
		}{
			{s("", "10"), "resource1", 0},
			{s("10", "20"), "resource1", 1},
			{s("20", "30"), "resource2", 1},
			{s("30", "40"), "resource1", 1},
			{s("40", "50"), "resource4", 1},
			{s("50", "60"), "resource4", 2},
			{s("60", "∞"), "resource4", 3},
		}

		for i, exp := range expectedSlices {
			if !entries[i].Slice.Equal(exp.s) || entries[i].LeftEntry.Resource != exp.leftRes || entries[i].RightEntry.Val != exp.rightVal {
				t.Errorf("Mismatch at %d: expected %+v, got %+v", i, exp, entries[i])
			}
		}
	})
}

// --------------------------------------------------------------------------------------
// 4. Exact Port of "partial_slice_map_test_cases" from slice_map_test_data.textproto
// --------------------------------------------------------------------------------------

func TestPartialSliceMapGoldenCases(t *testing.T) {
	s := func(low string, high string) friend.SliceEntry {
		lowKey := friend.NewSliceKeyFromString(low)
		if high == "" || high == "∞" {
			return friend.SliceEntry{Slice: friend.MustNewSlice(lowKey, friend.InfinityKey)}
		}
		return friend.SliceEntry{Slice: friend.MustNewSlice(lowKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString(high)))}
	}

	testCases := []struct {
		name     string
		input    []friend.SliceEntry
		expected []struct {
			s       friend.Slice
			isValue bool
		}
	}{
		{
			name:  "Zero input entries -> single GAP",
			input: []friend.SliceEntry{},
			expected: []struct {
				s       friend.Slice
				isValue bool
			}{
				{s("", "∞").Slice, false},
			},
		},
		{
			name:  "One entry at start [\"\" .. a) -> trailing GAP",
			input: []friend.SliceEntry{s("", "a")},
			expected: []struct {
				s       friend.Slice
				isValue bool
			}{
				{s("", "a").Slice, true},
				{s("a", "∞").Slice, false},
			},
		},
		{
			name:  "One entry at end [b .. ∞) -> leading GAP",
			input: []friend.SliceEntry{s("b", "∞")},
			expected: []struct {
				s       friend.Slice
				isValue bool
			}{
				{s("", "b").Slice, false},
				{s("b", "∞").Slice, true},
			},
		},
		{
			name:  "One entry in middle [a .. b) -> leading GAP + trailing GAP",
			input: []friend.SliceEntry{s("a", "b")},
			expected: []struct {
				s       friend.Slice
				isValue bool
			}{
				{s("", "a").Slice, false},
				{s("a", "b").Slice, true},
				{s("b", "∞").Slice, false},
			},
		},
		{
			name: "Gap at start and middle: [b .. c), [d .. ∞)",
			input: []friend.SliceEntry{
				s("b", "c"),
				s("d", "∞"),
			},
			expected: []struct {
				s       friend.Slice
				isValue bool
			}{
				{s("", "b").Slice, false},
				{s("b", "c").Slice, true},
				{s("c", "d").Slice, false},
				{s("d", "∞").Slice, true},
			},
		},
		{
			name: "Covering entire key space: [\"\" .. a), [a .. b), [b .. c), [c .. d), [d .. ∞)",
			input: []friend.SliceEntry{
				s("", "a"),
				s("a", "b"),
				s("b", "c"),
				s("c", "d"),
				s("d", "∞"),
			},
			expected: []struct {
				s       friend.Slice
				isValue bool
			}{
				{s("", "a").Slice, true},
				{s("a", "b").Slice, true},
				{s("b", "c").Slice, true},
				{s("c", "d").Slice, true},
				{s("d", "∞").Slice, true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fullMap := friend.CreateFromOrderedDisjointEntries(tc.input)
			entries := fullMap.Entries()

			if len(entries) != len(tc.expected) {
				t.Fatalf("Expected %d entries, got %d", len(tc.expected), len(entries))
			}

			for i, exp := range tc.expected {
				if !entries[i].Slice.Equal(exp.s) {
					t.Errorf("Index %d slice mismatch: expected %s, got %s", i, exp.s, entries[i].Slice)
				}
				if entries[i].IsValue != exp.isValue {
					t.Errorf("Index %d isValue mismatch: expected %v, got %v", i, exp.isValue, entries[i].IsValue)
				}
			}
		})
	}
}

// --------------------------------------------------------------------------------------
// 5. Exact Port of "SliceMap coalesce" golden cases from SliceMapSuite.scala
// --------------------------------------------------------------------------------------

func TestCoalesceSlicesGoldenCases(t *testing.T) {
	s := func(low string, high string) friend.Slice {
		lowKey := friend.NewSliceKeyFromString(low)
		if high == "" || high == "∞" {
			return friend.MustNewSlice(lowKey, friend.InfinityKey)
		}
		return friend.MustNewSlice(lowKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString(high)))
	}

	// Scala test: 7 slices with resource sets, coalescing adjacent ones
	// Input:
	//   "" - 10 : r0
	//   10 - 20 : r0
	//   20 - 30 : r1
	//   30 - 40 : r2
	//   40 - 50 : r2
	//   50 - 60 : r0,r1
	//   60 - ∞  : r0,r1
	// Expected:
	//   "" - 20 : r0
	//   20 - 30 : r1
	//   30 - 50 : r2
	//   50 - ∞  : r0,r1
	inputMap := friend.MustNewSliceMap([]ResourceEntry{
		{Slice: s("", "10"), Resource: "r0"},
		{Slice: s("10", "20"), Resource: "r0"},
		{Slice: s("20", "30"), Resource: "r1"},
		{Slice: s("30", "40"), Resource: "r2"},
		{Slice: s("40", "50"), Resource: "r2"},
		{Slice: s("50", "60"), Resource: "r0,r1"},
		{Slice: s("60", "∞"), Resource: "r0,r1"},
	})

	coalesced := friend.CoalesceSlices(
		inputMap,
		func(l, r ResourceEntry) bool { return l.Resource == r.Resource },
		func(e ResourceEntry, newSlice friend.Slice) ResourceEntry {
			e.Slice = newSlice
			return e
		},
	)

	entries := coalesced.Entries()
	if len(entries) != 4 {
		t.Fatalf("Expected 4 coalesced entries, got %d", len(entries))
	}

	expected := []struct {
		s   friend.Slice
		res string
	}{
		{s("", "20"), "r0"},
		{s("20", "30"), "r1"},
		{s("30", "50"), "r2"},
		{s("50", "∞"), "r0,r1"},
	}

	for i, exp := range expected {
		if !entries[i].Slice.Equal(exp.s) || entries[i].Resource != exp.res {
			t.Errorf("Mismatch at %d: expected %+v, got %+v", i, exp, entries[i])
		}
	}
}
