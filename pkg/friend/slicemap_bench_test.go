package friend_test

import (
	"math/rand"
	"testing"

	"github.com/quangh33/godicer/pkg/friend"
)

// BenchmarkLookUp measures in-memory binary search lookup speed.
func BenchmarkLookUp(b *testing.B) {
	rng := rand.New(rand.NewSource(12345))
	// Simulate a large cluster with 500 Slices partitioned in memory
	entries := createRandomCompleteSlices(500, rng)
	sliceMap, err := friend.NewSliceMap(entries)
	if err != nil {
		b.Fatal(err)
	}

	keys := make([]friend.SliceKey, 1000)
	for i := range 1000 {
		keys[i] = friend.Uint64Key(rng.Uint64())
	}

	b.ResetTimer()
	for i := range b.N {
		key := keys[i%1000]
		_ = sliceMap.LookUp(key)
	}
}
