package common_test

import (
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quangh33/godicer/pkg/common"
	"github.com/quangh33/godicer/pkg/friend"
)

func makeTestSlice(low, high string) friend.Slice {
	lowKey := friend.NewSliceKeyFromString(low)
	if high == "" || high == "∞" {
		return friend.MustNewSlice(lowKey, friend.InfinityKey)
	}
	return friend.MustNewSlice(lowKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString(high)))
}

func makeTestSquid(id string) friend.Squid {
	return friend.NewSquid("10.0.0.1:50051", time.UnixMilli(1000), id)
}

// --------------------------------------------------------------------------------------
// 1. Port of SliceAssignmentSuite.scala & slice_assignment_test_data.textproto
// --------------------------------------------------------------------------------------

func TestCheckAssignmentGeneration(t *testing.T) {
	pod0 := makeTestSquid("Pod0")
	swr := common.MustNewSliceWithResources(makeTestSlice("Nori", "Ori"), []friend.Squid{pod0})
	inc20 := common.MustNewIncarnation(20)
	gen20_42 := common.NewGeneration(inc20, 42)

	sa := common.MustNewSliceAssignment(swr, gen20_42, nil, nil)

	// 1. Valid: same incarnation with higher or equal generation number
	if err := sa.CheckAssignmentGeneration(gen20_42); err != nil {
		t.Fatalf("Expected valid, got error: %v", err)
	}
	if err := sa.CheckAssignmentGeneration(common.NewGeneration(inc20, 43)); err != nil {
		t.Fatalf("Expected valid, got error: %v", err)
	}
	if err := sa.CheckAssignmentGeneration(common.NewGeneration(inc20, 10000)); err != nil {
		t.Fatalf("Expected valid, got error: %v", err)
	}

	// 2. Invalid: assignment generation is less than slice generation
	errLess := sa.CheckAssignmentGeneration(common.NewGeneration(inc20, 41))
	if errLess == nil || !strings.Contains(errLess.Error(), "less than or equal to") {
		t.Fatalf("Expected 'less than or equal to' error, got: %v", errLess)
	}

	// 3. Invalid: assignment generation has different incarnation
	inc21 := common.MustNewIncarnation(21)
	errDiffInc := sa.CheckAssignmentGeneration(common.NewGeneration(inc21, 42))
	if errDiffInc == nil || !strings.Contains(errDiffInc.Error(), "same incarnation") {
		t.Fatalf("Expected 'same incarnation' error, got: %v", errDiffInc)
	}
}

func TestSliceAssignmentInvariants(t *testing.T) {
	pod0 := makeTestSquid("Pod0")
	pod1 := makeTestSquid("Pod1")
	sliceNoriOri := makeTestSlice("Nori", "Ori")
	gen := common.NewGeneration(common.MustNewIncarnation(42), 1)

	// Case 1: Empty assigned resources
	_, errEmptyRes := common.NewSliceWithResources(sliceNoriOri, []friend.Squid{})
	if errEmptyRes == nil || !strings.Contains(errEmptyRes.Error(), "must be non-empty") {
		t.Fatalf("Expected 'must be non-empty', got: %v", errEmptyRes)
	}

	swr := common.MustNewSliceWithResources(sliceNoriOri, []friend.Squid{pod0})

	// Case 2: Negative primary rate load
	negLoad := -1.0
	_, errNegLoad := common.NewSliceAssignment(swr, gen, nil, &negLoad)
	if errNegLoad == nil || !strings.Contains(errNegLoad.Error(), "non-negative") {
		t.Fatalf("Expected 'non-negative' error, got: %v", errNegLoad)
	}

	// Case 3: NaN primary rate load
	nanLoad := math.NaN()
	_, errNanLoad := common.NewSliceAssignment(swr, gen, nil, &nanLoad)
	if errNanLoad == nil || !strings.Contains(errNanLoad.Error(), "non-negative finite") {
		t.Fatalf("Expected finite error for NaN, got: %v", errNanLoad)
	}

	// Case 4: Inf primary rate load
	infLoad := math.Inf(1)
	_, errInfLoad := common.NewSliceAssignment(swr, gen, nil, &infLoad)
	if errInfLoad == nil || !strings.Contains(errInfLoad.Error(), "non-negative finite") {
		t.Fatalf("Expected finite error for Inf, got: %v", errInfLoad)
	}

	// Case 5: Subslice annotation with unassigned resource (pod1 not in swr)
	annotUnassigned := map[friend.Squid][]common.SubsliceAnnotation{
		pod1: {common.NewSubsliceAnnotation(sliceNoriOri, 1, nil)},
	}
	_, errUnassigned := common.NewSliceAssignment(swr, gen, annotUnassigned, nil)
	if errUnassigned == nil || !strings.Contains(errUnassigned.Error(), "resources that own the slice") {
		t.Fatalf("Expected unassigned resource error, got: %v", errUnassigned)
	}

	// Case 6: Subslice annotation with generation newer than slice assignment (2 > 1)
	annotNewerGen := map[friend.Squid][]common.SubsliceAnnotation{
		pod0: {common.NewSubsliceAnnotation(sliceNoriOri, 2, nil)},
	}
	_, errNewerGen := common.NewSliceAssignment(swr, gen, annotNewerGen, nil)
	if errNewerGen == nil || !strings.Contains(errNewerGen.Error(), "cannot have newer generation") {
		t.Fatalf("Expected newer generation error, got: %v", errNewerGen)
	}

	// Case 7: Overlapping subslice annotations
	annotOverlap := map[friend.Squid][]common.SubsliceAnnotation{
		pod0: {
			common.NewSubsliceAnnotation(sliceNoriOri, 1, nil),
			common.NewSubsliceAnnotation(sliceNoriOri, 1, nil),
		},
	}
	_, errOverlap := common.NewSliceAssignment(swr, gen, annotOverlap, nil)
	if errOverlap == nil || !strings.Contains(errOverlap.Error(), "ordered and disjoint") {
		t.Fatalf("Expected 'ordered and disjoint' error, got: %v", errOverlap)
	}

	// Case 8: State transfer from same resource
	annotSameTransfer := map[friend.Squid][]common.SubsliceAnnotation{
		pod0: {
			common.NewSubsliceAnnotation(sliceNoriOri, 1, &common.Transfer{FromResource: pod0}),
		},
	}
	_, errSameTransfer := common.NewSliceAssignment(swr, gen, annotSameTransfer, nil)
	if errSameTransfer == nil || !strings.Contains(errSameTransfer.Error(), "cannot be from and to the same resource") {
		t.Fatalf("Expected same resource transfer error, got: %v", errSameTransfer)
	}
}

// --------------------------------------------------------------------------------------
// 2. High-Concurrency Stress Test for AtomicAssignmentCell
// --------------------------------------------------------------------------------------

func TestAtomicAssignmentCellConcurrency(t *testing.T) {
	podA := makeTestSquid("PodA")
	podB := makeTestSquid("PodB")
	sliceFull := friend.FullSlice
	inc := common.MustNewIncarnation(1)

	makeAssignment := func(genNum uint64, pod friend.Squid) *common.Assignment {
		gen := common.NewGeneration(inc, genNum)
		swr := common.MustNewSliceWithResources(sliceFull, []friend.Squid{pod})
		sa := common.MustNewSliceAssignment(swr, gen, nil, nil)
		sm := friend.MustNewSliceMap([]common.SliceAssignment{sa})
		return common.MustNewAssignment(gen, false, common.ConsistencyModeEventual, sm)
	}

	initial := makeAssignment(1, podA)
	cell := common.NewAtomicAssignmentCell(initial)

	var listenerCallCount atomic.Int64
	cell.AddListener(func(newAssignment *common.Assignment) {
		listenerCallCount.Add(1)
	})

	const numReaders = 50
	const numReadsPerGoroutine = 50000
	const numWriterUpdates = 100

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})

	// Launch 50 concurrent reader goroutines simulating high-QPS lookups
	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(rand.Int63()))
			for i := range numReadsPerGoroutine {
				select {
				case <-stopReaders:
					return
				default:
				}
				asg := cell.Get()
				if asg == nil {
					t.Errorf("Unexpected nil assignment during read")
					return
				}
				// Verify lookup invariant
				key := friend.Uint64Key(rng.Uint64())
				sa := asg.LookUp(key)
				if len(sa.Resources()) == 0 {
					t.Errorf("Empty resources in lookup at read %d", i)
					return
				}
			}
		}()
	}

	// Launch 1 writer goroutine simulating Assigner pushing version updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		for g := uint64(2); g <= numWriterUpdates+1; g++ {
			var chosenPod friend.Squid
			if g%2 == 0 {
				chosenPod = podB
			} else {
				chosenPod = podA
			}
			newAsg := makeAssignment(g, chosenPod)
			updated := cell.Set(newAsg)
			if !updated {
				t.Errorf("Failed to update generation %d", g)
			}
			// Attempt to update with an older generation (must be rejected)
			staleAsg := makeAssignment(g-1, chosenPod)
			if cell.Set(staleAsg) {
				t.Errorf("Stale assignment was accepted incorrectly")
			}
		}
		close(stopReaders)
	}()

	wg.Wait()

	// Verify final state
	finalAsg := cell.Get()
	if finalAsg.Generation.Number != numWriterUpdates+1 {
		t.Fatalf("Expected final generation %d, got %d", numWriterUpdates+1, finalAsg.Generation.Number)
	}

	if listenerCallCount.Load() != numWriterUpdates {
		t.Fatalf("Expected %d listener calls, got %d", numWriterUpdates, listenerCallCount.Load())
	}
}

func TestAssignmentHelperMethods(t *testing.T) {
	pod1 := makeTestSquid("Pod1")
	pod2 := makeTestSquid("Pod2")

	s1 := makeTestSlice("", "m")
	s2 := makeTestSlice("m", "∞")

	swr1 := common.MustNewSliceWithResources(s1, []friend.Squid{pod1})
	swr2 := common.MustNewSliceWithResources(s2, []friend.Squid{pod2})

	gen := common.NewGeneration(common.MustNewIncarnation(1), 10)
	sa1 := common.MustNewSliceAssignment(swr1, gen, nil, nil)
	sa2 := common.MustNewSliceAssignment(swr2, gen, nil, nil)

	sm, err := friend.NewSliceMap([]common.SliceAssignment{sa1, sa2})
	if err != nil {
		t.Fatalf("NewSliceMap failed: %v", err)
	}

	asg := common.MustNewAssignment(gen, false, common.ConsistencyModeEventual, sm)

	// Test Resources()
	resources := asg.Resources()
	if len(resources) != 2 {
		t.Fatalf("expected 2 unique resources, got %d", len(resources))
	}

	// Test IsAssignedKey()
	appleKey := friend.NewSliceKeyFromString("apple")
	zebraKey := friend.NewSliceKeyFromString("zebra")

	if !asg.IsAssignedKey(appleKey, pod1) {
		t.Fatal("expected 'apple' to be assigned to pod1")
	}
	if asg.IsAssignedKey(appleKey, pod2) {
		t.Fatal("expected 'apple' NOT to be assigned to pod2")
	}
	if !asg.IsAssignedKey(zebraKey, pod2) {
		t.Fatal("expected 'zebra' to be assigned to pod2")
	}

	// Test GetAssignedSliceAssignments()
	pod1SAs := asg.GetAssignedSliceAssignments(pod1)
	if len(pod1SAs) != 1 || !pod1SAs[0].Slice().Equal(s1) {
		t.Fatalf("expected pod1 assigned to s1, got %v", pod1SAs)
	}

	// Test GetAssignedSlices()
	pod2Slices := asg.GetAssignedSlices(pod2)
	if len(pod2Slices) != 1 || !pod2Slices[0].Equal(s2) {
		t.Fatalf("expected pod2 assigned to s2, got %v", pod2Slices)
	}
}
