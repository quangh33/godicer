package common

import (
	"fmt"
	"math"

	"github.com/quangh33/godicer/pkg/friend"
)

// ConsistencyMode defines assignment consistency requirements.
// Equivalent to com.databricks.dicer.common.AssignmentConsistencyMode in Scala.
type ConsistencyMode int

const (
	ConsistencyModeEventual ConsistencyMode = iota
	ConsistencyModeStrict
)

func (cm ConsistencyMode) String() string {
	switch cm {
	case ConsistencyModeEventual:
		return "EVENTUAL"
	case ConsistencyModeStrict:
		return "STRICT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", cm)
	}
}

// SliceWithResources associates a Slice with a non-empty set of assigned resources (Squids).
// Ported directly from case class SliceWithResources in SliceAssignment.scala.
type SliceWithResources struct {
	Slice     friend.Slice
	Resources []friend.Squid // Set of assigned server pod incarnations (Squids)
}

// NewSliceWithResources constructs and validates SliceWithResources.
func NewSliceWithResources(slice friend.Slice, resources []friend.Squid) (SliceWithResources, error) {
	if len(resources) == 0 {
		return SliceWithResources{}, fmt.Errorf("the assigned resources must be non-empty")
	}
	cp := make([]friend.Squid, len(resources))
	copy(cp, resources)
	return SliceWithResources{
		Slice:     slice,
		Resources: cp,
	}, nil
}

// MustNewSliceWithResources constructs SliceWithResources or panics.
func MustNewSliceWithResources(slice friend.Slice, resources []friend.Squid) SliceWithResources {
	swr, err := NewSliceWithResources(slice, resources)
	if err != nil {
		panic(err)
	}
	return swr
}

// SliceAssignment represents the complete assignment of a Slice to a set of resources,
// including its localized generation, continuous subslice annotations, and measured load.
// Ported directly from case class SliceAssignment in SliceAssignment.scala.
type SliceAssignment struct {
	SliceWithResources            SliceWithResources
	Generation                    Generation
	SubsliceAnnotationsByResource map[friend.Squid][]SubsliceAnnotation // Resource -> disjoint, ordered annotations
	PrimaryRateLoadOpt            *float64                              // Optional measured QPS / throughput
}

// GetSlice implements friend.HasSlice so it can be placed directly in friend.SliceMap.
func (sa SliceAssignment) GetSlice() friend.Slice {
	return sa.SliceWithResources.Slice
}

// Slice returns the assigned Slice.
func (sa SliceAssignment) Slice() friend.Slice {
	return sa.SliceWithResources.Slice
}

// Resources returns a copy of the assigned resource addresses/Squids.
func (sa SliceAssignment) Resources() []friend.Squid {
	cp := make([]friend.Squid, len(sa.SliceWithResources.Resources))
	copy(cp, sa.SliceWithResources.Resources)
	return cp
}

// CheckAssignmentGeneration verifies requirements in the context of an assignment generation.
// Equivalent to SliceAssignment.checkAssignmentGeneration in Scala.
func (sa SliceAssignment) CheckAssignmentGeneration(assignmentGen Generation) error {
	if sa.Generation.Incarnation.Compare(assignmentGen.Incarnation) != 0 {
		return fmt.Errorf("slice generation %s must be in the same incarnation as assignment generation %s",
			sa.Generation, assignmentGen)
	}
	if sa.Generation.Compare(assignmentGen) > 0 {
		return fmt.Errorf("slice generation %s must be less than or equal to assignment generation %s",
			sa.Generation, assignmentGen)
	}
	return nil
}

// NewSliceAssignment constructs and fully validates a SliceAssignment matching Scala's validate().
func NewSliceAssignment(
	swr SliceWithResources,
	gen Generation,
	annotations map[friend.Squid][]SubsliceAnnotation,
	primaryRateLoadOpt *float64,
) (SliceAssignment, error) {
	if gen.IsEmpty() {
		return SliceAssignment{}, fmt.Errorf("slice assignment must have non-empty generation")
	}

	// Defensive copy of annotations map
	cpAnnotations := make(map[friend.Squid][]SubsliceAnnotation, len(annotations))
	resourceSet := make(map[friend.Squid]bool, len(swr.Resources))
	for _, r := range swr.Resources {
		resourceSet[r] = true
	}

	// Validate annotations invariants matching Scala validate()
	for resource, subsliceList := range annotations {
		if !resourceSet[resource] {
			return SliceAssignment{}, fmt.Errorf("subslice annotations must be to one of the resources that own the slice: expected %v, got %s",
				swr.Resources, resource)
		}
		if len(subsliceList) == 0 {
			return SliceAssignment{}, fmt.Errorf("subslice annotations for resource %s should not be empty", resource)
		}

		var prevHigh = friend.NewHighSliceKey(swr.Slice.LowInclusive)
		for _, annotation := range subsliceList {
			if annotation.ContinuousGenerationNumber > gen.Number {
				return SliceAssignment{}, fmt.Errorf("subslice annotation cannot have newer generation: %d > %d",
					annotation.ContinuousGenerationNumber, gen.Number)
			}

			// Subslice must be ordered, disjoint and >= prevHigh
			if prevHighKey, ok := prevHigh.Key(); ok {
				if annotation.Subslice.LowInclusive.Compare(prevHighKey) < 0 {
					return SliceAssignment{}, fmt.Errorf("subslice annotations must be ordered and disjoint: %s is not >= %s",
						annotation.Subslice.LowInclusive, prevHighKey)
				}
			}

			if annotation.StateTransfer != nil && annotation.StateTransfer.FromResource.Equal(resource) {
				return SliceAssignment{}, fmt.Errorf("state transfer cannot be from and to the same resource %s", resource)
			}

			prevHigh = annotation.Subslice.HighExclusive
		}

		// All subslices must fit within the parent slice
		if prevHigh.CompareWithHigh(swr.Slice.HighExclusive) > 0 {
			return SliceAssignment{}, fmt.Errorf("subslice annotations must be contained in slice: %s is not <= %s",
				prevHigh, swr.Slice.HighExclusive)
		}

		cpList := make([]SubsliceAnnotation, len(subsliceList))
		copy(cpList, subsliceList)
		cpAnnotations[resource] = cpList
	}

	// Validate primaryRateLoadOpt
	var loadCopy *float64
	if primaryRateLoadOpt != nil {
		val := *primaryRateLoadOpt
		if math.IsNaN(val) || math.IsInf(val, 0) || val < 0 {
			return SliceAssignment{}, fmt.Errorf("load measurement must be a non-negative finite number: got %f", val)
		}
		loadCopy = &val
	}

	return SliceAssignment{
		SliceWithResources:            swr,
		Generation:                    gen,
		SubsliceAnnotationsByResource: cpAnnotations,
		PrimaryRateLoadOpt:            loadCopy,
	}, nil
}

// MustNewSliceAssignment constructs SliceAssignment or panics.
func MustNewSliceAssignment(
	swr SliceWithResources,
	gen Generation,
	annotations map[friend.Squid][]SubsliceAnnotation,
	primaryRateLoadOpt *float64,
) SliceAssignment {
	sa, err := NewSliceAssignment(swr, gen, annotations, primaryRateLoadOpt)
	if err != nil {
		panic(err)
	}
	return sa
}

// Assignment represents the complete, global partitioning of the key space at a specific Generation.
// Equivalent to com.databricks.dicer.common.Assignment in Scala.
type Assignment struct {
	Generation      Generation
	IsFrozen        bool
	ConsistencyMode ConsistencyMode
	SliceMap        *friend.SliceMap[SliceAssignment]
}

// NewAssignment constructs and validates an Assignment.
func NewAssignment(
	gen Generation,
	frozen bool,
	mode ConsistencyMode,
	sliceMap *friend.SliceMap[SliceAssignment],
) (*Assignment, error) {
	if gen.IsEmpty() {
		return nil, fmt.Errorf("assignment must have non-empty generation")
	}
	if sliceMap == nil {
		return nil, fmt.Errorf("sliceMap must not be nil")
	}

	// Check each sliceAssignment's generation against assignment generation
	for _, sa := range sliceMap.Entries() {
		if err := sa.CheckAssignmentGeneration(gen); err != nil {
			return nil, err
		}
	}

	return &Assignment{
		Generation:      gen,
		IsFrozen:        frozen,
		ConsistencyMode: mode,
		SliceMap:        sliceMap,
	}, nil
}

// MustNewAssignment creates an Assignment or panics.
func MustNewAssignment(
	gen Generation,
	frozen bool,
	mode ConsistencyMode,
	sliceMap *friend.SliceMap[SliceAssignment],
) *Assignment {
	a, err := NewAssignment(gen, frozen, mode, sliceMap)
	if err != nil {
		panic(err)
	}
	return a
}

// Size returns the total number of slice partitions.
func (a *Assignment) Size() int {
	return a.SliceMap.Size()
}

// LookUp finds the SliceAssignment containing key.
func (a *Assignment) LookUp(key friend.SliceKey) SliceAssignment {
	return a.SliceMap.LookUp(key)
}

// SliceAssignments returns all slice assignments in slice order.
func (a *Assignment) SliceAssignments() []SliceAssignment {
	return a.SliceMap.Entries()
}

// Resources returns a deduplicated list of all server pod incarnations (Squids) assigned across all slices.
func (a *Assignment) Resources() []friend.Squid {
	seen := make(map[friend.Squid]struct{})
	var result []friend.Squid
	for _, sa := range a.SliceMap.Entries() {
		for _, sq := range sa.Resources() {
			if _, exists := seen[sq]; !exists {
				seen[sq] = struct{}{}
				result = append(result, sq)
			}
		}
	}
	return result
}

// IsAssignedKey returns whether the given key is assigned to the specified resource incarnation.
func (a *Assignment) IsAssignedKey(key friend.SliceKey, resource friend.Squid) bool {
	sa := a.LookUp(key)
	for _, sq := range sa.Resources() {
		if sq.Equal(resource) {
			return true
		}
	}
	return false
}

// GetAssignedSliceAssignments returns all SliceAssignments assigned to the specified resource incarnation.
func (a *Assignment) GetAssignedSliceAssignments(resource friend.Squid) []SliceAssignment {
	var result []SliceAssignment
	for _, sa := range a.SliceMap.Entries() {
		for _, sq := range sa.Resources() {
			if sq.Equal(resource) {
				result = append(result, sa)
				break
			}
		}
	}
	return result
}

// GetAssignedSlices returns all Slices assigned to the specified resource incarnation.
func (a *Assignment) GetAssignedSlices(resource friend.Squid) []friend.Slice {
	var result []friend.Slice
	for _, sa := range a.SliceMap.Entries() {
		for _, sq := range sa.Resources() {
			if sq.Equal(resource) {
				result = append(result, sa.Slice())
				break
			}
		}
	}
	return result
}
