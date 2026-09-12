package common

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/quangh33/godicer/pkg/friend"
	dicerproto "github.com/quangh33/godicer/pkg/proto"
)

// GenerationToProto converts a Generation to its protobuf representation.
func GenerationToProto(g Generation) *dicerproto.GenerationP {
	return &dicerproto.GenerationP{
		Incarnation: int64(g.Incarnation.Value()),
		Number:      int64(g.Number),
	}
}

// GenerationFromProto converts a protobuf GenerationP to a Generation.
func GenerationFromProto(p *dicerproto.GenerationP) Generation {
	if p == nil {
		return EmptyGeneration
	}
	inc := MustNewIncarnation(uint64(p.Incarnation))
	return NewGeneration(inc, uint64(p.Number))
}

// SliceToProto converts a friend.Slice to its protobuf representation.
func SliceToProto(s friend.Slice) *dicerproto.SliceP {
	sp := &dicerproto.SliceP{
		LowInclusive:   s.LowInclusive.Bytes(),
		IsHighInfinity: s.HighExclusive.IsInfinity(),
	}
	if key, ok := s.HighExclusive.Key(); ok {
		sp.HighExclusive = key.Bytes()
	}
	return sp
}

// SliceFromProto converts a protobuf SliceP to a friend.Slice.
func SliceFromProto(p *dicerproto.SliceP) (friend.Slice, error) {
	if p == nil {
		return friend.Slice{}, fmt.Errorf("slice proto cannot be nil")
	}
	low := friend.NewSliceKey(p.LowInclusive)
	var high friend.HighSliceKey
	if p.IsHighInfinity || p.HighExclusive == nil {
		high = friend.InfinityKey
	} else {
		high = friend.NewHighSliceKey(friend.NewSliceKey(p.HighExclusive))
	}
	return friend.NewSlice(low, high)
}

// SquidToProto converts a friend.Squid to its protobuf representation.
func SquidToProto(sq friend.Squid) *dicerproto.SquidP {
	parsedUUID, err := uuid.Parse(sq.ResourceUUID)
	var high, low int64
	if err == nil {
		high, low = uuidToInt64s(parsedUUID)
	}
	return &dicerproto.SquidP{
		ResourceAddress:    sq.ResourceAddress,
		CreationTimeMillis: sq.CreationTimeMillis,
		ResourceUuidHigh:   high,
		ResourceUuidLow:    low,
	}
}

// SquidFromProto converts a protobuf SquidP to a friend.Squid.
func SquidFromProto(p *dicerproto.SquidP) (friend.Squid, error) {
	if p == nil {
		return friend.Squid{}, fmt.Errorf("squid proto cannot be nil")
	}
	u := int64sToUUID(p.ResourceUuidHigh, p.ResourceUuidLow)
	return friend.NewSquid(p.ResourceAddress, time.UnixMilli(p.CreationTimeMillis).UTC(), u.String()), nil
}

func uuidToInt64s(u uuid.UUID) (int64, int64) {
	var high, low int64
	for i := range 8 {
		high = (high << 8) | int64(u[i])
		low = (low << 8) | int64(u[8+i])
	}
	return high, low
}

func int64sToUUID(high, low int64) uuid.UUID {
	var u uuid.UUID
	for i := 7; i >= 0; i-- {
		u[i] = byte(high & 0xff)
		high >>= 8
		u[8+i] = byte(low & 0xff)
		low >>= 8
	}
	return u
}

// AssignmentToProto serializes a full Assignment to DiffAssignmentP.
func AssignmentToProto(a *Assignment) *dicerproto.DiffAssignmentP {
	if a == nil {
		return nil
	}

	// Build unique resource pool
	resourceIndexMap := make(map[friend.Squid]int32)
	var resources []*dicerproto.SquidP

	getResourceIndex := func(sq friend.Squid) int32 {
		if idx, found := resourceIndexMap[sq]; found {
			return idx
		}
		idx := int32(len(resources))
		resourceIndexMap[sq] = idx
		resources = append(resources, SquidToProto(sq))
		return idx
	}

	var sliceAssignments []*dicerproto.SliceAssignmentP
	if a.SliceMap != nil {
		for _, sa := range a.SliceMap.Entries() {
			var resourceIndices []int32
			for _, sq := range sa.SliceWithResources.Resources {
				resourceIndices = append(resourceIndices, getResourceIndex(sq))
			}

			var annotationsProto []*dicerproto.SubsliceAnnotationP
			for sq, annList := range sa.SubsliceAnnotationsByResource {
				resIdx := getResourceIndex(sq)
				for _, ann := range annList {
					ap := &dicerproto.SubsliceAnnotationP{
						ResourceId:       resIdx,
						Subslice:         SliceToProto(ann.Subslice),
						GenerationNumber: int64(ann.ContinuousGenerationNumber),
					}
					if ann.StateTransfer != nil {
						fromIdx := getResourceIndex(ann.StateTransfer.FromResource)
						ap.StateTransfer = &dicerproto.TransferP{
							Id:             1,
							FromResourceId: fromIdx,
						}
					}
					annotationsProto = append(annotationsProto, ap)
				}
			}

			sap := &dicerproto.SliceAssignmentP{
				Slice:               SliceToProto(sa.SliceWithResources.Slice),
				Generation:          GenerationToProto(sa.Generation),
				ResourceIndices:     resourceIndices,
				SubsliceAnnotations: annotationsProto,
				PrimaryRateLoad:     sa.PrimaryRateLoadOpt,
			}
			sliceAssignments = append(sliceAssignments, sap)
		}
	}

	return &dicerproto.DiffAssignmentP{
		Generation:       GenerationToProto(a.Generation),
		SliceAssignments: sliceAssignments,
		Resources:        resources,
		IsFrozen:         a.IsFrozen,
	}
}

// AssignmentFromProto deserializes a DiffAssignmentP to an Assignment.
func AssignmentFromProto(p *dicerproto.DiffAssignmentP) (*Assignment, error) {
	if p == nil {
		return nil, fmt.Errorf("diff assignment proto cannot be nil")
	}

	gen := GenerationFromProto(p.Generation)
	resources := make([]friend.Squid, len(p.Resources))
	for i, rp := range p.Resources {
		sq, err := SquidFromProto(rp)
		if err != nil {
			return nil, err
		}
		resources[i] = sq
	}

	sliceAssignments := make([]SliceAssignment, 0, len(p.SliceAssignments))
	for _, sap := range p.SliceAssignments {
		slice, err := SliceFromProto(sap.Slice)
		if err != nil {
			return nil, err
		}
		sliceGen := GenerationFromProto(sap.Generation)

		var assignedResources []friend.Squid
		for _, idx := range sap.ResourceIndices {
			if int(idx) >= len(resources) {
				return nil, fmt.Errorf("resource index %d out of bounds (len %d)", idx, len(resources))
			}
			assignedResources = append(assignedResources, resources[idx])
		}
		swr, err := NewSliceWithResources(slice, assignedResources)
		if err != nil {
			return nil, err
		}

		annotationsByResource := make(map[friend.Squid][]SubsliceAnnotation)
		for _, ap := range sap.SubsliceAnnotations {
			if int(ap.ResourceId) >= len(resources) {
				return nil, fmt.Errorf("annotation resource index %d out of bounds", ap.ResourceId)
			}
			sq := resources[ap.ResourceId]
			subslice, err := SliceFromProto(ap.Subslice)
			if err != nil {
				return nil, err
			}
			var transfer *Transfer
			if ap.StateTransfer != nil {
				if int(ap.StateTransfer.FromResourceId) >= len(resources) {
					return nil, fmt.Errorf("state transfer from_resource index %d out of bounds", ap.StateTransfer.FromResourceId)
				}
				transfer = &Transfer{
					FromResource: resources[ap.StateTransfer.FromResourceId],
				}
			}
			ann := NewSubsliceAnnotation(subslice, uint64(ap.GenerationNumber), transfer)
			annotationsByResource[sq] = append(annotationsByResource[sq], ann)
		}

		sa, err := NewSliceAssignment(swr, sliceGen, annotationsByResource, sap.PrimaryRateLoad)
		if err != nil {
			return nil, err
		}
		sliceAssignments = append(sliceAssignments, sa)
	}

	sliceMap, err := friend.NewSliceMap(sliceAssignments)
	if err != nil {
		return nil, fmt.Errorf("failed to construct SliceMap: %w", err)
	}

	return NewAssignment(gen, p.IsFrozen, ConsistencyModeEventual, sliceMap)
}
