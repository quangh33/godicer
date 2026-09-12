package common

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quangh33/godicer/pkg/friend"
	dicerproto "github.com/quangh33/godicer/pkg/proto"
	"google.golang.org/protobuf/proto"
)

func TestProtoConverterRoundTrip(t *testing.T) {
	u1 := uuid.New().String()
	u2 := uuid.New().String()
	now := time.Now().UTC().Truncate(time.Millisecond)

	sq1 := friend.NewSquid("10.0.0.1:8080", now, u1)
	sq2 := friend.NewSquid("10.0.0.2:8080", now.Add(time.Second), u2)

	s1 := friend.MustNewSlice(friend.MinSliceKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString("m")))
	s2 := friend.MustNewSlice(friend.NewSliceKeyFromString("m"), friend.InfinityKey)

	gen := NewGeneration(MustNewIncarnation(10), 200)
	transfer := &Transfer{
		FromResource: sq2,
	}

	swr1, err := NewSliceWithResources(s1, []friend.Squid{sq1})
	if err != nil {
		t.Fatalf("failed to create swr1: %v", err)
	}

	sa1, err := NewSliceAssignment(
		swr1,
		gen,
		map[friend.Squid][]SubsliceAnnotation{
			sq1: {
				NewSubsliceAnnotation(s1, 150, transfer),
			},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("failed to create sa1: %v", err)
	}

	loadVal := 123.45
	swr2, err := NewSliceWithResources(s2, []friend.Squid{sq2})
	if err != nil {
		t.Fatalf("failed to create swr2: %v", err)
	}

	sa2, err := NewSliceAssignment(
		swr2,
		gen,
		nil,
		&loadVal,
	)
	if err != nil {
		t.Fatalf("failed to create sa2: %v", err)
	}

	sliceMap, err := friend.NewSliceMap([]SliceAssignment{sa1, sa2})
	if err != nil {
		t.Fatalf("failed to create slice map: %v", err)
	}

	assignment, err := NewAssignment(
		gen,
		false,
		ConsistencyModeEventual,
		sliceMap,
	)
	if err != nil {
		t.Fatalf("failed to create assignment: %v", err)
	}

	// 1. Convert to Protobuf
	protoMsg := AssignmentToProto(assignment)
	if protoMsg == nil {
		t.Fatal("expected non-nil proto message")
	}

	// 2. Marshal and unmarshal binary
	data, err := proto.Marshal(protoMsg)
	if err != nil {
		t.Fatalf("proto.Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty marshaled bytes")
	}

	protoMsg2 := &dicerproto.DiffAssignmentP{}
	if err := proto.Unmarshal(data, protoMsg2); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}

	// 3. Convert back from Protobuf
	assignmentBack, err := AssignmentFromProto(protoMsg2)
	if err != nil {
		t.Fatalf("AssignmentFromProto failed: %v", err)
	}

	// Verify fields
	if assignmentBack.Generation != assignment.Generation {
		t.Fatalf("expected generation %v, got %v", assignment.Generation, assignmentBack.Generation)
	}
	if assignmentBack.IsFrozen != assignment.IsFrozen {
		t.Fatalf("expected isFrozen %v, got %v", assignment.IsFrozen, assignmentBack.IsFrozen)
	}
	if assignmentBack.SliceMap.Size() != 2 {
		t.Fatalf("expected 2 slice map entries, got %d", assignmentBack.SliceMap.Size())
	}

	// Verify lookups on reconstructed slice map
	e1 := assignmentBack.SliceMap.LookUp(friend.NewSliceKeyFromString("abc"))
	if len(e1.Resources()) != 1 || e1.Resources()[0].ResourceAddress != sq1.ResourceAddress {
		t.Fatalf("expected route to %s, got %v", sq1.ResourceAddress, e1.Resources())
	}
	if len(e1.SubsliceAnnotationsByResource[sq1]) != 1 {
		t.Fatalf("expected subslice annotation for sq1")
	}
	ann := e1.SubsliceAnnotationsByResource[sq1][0]
	if ann.ContinuousGenerationNumber != 150 {
		t.Fatalf("expected continuous generation 150, got %d", ann.ContinuousGenerationNumber)
	}
	if ann.StateTransfer == nil || ann.StateTransfer.FromResource.ResourceAddress != sq2.ResourceAddress {
		t.Fatalf("expected state transfer from %s, got %v", sq2.ResourceAddress, ann.StateTransfer)
	}

	e2 := assignmentBack.SliceMap.LookUp(friend.NewSliceKeyFromString("xyz"))
	if e2.PrimaryRateLoadOpt == nil || *e2.PrimaryRateLoadOpt != 123.45 {
		t.Fatalf("expected primaryRateLoad 123.45, got %v", e2.PrimaryRateLoadOpt)
	}
}
