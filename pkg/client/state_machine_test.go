package client

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quangh33/godicer/pkg/common"
	"github.com/quangh33/godicer/pkg/friend"
	dicerproto "github.com/quangh33/godicer/pkg/proto"
)

func createTestAssignment(inc uint64, num uint64, address string) *common.Assignment {
	sq := friend.NewSquid(address, time.Now().UTC(), uuid.New().String())
	s := friend.FullSlice
	swr, _ := common.NewSliceWithResources(s, []friend.Squid{sq})
	gen := common.NewGeneration(common.MustNewIncarnation(inc), num)
	sa, _ := common.NewSliceAssignment(swr, gen, nil, nil)
	sm, _ := friend.NewSliceMap([]common.SliceAssignment{sa})
	a, _ := common.NewAssignment(gen, false, common.ConsistencyModeEventual, sm)
	return a
}

func TestStateMachineInitialSendRequest(t *testing.T) {
	cfg := DefaultStateMachineConfig()
	sm := NewAssignmentSyncStateMachine(cfg, 42)

	now := time.Now()
	actions := sm.OnAdvance(now)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action on initial advance, got %d", len(actions))
	}

	req, ok := actions[0].(ActionSendRequest)
	if !ok {
		t.Fatalf("expected ActionSendRequest, got %T", actions[0])
	}
	if req.OpID != 1 {
		t.Fatalf("expected opID 1, got %d", req.OpID)
	}
	if !req.Redirect.IsEmpty() {
		t.Fatalf("expected empty redirect initially")
	}
}

func TestStateMachineDeadlineExceededAndBackoff(t *testing.T) {
	cfg := DefaultStateMachineConfig()
	cfg.WatchRPCTimeout = 10 * time.Second
	cfg.MinRetryDelay = 100 * time.Millisecond
	cfg.MaxRetryDelay = 1 * time.Second
	sm := NewAssignmentSyncStateMachine(cfg, 42)

	now := time.Now()
	actions := sm.OnAdvance(now)
	if len(actions) != 1 {
		t.Fatalf("expected initial action, got %d", len(actions))
	}
	firstReq := actions[0].(ActionSendRequest)

	// Inject response with redirect
	redirectAddr := "10.0.0.99:50051"
	respWithRedirect := &dicerproto.ClientResponseP{
		SuggestedRpcTimeoutMillis: 10000,
		Redirect: &dicerproto.RedirectP{
			Address:       redirectAddr,
			RedirectToken: []byte{1, 2, 3, 4},
		},
	}

	actions2 := sm.OnEvent(now, EventReadSuccess{
		Address:  nil,
		OpID:     firstReq.OpID,
		Response: respWithRedirect,
	})

	if len(actions2) != 1 {
		t.Fatalf("expected 1 action after read success, got %d", len(actions2))
	}
	redirectReq := actions2[0].(ActionSendRequest)
	if redirectReq.Redirect.IsEmpty() || *redirectReq.Redirect.Address != redirectAddr {
		t.Fatalf("expected redirect to %s, got %v", redirectAddr, redirectReq.Redirect.Address)
	}

	// Advance clock slightly: before deadline, should NOT retry
	now = now.Add(9 * time.Second)
	noActions := sm.OnAdvance(now)
	if len(noActions) != 0 {
		t.Fatalf("expected no actions before deadline, got %d", len(noActions))
	}
	if sm.IsInBackoff() {
		t.Fatal("expected not in backoff before deadline")
	}

	// Advance clock past deadline: should enter backoff
	now = now.Add(2 * time.Second) // total 11s > 10s
	_ = sm.OnAdvance(now)
	if !sm.IsInBackoff() {
		t.Fatal("expected to enter backoff after deadline")
	}

	// Advance clock past backoff delay: should emit fallback request to default address with redirect cleared
	now = now.Add(cfg.MinRetryDelay * 2)
	retryActions := sm.OnAdvance(now)
	if len(retryActions) != 1 {
		t.Fatalf("expected retry action, got %d", len(retryActions))
	}
	fallbackReq := retryActions[0].(ActionSendRequest)
	if !fallbackReq.Redirect.IsEmpty() {
		t.Fatalf("expected fallback redirect to be empty, got %v", fallbackReq.Redirect)
	}
}

func TestStateMachineIncorporateNewAssignment(t *testing.T) {
	cfg := DefaultStateMachineConfig()
	sm := NewAssignmentSyncStateMachine(cfg, 42)

	now := time.Now()
	actions := sm.OnAdvance(now)
	req := actions[0].(ActionSendRequest)

	// Send successful response with new assignment (gen 1#100)
	asn1 := createTestAssignment(1, 100, "pod1:50051")
	resp1 := &dicerproto.ClientResponseP{
		SyncAssignmentState: &dicerproto.SyncAssignmentStateP{
			State: &dicerproto.SyncAssignmentStateP_KnownAssignment{
				KnownAssignment: common.AssignmentToProto(asn1),
			},
		},
		SuggestedRpcTimeoutMillis: 30000,
	}

	actions1 := sm.OnEvent(now, EventReadSuccess{
		OpID:     req.OpID,
		Response: resp1,
	})

	var usedAsn *common.Assignment
	for _, a := range actions1 {
		if u, ok := a.(ActionUseAssignment); ok {
			usedAsn = u.Assignment
		}
	}
	if usedAsn == nil {
		t.Fatal("expected ActionUseAssignment to be emitted")
	}
	if usedAsn.Generation != asn1.Generation {
		t.Fatalf("expected gen %s, got %s", asn1.Generation, usedAsn.Generation)
	}

	// Now send older assignment (gen 1#50): should NOT emit ActionUseAssignment
	asnOlder := createTestAssignment(1, 50, "pod1:50051")
	respOlder := &dicerproto.ClientResponseP{
		SyncAssignmentState: &dicerproto.SyncAssignmentStateP{
			State: &dicerproto.SyncAssignmentStateP_KnownAssignment{
				KnownAssignment: common.AssignmentToProto(asnOlder),
			},
		},
	}

	actionsOlder := sm.OnEvent(now, EventReadSuccess{
		OpID:     9999, // old op
		Response: respOlder,
	})
	for _, a := range actionsOlder {
		if _, ok := a.(ActionUseAssignment); ok {
			t.Fatal("unexpected ActionUseAssignment for older generation")
		}
	}
}

func TestStateMachineCancel(t *testing.T) {
	cfg := DefaultStateMachineConfig()
	sm := NewAssignmentSyncStateMachine(cfg, 42)

	now := time.Now()
	sm.OnAdvance(now)

	sm.OnEvent(now, EventCancel{})
	if !sm.IsCancelled() {
		t.Fatal("expected state machine to be cancelled")
	}

	actions := sm.OnAdvance(now.Add(time.Hour))
	if len(actions) != 0 {
		t.Fatalf("expected 0 actions after cancellation, got %d", len(actions))
	}
}
