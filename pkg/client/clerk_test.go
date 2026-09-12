package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quangh33/godicer/pkg/common"
	"github.com/quangh33/godicer/pkg/friend"
	dicerproto "github.com/quangh33/godicer/pkg/proto"
	"google.golang.org/grpc"
)

type mockAssignmentServer struct {
	dicerproto.UnimplementedAssignmentServiceServer

	mu              sync.Mutex
	lastKnownGen    common.Generation
	assignmentQueue []*common.Assignment
}

func (s *mockAssignmentServer) pushAssignment(a *common.Assignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assignmentQueue = append(s.assignmentQueue, a)
}

func (s *mockAssignmentServer) Watch(ctx context.Context, req *dicerproto.ClientRequestP) (*dicerproto.ClientResponseP, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var resp *dicerproto.ClientResponseP
	if len(s.assignmentQueue) > 0 {
		next := s.assignmentQueue[0]
		s.assignmentQueue = s.assignmentQueue[1:]
		s.lastKnownGen = next.Generation

		resp = &dicerproto.ClientResponseP{
			SyncAssignmentState: &dicerproto.SyncAssignmentStateP{
				State: &dicerproto.SyncAssignmentStateP_KnownAssignment{
					KnownAssignment: common.AssignmentToProto(next),
				},
			},
			SuggestedRpcTimeoutMillis: 5000,
		}
	} else {
		resp = &dicerproto.ClientResponseP{
			SyncAssignmentState: &dicerproto.SyncAssignmentStateP{
				State: &dicerproto.SyncAssignmentStateP_KnownGeneration{
					KnownGeneration: common.GenerationToProto(s.lastKnownGen),
				},
			},
			SuggestedRpcTimeoutMillis: 5000,
		}
	}
	return resp, nil
}

func TestClerkRoutingAndLiveUpdates(t *testing.T) {
	// Setup in-process gRPC test server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	mockServer := &mockAssignmentServer{}
	dicerproto.RegisterAssignmentServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	serverAddr := listener.Addr().String()

	// Initial assignment: ["" .. "m") -> Pod1, ["m" .. +∞) -> Pod2
	sq1 := friend.NewSquid("10.0.0.1:50051", time.Now().UTC(), uuid.New().String())
	sq2 := friend.NewSquid("10.0.0.2:50051", time.Now().UTC(), uuid.New().String())

	s1 := friend.MustNewSlice(friend.MinSliceKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString("m")))
	s2 := friend.MustNewSlice(friend.NewSliceKeyFromString("m"), friend.InfinityKey)

	swr1, _ := common.NewSliceWithResources(s1, []friend.Squid{sq1})
	swr2, _ := common.NewSliceWithResources(s2, []friend.Squid{sq2})

	gen1 := common.NewGeneration(common.MustNewIncarnation(1), 100)
	sa1, _ := common.NewSliceAssignment(swr1, gen1, nil, nil)
	sa2, _ := common.NewSliceAssignment(swr2, gen1, nil, nil)

	sm1, _ := friend.NewSliceMap([]common.SliceAssignment{sa1, sa2})
	asn1, _ := common.NewAssignment(gen1, false, common.ConsistencyModeEventual, sm1)

	mockServer.pushAssignment(asn1)

	cfg := DefaultStateMachineConfig()
	cfg.DefaultWatchAddress = serverAddr
	cfg.MinRetryDelay = 20 * time.Millisecond
	cfg.WatchRPCTimeout = 2 * time.Second

	clerk := NewClerk("test-target", "test-client", cfg)
	defer clerk.Close()

	// Wait for clerk to receive initial assignment
	var currentAsn *common.Assignment
	for i := range 50 {
		_ = i
		currentAsn = clerk.Assignment()
		if currentAsn != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if currentAsn == nil {
		t.Fatal("clerk failed to receive initial assignment in time")
	}

	// 1. Verify routing
	dest1, err := clerk.Route("apple")
	if err != nil {
		t.Fatalf("routing 'apple' failed: %v", err)
	}
	if dest1.ResourceAddress != sq1.ResourceAddress {
		t.Fatalf("expected 'apple' to route to %s, got %s", sq1.ResourceAddress, dest1.ResourceAddress)
	}

	dest2, err := clerk.Route("zebra")
	if err != nil {
		t.Fatalf("routing 'zebra' failed: %v", err)
	}
	if dest2.ResourceAddress != sq2.ResourceAddress {
		t.Fatalf("expected 'zebra' to route to %s, got %s", sq2.ResourceAddress, dest2.ResourceAddress)
	}

	// 2. Push updated assignment (Gen 1#200): Split s1 into ["" .. "e") -> Pod1, ["e" .. "m") -> Pod3
	sq3 := friend.NewSquid("10.0.0.3:50051", time.Now().UTC(), uuid.New().String())
	s1a := friend.MustNewSlice(friend.MinSliceKey, friend.NewHighSliceKey(friend.NewSliceKeyFromString("e")))
	s1b := friend.MustNewSlice(friend.NewSliceKeyFromString("e"), friend.NewHighSliceKey(friend.NewSliceKeyFromString("m")))

	swr1a, _ := common.NewSliceWithResources(s1a, []friend.Squid{sq1})
	swr1b, _ := common.NewSliceWithResources(s1b, []friend.Squid{sq3})

	gen2 := common.NewGeneration(common.MustNewIncarnation(1), 200)
	sa1a, _ := common.NewSliceAssignment(swr1a, gen2, nil, nil)
	sa1b, _ := common.NewSliceAssignment(swr1b, gen2, nil, nil)
	sa2b, _ := common.NewSliceAssignment(swr2, gen2, nil, nil)

	sm2, _ := friend.NewSliceMap([]common.SliceAssignment{sa1a, sa1b, sa2b})
	asn2, _ := common.NewAssignment(gen2, false, common.ConsistencyModeEventual, sm2)

	mockServer.pushAssignment(asn2)

	// Wait for clerk to update to gen2
	for i := range 50 {
		_ = i
		if clerk.Assignment().Generation.Equal(gen2) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !clerk.Assignment().Generation.Equal(gen2) {
		t.Fatalf("clerk did not update to gen2: %s", clerk.Assignment().Generation)
	}

	// Now "goat" (between "e" and "m") should route to Pod3!
	dest3, err := clerk.Route("goat")
	if err != nil {
		t.Fatalf("routing 'goat' failed: %v", err)
	}
	if dest3.ResourceAddress != sq3.ResourceAddress {
		t.Fatalf("expected 'goat' to route to %s, got %s", sq3.ResourceAddress, dest3.ResourceAddress)
	}
}

func TestResourceRouterMultiReplicaRoundRobin(t *testing.T) {
	sqA := friend.NewSquid("10.0.0.10:50051", time.Now().UTC(), uuid.New().String())
	sqB := friend.NewSquid("10.0.0.20:50051", time.Now().UTC(), uuid.New().String())

	s := friend.FullSlice
	swr, _ := common.NewSliceWithResources(s, []friend.Squid{sqA, sqB})
	gen := common.NewGeneration(common.MustNewIncarnation(1), 100)
	sa, _ := common.NewSliceAssignment(swr, gen, nil, nil)
	sm, _ := friend.NewSliceMap([]common.SliceAssignment{sa})
	asn, _ := common.NewAssignment(gen, false, common.ConsistencyModeEventual, sm)

	cell := common.NewAtomicAssignmentCell(asn)
	router := NewResourceRouter(cell)

	all, err := router.RouteAll("any_key")
	if err != nil {
		t.Fatalf("RouteAll failed: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(all))
	}

	// Consecutive calls to Route() should alternate between sqA and sqB
	r1, _ := router.Route("any_key")
	r2, _ := router.Route("any_key")
	r3, _ := router.Route("any_key")

	if r1.ResourceAddress == r2.ResourceAddress {
		t.Fatalf("expected alternating replicas: r1=%s, r2=%s", r1.ResourceAddress, r2.ResourceAddress)
	}
	if r1.ResourceAddress != r3.ResourceAddress {
		t.Fatalf("expected round robin cycle: r1=%s, r3=%s", r1.ResourceAddress, r3.ResourceAddress)
	}
}

func TestClerkNoAssignmentError(t *testing.T) {
	cell := common.NewAtomicAssignmentCell(nil)
	router := NewResourceRouter(cell)

	_, err := router.Route("test")
	if err == nil || err != ErrNoAssignmentAvailable {
		t.Fatalf("expected ErrNoAssignmentAvailable, got %v", err)
	}
}

func TestRouteTwoLevel(t *testing.T) {
	sqA := friend.NewSquid("10.0.0.1:50051", time.Now().UTC(), uuid.New().String())
	sqB := friend.NewSquid("10.0.0.2:50051", time.Now().UTC(), uuid.New().String())
	sqC := friend.NewSquid("10.0.0.3:50051", time.Now().UTC(), uuid.New().String())

	s := friend.FullSlice
	swr, _ := common.NewSliceWithResources(s, []friend.Squid{sqA, sqB, sqC})
	gen := common.NewGeneration(common.MustNewIncarnation(1), 100)
	sa, _ := common.NewSliceAssignment(swr, gen, nil, nil)
	sm, _ := friend.NewSliceMap([]common.SliceAssignment{sa})
	asn, _ := common.NewAssignment(gen, false, common.ConsistencyModeEventual, sm)

	cell := common.NewAtomicAssignmentCell(asn)
	router := NewResourceRouter(cell)

	// Same primaryKey and secondaryKey must always resolve to the exact same replica
	dest1, err := router.RouteTwoLevel("user_partition_1", "session_abc")
	if err != nil {
		t.Fatalf("RouteTwoLevel failed: %v", err)
	}
	dest2, _ := router.RouteTwoLevel("user_partition_1", "session_abc")
	if dest1.ResourceAddress != dest2.ResourceAddress {
		t.Fatalf("expected deterministic replica for same secondaryKey: %s != %s", dest1.ResourceAddress, dest2.ResourceAddress)
	}
}

func TestRouteWithRetry(t *testing.T) {
	sq1 := friend.NewSquid("10.0.0.1:50051", time.Now().UTC(), uuid.New().String())
	sq2 := friend.NewSquid("10.0.0.2:50051", time.Now().UTC(), uuid.New().String())

	s := friend.FullSlice
	swr, _ := common.NewSliceWithResources(s, []friend.Squid{sq1, sq2})
	gen := common.NewGeneration(common.MustNewIncarnation(1), 100)
	sa, _ := common.NewSliceAssignment(swr, gen, nil, nil)
	sm, _ := friend.NewSliceMap([]common.SliceAssignment{sa})
	asn, _ := common.NewAssignment(gen, false, common.ConsistencyModeEventual, sm)

	cell := common.NewAtomicAssignmentCell(asn)
	router := NewResourceRouter(cell)

	// Step 1: No tried replicas -> should return sq1
	first, err := router.RouteWithRetry("key", nil)
	if err != nil {
		t.Fatalf("RouteWithRetry first failed: %v", err)
	}
	if first.ResourceAddress != sq1.ResourceAddress {
		t.Fatalf("expected first to be sq1, got %s", first.ResourceAddress)
	}

	// Step 2: Pass sq1 as tried -> should pick sq2
	second, err := router.RouteWithRetry("key", []friend.Squid{sq1})
	if err != nil {
		t.Fatalf("RouteWithRetry second failed: %v", err)
	}
	if second.ResourceAddress != sq2.ResourceAddress {
		t.Fatalf("expected second to be sq2, got %s", second.ResourceAddress)
	}

	// Step 3: Pass both sq1 and sq2 as tried -> should return ErrAllReplicasExhausted
	_, err = router.RouteWithRetry("key", []friend.Squid{sq1, sq2})
	if err == nil || !errors.Is(err, ErrAllReplicasExhausted) {
		t.Fatalf("expected ErrAllReplicasExhausted, got %v", err)
	}
}

func TestClerkWaitReady(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	mockServer := &mockAssignmentServer{}
	dicerproto.RegisterAssignmentServiceServer(grpcServer, mockServer)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	sq := friend.NewSquid("10.0.0.1:50051", time.Now().UTC(), uuid.New().String())
	swr, _ := common.NewSliceWithResources(friend.FullSlice, []friend.Squid{sq})
	gen := common.NewGeneration(common.MustNewIncarnation(1), 10)
	sa, _ := common.NewSliceAssignment(swr, gen, nil, nil)
	sm, _ := friend.NewSliceMap([]common.SliceAssignment{sa})
	asn, _ := common.NewAssignment(gen, false, common.ConsistencyModeEventual, sm)

	mockServer.pushAssignment(asn)

	cfg := DefaultStateMachineConfig()
	cfg.DefaultWatchAddress = listener.Addr().String()
	cfg.MinRetryDelay = 10 * time.Millisecond

	clerk := NewClerk("target", "client", cfg)
	defer clerk.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := clerk.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady failed: %v", err)
	}

	dest, err := clerk.Route("any_key")
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}
	if dest.ResourceAddress != sq.ResourceAddress {
		t.Fatalf("expected %s, got %s", sq.ResourceAddress, dest.ResourceAddress)
	}
}

