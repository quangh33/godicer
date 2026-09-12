package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quangh33/godicer/pkg/common"
	"github.com/quangh33/godicer/pkg/friend"
	dicerproto "github.com/quangh33/godicer/pkg/proto"
	"google.golang.org/grpc"
)

func TestSliceLookupCacheDeduplication(t *testing.T) {
	cache := NewSliceLookupCache()
	defer cache.Close()

	cfg1 := DefaultStateMachineConfig()
	cfg1.DefaultWatchAddress = "10.0.0.1:50051"

	// 1. First getOrCreate creates a new lookup
	lookupA := cache.GetOrCreate("target-A", "client-1", cfg1)
	if lookupA == nil {
		t.Fatal("expected non-nil lookup")
	}
	if cache.Size() != 1 {
		t.Fatalf("expected cache size 1, got %d", cache.Size())
	}

	// 2. Second getOrCreate with same target and config returns the SAME instance
	lookupA2 := cache.GetOrCreate("target-A", "client-2", cfg1)
	if lookupA != lookupA2 {
		t.Fatal("expected identical SliceLookup instance to be reused from cache")
	}
	if cache.Size() != 1 {
		t.Fatalf("expected cache size still 1, got %d", cache.Size())
	}

	// 3. Different target creates a new entry
	lookupB := cache.GetOrCreate("target-B", "client-3", cfg1)
	if lookupB == lookupA {
		t.Fatal("expected different SliceLookup instance for different target")
	}
	if cache.Size() != 2 {
		t.Fatalf("expected cache size 2, got %d", cache.Size())
	}

	// 4. Different watch address creates a new entry
	cfg2 := DefaultStateMachineConfig()
	cfg2.DefaultWatchAddress = "10.0.0.2:50051"
	lookupAOtherAddr := cache.GetOrCreate("target-A", "client-4", cfg2)
	if lookupAOtherAddr == lookupA {
		t.Fatal("expected different SliceLookup instance for different watch address")
	}
	if cache.Size() != 3 {
		t.Fatalf("expected cache size 3, got %d", cache.Size())
	}
}

func TestClerkSharingViaLookupCache(t *testing.T) {
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

	cache := NewSliceLookupCache()
	defer cache.Close()

	// Create 3 Clerks for the same target sharing the cache
	clerk1 := NewClerkWithCache(cache, "shared-target", "frontend-1", cfg)
	clerk2 := NewClerkWithCache(cache, "shared-target", "frontend-2", cfg)
	clerk3 := NewClerkWithCache(cache, "shared-target", "frontend-3", cfg)

	// All 3 clerks must share the exact same SliceLookup
	if clerk1.Lookup() != clerk2.Lookup() || clerk2.Lookup() != clerk3.Lookup() {
		t.Fatal("expected all 3 Clerks to share the identical SliceLookup pointer")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := clerk1.WaitReady(ctx); err != nil {
		t.Fatalf("clerk1 WaitReady failed: %v", err)
	}

	// All 3 clerks immediately have the assignment ready
	for i, c := range []*Clerk{clerk1, clerk2, clerk3} {
		dest, err := c.Route("my_key")
		if err != nil {
			t.Fatalf("clerk%d route failed: %v", i+1, err)
		}
		if dest.ResourceAddress != sq.ResourceAddress {
			t.Fatalf("clerk%d route mismatch: %s", i+1, dest.ResourceAddress)
		}
	}
}
