package client

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync/atomic"

	"github.com/quangh33/godicer/pkg/common"
	"github.com/quangh33/godicer/pkg/friend"
)

var (
	// ErrNoAssignmentAvailable is returned when routing is attempted before any assignment has been loaded.
	ErrNoAssignmentAvailable = errors.New("no assignment available: client has not received initial assignment from assigner")
	// ErrNoResourcesAssigned is returned when a slice has an empty resource set.
	ErrNoResourcesAssigned = errors.New("no resources assigned to slice")
	// ErrAllReplicasExhausted is returned when all assigned replicas for a slice have already been tried and failed.
	ErrAllReplicasExhausted = errors.New("all assigned replicas for slice have been tried and exhausted")
)

// ResourceRouter resolves routing keys to server pod incarnations (Squids)
// using lock-free O(log N) binary search over the cached Assignment.
// Ported directly from com.databricks.dicer.client.ResourceRouter in Scala.
type ResourceRouter struct {
	cell       *common.AtomicAssignmentCell
	rrCounters atomic.Uint64 // Used for round-robin balancing across multiple replicas
}

// NewResourceRouter constructs a ResourceRouter backed by an AtomicAssignmentCell.
func NewResourceRouter(cell *common.AtomicAssignmentCell) *ResourceRouter {
	return &ResourceRouter{
		cell: cell,
	}
}

// RouteAll returns all server pod incarnations (Squids) assigned to the slice containing key.
func (r *ResourceRouter) RouteAll(key string) ([]friend.Squid, error) {
	sliceKey := friend.NewSliceKeyFromString(key)
	assignment := r.cell.Get()
	if assignment == nil {
		return nil, ErrNoAssignmentAvailable
	}

	sa := assignment.SliceMap.LookUp(sliceKey)
	resources := sa.Resources()
	if len(resources) == 0 {
		return nil, fmt.Errorf("%w for key %q", ErrNoResourcesAssigned, key)
	}
	return resources, nil
}

// Route returns a single primary server pod incarnation (Squid) for the given key.
// If multiple replicas are assigned, it round-robins across them to balance read load.
func (r *ResourceRouter) Route(key string) (friend.Squid, error) {
	resources, err := r.RouteAll(key)
	if err != nil {
		return friend.Squid{}, err
	}

	if len(resources) == 1 {
		return resources[0], nil
	}

	// Round-robin selection across replicas
	idx := r.rrCounters.Add(1) % uint64(len(resources))
	return resources[idx], nil
}

// RouteTwoLevel implements two-level sharding matching Scala's getStubForKey(primaryKey, secondaryKey).
// The primaryKey selects the owning slice, while the secondaryKey deterministically hashes to
// one specific replica in the assigned replica set.
func (r *ResourceRouter) RouteTwoLevel(primaryKey, secondaryKey string) (friend.Squid, error) {
	resources, err := r.RouteAll(primaryKey)
	if err != nil {
		return friend.Squid{}, err
	}

	if len(resources) == 1 {
		return resources[0], nil
	}

	// Deterministically hash secondaryKey across the replicas
	h := fnv.New64a()
	_, _ = h.Write([]byte(secondaryKey))
	hashVal := h.Sum64()
	idx := hashVal % uint64(len(resources))
	return resources[idx], nil
}

// RouteWithRetry implements retry-aware replica picking matching Scala's getNextStubForKey.
// Given a slice key and a list of already tried Squids, it picks an unpicked replica.
// If all replicas have been tried, it returns ErrAllReplicasExhausted.
func (r *ResourceRouter) RouteWithRetry(key string, tried []friend.Squid) (friend.Squid, error) {
	resources, err := r.RouteAll(key)
	if err != nil {
		return friend.Squid{}, err
	}

	triedMap := make(map[string]struct{}, len(tried))
	for _, sq := range tried {
		triedMap[sq.String()] = struct{}{}
	}

	var untried []friend.Squid
	for _, sq := range resources {
		if _, wasTried := triedMap[sq.String()]; !wasTried {
			untried = append(untried, sq)
		}
	}

	if len(untried) == 0 {
		return friend.Squid{}, fmt.Errorf("%w for key %q (%d replicas tried)", ErrAllReplicasExhausted, key, len(resources))
	}

	// Pick the first available untried replica
	return untried[0], nil
}

// Assignment returns the current snapshot of the cached Assignment.
func (r *ResourceRouter) Assignment() *common.Assignment {
	return r.cell.Get()
}
