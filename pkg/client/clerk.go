package client

import (
	"context"

	"github.com/quangh33/godicer/pkg/common"
	"github.com/quangh33/godicer/pkg/friend"
)

// Clerk is the primary client interface for resolving routing keys to destination server pods.
// Equivalent to com.databricks.dicer.client.Clerk in Scala.
type Clerk struct {
	lookup *SliceLookup
}

// NewClerk creates and starts a new Clerk watching the specified Dicer target.
func NewClerk(target string, clientName string, cfg StateMachineConfig, opts ...SliceLookupOption) *Clerk {
	lookup := NewSliceLookup(target, clientName, cfg, opts...)
	lookup.Start()
	return &Clerk{
		lookup: lookup,
	}
}

// NewClerkWithCache creates a Clerk that shares its underlying SliceLookup instance
// with other Clerks via the provided SliceLookupCache.
func NewClerkWithCache(
	cache *SliceLookupCache,
	target string,
	clientName string,
	cfg StateMachineConfig,
	opts ...SliceLookupOption,
) *Clerk {
	if cache == nil {
		return NewClerk(target, clientName, cfg, opts...)
	}
	lookup := cache.GetOrCreate(target, clientName, cfg, opts...)
	return &Clerk{
		lookup: lookup,
	}
}

// Ready returns a channel that is closed when the initial assignment has been received from Dicer.
// Equivalent to Clerk.ready Future in Scala.
func (c *Clerk) Ready() <-chan struct{} {
	return c.lookup.Ready()
}

// WaitReady blocks until the initial assignment is received or the context is cancelled.
func (c *Clerk) WaitReady(ctx context.Context) error {
	return c.lookup.WaitReady(ctx)
}

// Route resolves a key to a single destination server pod incarnation (Squid).
// When multiple replicas are present, it balances load across them using round-robin.
func (c *Clerk) Route(key string) (friend.Squid, error) {
	return c.lookup.Router().Route(key)
}

// RouteAll returns all server pod incarnations (Squids) assigned to the slice containing key.
func (c *Clerk) RouteAll(key string) ([]friend.Squid, error) {
	return c.lookup.Router().RouteAll(key)
}

// RouteTwoLevel implements two-level sharding matching Scala's Clerk.getStubForKey(primaryKey, secondaryKey).
// The primaryKey selects the owning slice, and the secondaryKey deterministically chooses
// a replica in the assigned set.
func (c *Clerk) RouteTwoLevel(primaryKey, secondaryKey string) (friend.Squid, error) {
	return c.lookup.Router().RouteTwoLevel(primaryKey, secondaryKey)
}

// RouteWithRetry implements retry-aware routing matching Scala's Clerk.getNextStubForKey.
// When calling a replica fails, passing the already-tried squids ensures a different,
// untried replica is returned.
func (c *Clerk) RouteWithRetry(key string, tried []friend.Squid) (friend.Squid, error) {
	return c.lookup.Router().RouteWithRetry(key, tried)
}

// Assignment returns the latest cached Assignment, or nil if none has been received yet.
func (c *Clerk) Assignment() *common.Assignment {
	return c.lookup.Router().Assignment()
}

// Lookup returns the underlying SliceLookup.
func (c *Clerk) Lookup() *SliceLookup {
	return c.lookup
}

// Close gracefully stops background synchronization tasks.
func (c *Clerk) Close() {
	c.lookup.Stop()
}
