package client

import (
	"fmt"
	"sync"
)

// SliceLookupCache caches and shares SliceLookup instances keyed by Target and StateMachineConfig.
// It prevents redundant background Watch streams and RPC overhead when multiple Clerks
// access the same Dicer target within the same process.
// Ported directly from com.databricks.dicer.client.SliceLookupCache in Scala.
type SliceLookupCache struct {
	mu    sync.Mutex
	cache map[string]*SliceLookup
}

// NewSliceLookupCache constructs a thread-safe SliceLookupCache.
func NewSliceLookupCache() *SliceLookupCache {
	return &SliceLookupCache{
		cache: make(map[string]*SliceLookup),
	}
}

// GetOrCreate returns an existing started SliceLookup for the specified target and configuration,
// or creates and starts a new one if it does not exist yet.
func (c *SliceLookupCache) GetOrCreate(
	target string,
	clientName string,
	cfg StateMachineConfig,
	opts ...SliceLookupOption,
) *SliceLookup {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Cache key derived from target and watch address
	cacheKey := fmt.Sprintf("%s|%s", target, cfg.DefaultWatchAddress)
	if existing, found := c.cache[cacheKey]; found {
		return existing
	}

	lookup := NewSliceLookup(target, clientName, cfg, opts...)
	lookup.Start()
	c.cache[cacheKey] = lookup
	return lookup
}

// Size returns the number of active cached SliceLookup instances.
func (c *SliceLookupCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

// Close stops all cached SliceLookup instances and clears the cache.
func (c *SliceLookupCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, lookup := range c.cache {
		lookup.Stop()
	}
	c.cache = make(map[string]*SliceLookup)
}
