package common

import (
	"sync"
	"sync/atomic"
)

// AssignmentListener represents a callback function invoked when a new Assignment is published.
type AssignmentListener func(newAssignment *Assignment)

// AtomicAssignmentCell provides thread-safe, lock-free reads and atomic updates for an Assignment.
// It serves as the Go equivalent of WatchValueCell[Assignment] (AssignmentValueCell) in Scala Dicer.
//
// Read operations (Get/LookUp) perform an atomic pointer load, executing in a few nanoseconds
// without acquiring any mutex or blocking other goroutines.
// Update operations atomically swap the pointer and notify registered listeners.
type AtomicAssignmentCell struct {
	ptr       atomic.Pointer[Assignment]
	mu        sync.RWMutex
	listeners []AssignmentListener
}

// NewAtomicAssignmentCell creates an AtomicAssignmentCell initialized with an initial Assignment.
func NewAtomicAssignmentCell(initial *Assignment) *AtomicAssignmentCell {
	cell := &AtomicAssignmentCell{}
	if initial != nil {
		cell.ptr.Store(initial)
	}
	return cell
}

// Get returns the current Assignment via lock-free atomic pointer load.
// Guaranteed to complete in O(1) without blocking or lock contention.
func (c *AtomicAssignmentCell) Get() *Assignment {
	return c.ptr.Load()
}

// Set atomically updates the current Assignment and notifies registered listeners.
// Returns true if the assignment was updated (i.e. newer generation or first initialization).
func (c *AtomicAssignmentCell) Set(newAssignment *Assignment) bool {
	if newAssignment == nil {
		return false
	}

	for {
		old := c.ptr.Load()
		if old != nil {
			// Monotonicity check: only accept assignments with greater or equal generation
			if newAssignment.Generation.Compare(old.Generation) < 0 {
				return false
			}
		}

		// Perform atomic compare-and-swap (or unconditional store if no race)
		if c.ptr.CompareAndSwap(old, newAssignment) {
			c.notifyListeners(newAssignment)
			return true
		}
	}
}

// AddListener registers a listener callback to be invoked whenever a new Assignment is set.
func (c *AtomicAssignmentCell) AddListener(listener AssignmentListener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listeners = append(c.listeners, listener)
}

// notifyListeners invokes all registered callbacks under read lock.
func (c *AtomicAssignmentCell) notifyListeners(newAssignment *Assignment) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, listener := range c.listeners {
		listener(newAssignment)
	}
}
