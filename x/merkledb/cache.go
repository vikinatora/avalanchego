// Copyright (C) 2019-2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"sync"

	"github.com/ava-labs/avalanchego/utils/linkedhashmap"
	"github.com/ava-labs/avalanchego/utils/wrappers"
)

// A cache that calls [onEviction] on the evicted element.
type nodeCache struct {
	lock    sync.RWMutex
	maxSize int
	fifo    linkedhashmap.LinkedHashmap[path, *node]
	// Must not call any method that grabs [c.lock]
	// because this would cause a deadlock.
	onEviction func(path, *node) error
}

func newNodeCache(maxSize int, onEviction func(path, *node) error) *nodeCache {
	return &nodeCache{
		maxSize:    maxSize,
		fifo:       linkedhashmap.New[path, *node](),
		onEviction: onEviction,
	}
}

// removeOldest returns and removes the oldest element from this cache.
// Assumes [c.lock] is held.
func (c *nodeCache) removeOldest() (path, *node, bool) {
	k, v, exists := c.fifo.Oldest()
	if exists {
		c.fifo.Delete(k)
	}
	return k, v, exists
}

// Get an element from this cache.
func (c *nodeCache) Get(key path) (*node, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.fifo.Get(key)
}

// Put an element into this cache. If this causes an element
// to be evicted, calls [c.onEviction] on the evicted element
// and returns the error from [c.onEviction]. Otherwise returns nil.
func (c *nodeCache) Put(key path, value *node) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.fifo.Put(key, value) // Mark as MRU

	if c.fifo.Len() > c.maxSize {
		oldestKey, oldestVal, _ := c.fifo.Oldest()
		c.fifo.Delete(oldestKey)
		return c.onEviction(oldestKey, oldestVal)
	}
	return nil
}

// Flush removes all elements from the cache.
// Returns the last non-nil error during [c.onEviction], if any.
// If [c.onEviction] errors, it will still be called for any
// subsequent elements and the cache will still be emptied.
func (c *nodeCache) Flush() error {
	c.lock.Lock()
	defer func() {
		c.fifo = linkedhashmap.New[path, *node]()
		c.lock.Unlock()
	}()

	// Note that we can't use [c.fifo]'s iterator because [c.onEviction]
	// modifies [c.fifo], which violates the iterator's invariant.
	var errs wrappers.Errs
	for {
		key, node, exists := c.removeOldest()
		if !exists {
			// The cache is empty.
			return errs.Err
		}

		errs.Add(c.onEviction(key, node))
	}
}
