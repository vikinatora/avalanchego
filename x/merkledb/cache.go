// Copyright (C) 2019-2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"sync"

	"github.com/ava-labs/avalanchego/utils/linkedhashmap"
	"github.com/ava-labs/avalanchego/utils/wrappers"
)

// A cache that calls [onEviction] on the evicted element.
type onEvictCache[K comparable, V any] struct {
	lock        sync.RWMutex
	maxSize     int
	currentSize int
	fifo        linkedhashmap.LinkedHashmap[K, V]

	// Must not call any method that grabs [c.lock]
	// because this would cause a deadlock.
	onEviction              func(K, V) error
	onEvictionBatchFinished func() error
	sizeFunc                func(K, V) int

	evictionBatchSize int
}

func newOnEvictCache[K comparable, V any](maxSize int, evictionBatchSize int, onEviction func(K, V) error, onEvictionBatchFinished func() error, sizeFunc func(K, V) int) onEvictCache[K, V] {
	return onEvictCache[K, V]{
		maxSize:                 maxSize,
		fifo:                    linkedhashmap.New[K, V](),
		onEviction:              onEviction,
		sizeFunc:                sizeFunc,
		evictionBatchSize:       evictionBatchSize,
		onEvictionBatchFinished: onEvictionBatchFinished,
	}
}

// Get an element from this cache.
func (c *onEvictCache[K, V]) Get(key K) (V, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.fifo.Get(key)
}

// Put an element into this cache. If this causes an element
// to be evicted, calls [c.onEviction] on the evicted element
// and returns the error from [c.onEviction]. Otherwise returns nil.
func (c *onEvictCache[K, V]) Put(key K, value V) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	oldValue, exists := c.fifo.Get(key)
	if exists {
		c.currentSize -= c.sizeFunc(key, oldValue)
	}
	c.fifo.Put(key, value) // Mark as MRU
	c.currentSize += c.sizeFunc(key, value)
	if c.currentSize > c.maxSize {
		return c.evictBatch()
	}
	return nil
}

func (c *onEvictCache[K, V]) evictBatch() error {
	for evictionSize := 0; evictionSize < c.evictionBatchSize; {
		oldestKey, oldestVal, exists := c.fifo.Oldest()
		if !exists {
			return c.onEvictionBatchFinished()
		}
		size := c.sizeFunc(oldestKey, oldestVal)
		evictionSize += size
		c.currentSize -= size
		c.fifo.Delete(oldestKey)
		if err := c.onEviction(oldestKey, oldestVal); err != nil {
			return err
		}
	}
	return c.onEvictionBatchFinished()
}

// Flush removes all elements from the cache.
// Returns the last non-nil error during [c.onEviction], if any.
// If [c.onEviction] errors, it will still be called for any
// subsequent elements and the cache will still be emptied.
func (c *onEvictCache[K, V]) Flush() error {
	c.lock.Lock()
	defer func() {
		c.fifo = linkedhashmap.New[K, V]()
		c.lock.Unlock()
	}()

	// Note that we can't use [c.fifo]'s iterator because [c.onEviction]
	// modifies [c.fifo], which violates the iterator's invariant.
	var errs wrappers.Errs
	for c.fifo.Len() > 0 {
		errs.Add(c.evictBatch())
	}
	return errs.Err
}
