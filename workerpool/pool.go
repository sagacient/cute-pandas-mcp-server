// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package workerpool provides a semaphore-based worker pool for limiting concurrent operations.
package workerpool

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrPoolExhausted is returned when the worker pool is full and cannot accept new work.
var ErrPoolExhausted = errors.New("server is busy. All worker slots are occupied. Please try again later")

// Pool manages a fixed number of worker slots using a semaphore pattern.
type Pool struct {
	maxWorkers     int
	acquireTimeout time.Duration
	sem            chan struct{}
	mu             sync.RWMutex
	activeCount    int
	totalProcessed int64
}

// NewPool creates a new worker pool with the specified maximum workers and acquire timeout.
func NewPool(maxWorkers int, acquireTimeout time.Duration) *Pool {
	return &Pool{
		maxWorkers:     maxWorkers,
		acquireTimeout: acquireTimeout,
		sem:            make(chan struct{}, maxWorkers),
	}
}

// Acquire attempts to acquire a worker slot from the pool.
// Returns ErrPoolExhausted if a slot cannot be acquired within the timeout.
func (p *Pool) Acquire(ctx context.Context) error {
	// Create a timeout context if one isn't already set
	timeoutCtx, cancel := context.WithTimeout(ctx, p.acquireTimeout)
	defer cancel()

	select {
	case p.sem <- struct{}{}:
		p.mu.Lock()
		p.activeCount++
		p.mu.Unlock()
		return nil
	case <-timeoutCtx.Done():
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return ErrPoolExhausted
		}
		return timeoutCtx.Err()
	}
}

// Release releases a worker slot back to the pool.
func (p *Pool) Release() {
	select {
	case <-p.sem:
		p.mu.Lock()
		p.activeCount--
		p.totalProcessed++
		p.mu.Unlock()
	default:
		// Pool was not acquired, ignore
	}
}

// TryAcquire attempts to acquire a worker slot without blocking.
// Returns true if a slot was acquired, false otherwise.
func (p *Pool) TryAcquire() bool {
	select {
	case p.sem <- struct{}{}:
		p.mu.Lock()
		p.activeCount++
		p.mu.Unlock()
		return true
	default:
		return false
	}
}

// Stats returns the current pool statistics.
type Stats struct {
	MaxWorkers     int
	ActiveWorkers  int
	AvailableSlots int
	TotalProcessed int64
}

// Stats returns the current pool statistics.
func (p *Pool) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return Stats{
		MaxWorkers:     p.maxWorkers,
		ActiveWorkers:  p.activeCount,
		AvailableSlots: p.maxWorkers - p.activeCount,
		TotalProcessed: p.totalProcessed,
	}
}

// IsFull returns true if all worker slots are currently in use.
func (p *Pool) IsFull() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeCount >= p.maxWorkers
}
