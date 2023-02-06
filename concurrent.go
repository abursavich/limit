// SPDX-License-Identifier: MIT
//
// Copyright 2023 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

package limit

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// A MaxConcurrentOption optional configuration for a MaxConcurrent Policy.
type MaxConcurrentOption interface {
	applyMaxConcurrentOption(*maxConcurrent)
}

type maxConcurrentOptionFunc func(*maxConcurrent)

func (fn maxConcurrentOptionFunc) applyMaxConcurrentOption(m *maxConcurrent) {
	fn(m)
}

// WithMaxConcurrentObserver returns a MaxConcurrentOption that sets an Observer.
func WithMaxConcurrentObserver(observer Observer) MaxConcurrentOption {
	return maxConcurrentOptionFunc(func(m *maxConcurrent) {
		m.obs = observer
	})
}

type maxConcurrent struct {
	n   int64
	max int64
	obs Observer

	mu   sync.Mutex
	cond sync.Cond
}

// MaxConcurrent returns a Policy that limits the maximum concurrent operations to n.
// It provides no fairness guarantees.
func MaxConcurrent(n int, options ...MaxConcurrentOption) Policy {
	m := &maxConcurrent{
		max: int64(n),
		obs: noopObs,
	}
	m.cond.L = &m.mu
	for _, o := range options {
		o.applyMaxConcurrentOption(m)
	}
	return m
}

func (m *maxConcurrent) Allow() bool {
	if m.compareAndSwap() {
		m.obs.ObserveAllow(0)
		return true
	}
	return false
}

func (m *maxConcurrent) Wait(ctx context.Context) error {
	// Check if ctx is already done.
	select {
	case <-ctx.Done():
		m.obs.ObserveCancel()
		return ctx.Err()
	default:
	}

	// Fast path.
	if m.compareAndSwap() {
		m.obs.ObserveAllow(0)
		return nil
	}

	m.obs.ObserveEnqueue()
	defer m.obs.ObserveDequeue()

	start := time.Now()
	wake := make(chan struct{})
	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		for !m.compareAndSwap() {
			m.cond.Wait()
		}

		select {
		case wake <- struct{}{}:
			// OK
		case <-ctx.Done():
			// Give up slot.
			atomic.AddInt64(&m.n, -1)
			m.cond.Signal()
		}
	}()

	select {
	case <-wake:
		m.obs.ObserveAllow(time.Since(start))
		return nil
	case <-ctx.Done():
		m.obs.ObserveCancel()
		return ctx.Err()
	}
}

func (m *maxConcurrent) Report(latency time.Duration, err error) {
	m.obs.ObserveReport(latency, err)

	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.AddInt64(&m.n, -1)
	m.cond.Signal()
}

func (m *maxConcurrent) compareAndSwap() bool {
	for n := atomic.LoadInt64(&m.n); n < m.max; n = atomic.LoadInt64(&m.n) {
		if atomic.CompareAndSwapInt64(&m.n, n, n+1) {
			return true
		}
	}
	return false
}
