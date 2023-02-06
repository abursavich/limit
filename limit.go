// SPDX-License-Identifier: MIT
//
// Copyright 2023 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

// Package limit provides rate limiting policies.
package limit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrRejected signals that the execution of the limited function has been rejected.
	ErrRejected = errors.New("limit: execution rejected")

	// ErrRevoked signals that a limited function was not executed after a successful Wait.
	ErrRevoked = errors.New("limit: execution revoked")
)

// A Policy is a policy for limiting the execution of a function.
type Policy interface {
	// Wait waits until a function may be executed according to the policy.
	// It returns an error if ctx is done or the policy rejects the execution.
	//
	// If an error is not returned, Done must be called with the results of the
	// limited function.
	//
	// If ctx has a deadline and the policy provides a scheduled execution time
	// after the deadline, it may return context.DeadlineExceeded preemptively.
	Wait(ctx context.Context) error

	// Done must be called after a successful Wait with the result of the limited function.
	// Use ErrRevoked to signal that the function was not executed after a successful Wait.
	Done(latency time.Duration, err error)
}

// Do executes the function according to the given policy.
func Do(ctx context.Context, policy Policy, fn func() error) (err error) {
	if err := policy.Wait(ctx); err != nil {
		return err
	}
	start := time.Now()
	defer func() { policy.Done(time.Since(start), err) }()
	return fn()
}

type serialPolicy []Policy

// SerialPolicy returns a Policy that serially combines the policies in the given order.
func SerialPolicy(policies ...Policy) Policy {
	s := make(serialPolicy, len(policies))
	copy(s, policies)
	return s
}

func (s serialPolicy) Wait(ctx context.Context) error {
	for i, p := range s {
		if err := p.Wait(ctx); err != nil {
			s[:i].Done(0, ErrRevoked)
			return err
		}
	}
	return nil
}

func (s serialPolicy) Done(latency time.Duration, err error) {
	for i := len(s) - 1; i >= 0; i-- {
		s.Done(latency, err)
	}
}

var allowAllPolicy = Policy(&allowAll{})

// AllowAll returns a Policy that never waits.
func AllowAll() Policy { return allowAllPolicy }

type allowAll struct{}

func (*allowAll) Wait(context.Context) error { return nil }
func (*allowAll) Done(time.Duration, error)  {}

var allowNonePolicy = Policy(&allowNone{})

// AllowNone returns a Policy that rejects everything.
func AllowNone() Policy { return allowNonePolicy }

type allowNone struct{}

func (*allowNone) Wait(ctx context.Context) error { return ErrRejected }
func (*allowNone) Done(time.Duration, error)      {}

type maxConcurrent struct {
	max int64
	n   int64

	mu   sync.Mutex
	cond sync.Cond
}

// MaxConcurrent returns a Policy that limits the maximum concurrent executions to n.
// It provides no fairness guarantees.
func MaxConcurrent(n int) Policy {
	m := &maxConcurrent{max: int64(n)}
	m.cond.L = &m.mu
	return m
}

func (m *maxConcurrent) Wait(ctx context.Context) error {
	// Check if ctx is already done.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Fast path.
	if m.compareAndSwap() {
		return nil
	}

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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *maxConcurrent) Done(latency time.Duration, err error) {
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
