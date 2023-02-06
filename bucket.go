// SPDX-License-Identifier: MIT
//
// Copyright 2023 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

package limit

import (
	"context"
	"math"
	"sync"
	"time"
)

// A TokenBucketOption provide optional configuration for a TokenBucket Policy.
type TokenBucketOption interface {
	applyTokenBucketOption(*tokenBucket)
}

type tokenBucketOptionFunc func(*tokenBucket)

func (fn tokenBucketOptionFunc) applyTokenBucketOption(bkt *tokenBucket) {
	fn(bkt)
}

// WithTokenBucketObserver returns a TokenBucketOption that sets an Observer.
func WithTokenBucketObserver(observer Observer) TokenBucketOption {
	return tokenBucketOptionFunc(func(bkt *tokenBucket) {
		bkt.obs = observer
	})
}

// Rate defines a frequency as number of operations per second.
type Rate float64

// Every converts a time interval between operations to a Rate.
func Every(interval time.Duration) Rate {
	if interval <= 0 {
		return Rate(math.Inf(1))
	}
	return 1 / Rate(interval.Seconds())
}

type tokenBucket struct {
	size int
	rate Rate
	wake chan struct{}
	obs  Observer

	mu      sync.Mutex
	tokens  float64
	updated time.Time
}

// TokenBucket returns a token bucket Policy with the given bucket size and refill rate.
func TokenBucket(size int, rate Rate, options ...TokenBucketOption) Policy {
	// TODO: Apply Observer Option to AllowAll/None.
	if float64(rate) >= math.MaxFloat64 { // max or +inf
		return AllowAll()
	}
	if size == 0 {
		return RejectAll()
	}
	bkt := &tokenBucket{
		size: size,
		rate: rate,
		wake: make(chan struct{}),
		obs:  noopObs,
	}
	for _, o := range options {
		o.applyTokenBucketOption(bkt)
	}
	return bkt
}

func (bkt *tokenBucket) Allow() bool {
	bkt.mu.Lock()
	defer bkt.mu.Unlock()

	// Refill the bucket.
	now := time.Now()
	tokens := bkt.refill(now)

	// Check capacity.
	allow := true
	if tokens < 1 {
		allow = false
		bkt.obs.ObserveReject()
	} else {
		tokens -= 1
		bkt.obs.ObserveAllow(0)
	}

	// Update the bucket state.
	bkt.updated = now
	bkt.tokens = tokens
	return allow
}

func (bkt *tokenBucket) Wait(ctx context.Context) error {
	start, wait, err := bkt.schedule(ctx)
	if err != nil {
		bkt.obs.ObserveCancel()
		return err
	}
	if wait == 0 {
		bkt.obs.ObserveAllow(0)
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	bkt.obs.ObserveEnqueue()
	defer bkt.obs.ObserveDequeue()

	select {
	case <-timer.C:
		bkt.obs.ObserveAllow(time.Since(start))
		return nil
	case <-bkt.wake:
		bkt.obs.ObserveAllow(time.Since(start))
		return nil
	case <-ctx.Done():
		bkt.obs.ObserveCancel()
		bkt.release(start)
		return ctx.Err()
	}
}

func (bkt *tokenBucket) Report(latency time.Duration, err error) {
	bkt.obs.ObserveReport(latency, err)
	if err != ErrAbandoned {
		return
	}
	select {
	case bkt.wake <- struct{}{}:
		// There is a race condition such that a goroutine
		// may be about to start waiting and we miss it,
		// but this best-effort is fine.
	default:
		bkt.mu.Lock()
		defer bkt.mu.Unlock()
		bkt.tokens = math.Min(float64(bkt.size), bkt.tokens+1)
	}
}

func (bkt *tokenBucket) schedule(ctx context.Context) (now time.Time, wait time.Duration, err error) {
	// Check if ctx is already done.
	select {
	case <-ctx.Done():
		return now, 0, ctx.Err()
	default:
	}

	bkt.mu.Lock()
	defer bkt.mu.Unlock()

	now = time.Now()

	// Refill bucket and deduct token.
	tokens := bkt.refill(now)
	tokens -= 1

	// If the bucket is at a token deficit, calculate the wait time.
	if tokens < 0 {
		seconds := -tokens / float64(bkt.rate)
		wait = time.Duration(seconds * float64(time.Second))

		// Short circuit if the context's deadline would be exceeded.
		if deadline, ok := ctx.Deadline(); ok && now.Add(wait).After(deadline) {
			return now, 0, context.DeadlineExceeded
		}
	}

	// Update the bucket state.
	bkt.updated = now
	bkt.tokens = tokens
	return now, wait, nil
}

func (bkt *tokenBucket) refill(now time.Time) (tokens float64) {
	prev := timeMin(bkt.updated, now)
	delta := float64(bkt.rate) * now.Sub(prev).Seconds()
	return math.Min(float64(bkt.size), bkt.tokens+delta)
}

func (bkt *tokenBucket) release(start time.Time) {
	bkt.mu.Lock()
	defer bkt.mu.Unlock()

	now := time.Now()
	delta := now.Sub(start).Seconds() * float64(bkt.rate)
	tokens := bkt.refill(now)

	// Update the bucket state.
	bkt.updated = now
	bkt.tokens = math.Min(float64(bkt.size), tokens+delta)
}

func timeMin(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
