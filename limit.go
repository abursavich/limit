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
	"time"
)

var (
	// ErrRejected signals that the operation was rejected by the Policy.
	ErrRejected = errors.New("limit: operation rejected")

	// ErrAbandoned signals that the operation was abandoned after being allowed by the Policy.
	ErrAbandoned = errors.New("limit: operation abandoned")
)

// A Policy is a policy for limiting operations.
type Policy interface {
	// Allow returns a value indicating if an operation is currently allowed.
	// If it is allowed, Report must be called with the operation's results.
	Allow() bool

	// Wait waits until an operation is allowed according to the policy.
	// It returns an error if ctx is done or the Policy rejects the operation.
	//
	// If an error is not returned, Report must be called with the results of
	// the operation.
	//
	// If ctx has a deadline and the policy provides a scheduled operation time
	// after the deadline, it may return context.DeadlineExceeded preemptively.
	Wait(ctx context.Context) error

	// Report must be called with the results of an operation after it is allowed.
	// Use ErrAbandoned to signal that the operation was abandoned after being allowed.
	Report(latency time.Duration, err error)
}

// Do executes the function according to the given policy.
func Do(ctx context.Context, policy Policy, fn func() error) (err error) {
	if err := policy.Wait(ctx); err != nil {
		return err
	}
	start := time.Now()
	defer func() { policy.Report(time.Since(start), err) }()
	return fn()
}

type serialPolicy []Policy

// SerialPolicy returns a Policy that serially combines the policies in the given order.
func SerialPolicy(policies ...Policy) Policy {
	s := make(serialPolicy, len(policies))
	copy(s, policies)
	return s
}

func (s serialPolicy) Allow() bool {
	for i, p := range s {
		if !p.Allow() {
			s[:i].Report(0, ErrAbandoned)
			return false
		}
	}
	return true
}

func (s serialPolicy) Wait(ctx context.Context) error {
	for i, p := range s {
		if err := p.Wait(ctx); err != nil {
			s[:i].Report(0, ErrAbandoned)
			return err
		}
	}
	return nil
}

func (s serialPolicy) Report(latency time.Duration, err error) {
	for i := len(s) - 1; i >= 0; i-- {
		s[i].Report(latency, err)
	}
}

var allowAllPolicy = Policy(&allowAll{})

// AllowAll returns a Policy that never waits.
func AllowAll() Policy { return allowAllPolicy }

type allowAll struct{}

func (*allowAll) Allow() bool                 { return true }
func (*allowAll) Wait(context.Context) error  { return nil }
func (*allowAll) Report(time.Duration, error) {}

var rejectAllPolicy = Policy(&rejectAll{})

// RejectAll returns a Policy that rejects everything.
func RejectAll() Policy { return rejectAllPolicy }

type rejectAll struct{}

func (*rejectAll) Allow() bool                    { return false }
func (*rejectAll) Wait(ctx context.Context) error { return ErrRejected }
func (*rejectAll) Report(time.Duration, error)    {}

// An Observer observes operation events.
type Observer interface {
	// ObserveAllow is called when an operation is allowed.
	ObserveAllow(wait time.Duration)
	// ObserveReport is called when the result of an operation is reported.
	ObserveReport(latency time.Duration, err error)

	// ObserveEnqueue is called when an operation can't currently be allowed
	// and is parked until it's allowed or canceled in the future.
	ObserveEnqueue()
	// ObserveDequeue allow is called when an operation is unparked
	// due to being allowed or canceled.
	ObserveDequeue()

	// ObserveCancel is called when an operation is canceled.
	ObserveCancel()
	// ObserveReject is called when an operation is rejected.
	ObserveReject()
}

var noopObs = Observer(&noopObserver{})

type noopObserver struct{}

func (noopObserver) ObserveAllow(wait time.Duration)                {}
func (noopObserver) ObserveReport(latency time.Duration, err error) {}
func (noopObserver) ObserveEnqueue()                                {}
func (noopObserver) ObserveDequeue()                                {}
func (noopObserver) ObserveCancel()                                 {}
func (noopObserver) ObserveReject()                                 {}
