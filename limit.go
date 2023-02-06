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
	// If an error is not returned, Report must be called with the results of the
	// limited function.
	//
	// If ctx has a deadline and the policy provides a scheduled execution time
	// after the deadline, it may return context.DeadlineExceeded preemptively.
	Wait(ctx context.Context) error

	// Report must be called after a successful Wait with the result of the limited function.
	// Use ErrRevoked to signal that the function was not executed after a successful Wait.
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

func (s serialPolicy) Wait(ctx context.Context) error {
	for i, p := range s {
		if err := p.Wait(ctx); err != nil {
			s[:i].Report(0, ErrRevoked)
			return err
		}
	}
	return nil
}

func (s serialPolicy) Report(latency time.Duration, err error) {
	for i := len(s) - 1; i >= 0; i-- {
		s.Report(latency, err)
	}
}

var allowAllPolicy = Policy(&allowAll{})

// AllowAll returns a Policy that never waits.
func AllowAll() Policy { return allowAllPolicy }

type allowAll struct{}

func (*allowAll) Wait(context.Context) error  { return nil }
func (*allowAll) Report(time.Duration, error) {}

var rejectAllPolicy = Policy(&rejectAll{})

// RejectAll returns a Policy that rejects everything.
func RejectAll() Policy { return rejectAllPolicy }

type rejectAll struct{}

func (*rejectAll) Wait(ctx context.Context) error { return ErrRejected }
func (*rejectAll) Report(time.Duration, error)    {}

// An Observer observes limit events.
type Observer interface {
	ObservePending(wait time.Duration)
	ObserveReport(latency time.Duration, err error)

	ObserveEnqueue()
	ObserveDequeue()

	ObserveCancel()
	ObserveReject()
}

var noopObs = Observer(&noopObserver{})

type noopObserver struct{}

func (noopObserver) ObservePending(wait time.Duration)              {}
func (noopObserver) ObserveReport(latency time.Duration, err error) {}
func (noopObserver) ObserveEnqueue()                                {}
func (noopObserver) ObserveDequeue()                                {}
func (noopObserver) ObserveCancel()                                 {}
func (noopObserver) ObserveReject()                                 {}
