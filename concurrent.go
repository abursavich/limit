package limit

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

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
