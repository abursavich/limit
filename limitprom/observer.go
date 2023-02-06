// SPDX-License-Identifier: MIT
//
// Copyright 2023 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

package limitprom

import (
	"time"

	"bursavich.dev/limit"
	"github.com/prometheus/client_golang/prometheus"
)

type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (fn optionFunc) apply(c *config) { fn(c) }

type config struct {
	namespace               string
	disablePendingGauge     bool
	disablePendingCounter   bool
	disableReportedCounter  bool
	disableQueuedGauge      bool
	disableQueuedCounter    bool
	disableDequeuedCounter  bool
	disableCanceledCounter  bool
	disableRejectedCounter  bool
	disableAbandonedCounter bool
}

// WithNamespace returns an Option that sets the namespace for all metrics.
// The default is empty.
func WithNamespace(namespace string) Option {
	return optionFunc(func(c *config) {
		c.namespace = namespace
	})
}

// WithPendingGauge returns an Option that sets if the pending gauge is enabled.
// It is enabled by default.
func WithPendingGauge(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disablePendingGauge = !enabled
	})
}

// WithPendingCounter returns an Option that sets if the pending counter is enabled.
// It is enabled by default.
func WithPendingCounter(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disablePendingCounter = !enabled
	})
}

// WithReportedCounter returns an Option that sets if the reported counter is enabled.
// It is enabled by default.
func WithReportedCounter(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disableReportedCounter = !enabled
	})
}

// WithQueuedGauge returns an Option that sets if the queued gauge is enabled.
// It is enabled by default.
func WithQueuedGauge(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disableQueuedGauge = !enabled
	})
}

// WithQueuedCounter returns an Option that sets if the queued counter is enabled.
// It is enabled by default.
func WithQueuedCounter(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disableQueuedCounter = !enabled
	})
}

// WithDequeuedCounter returns an Option that sets if the dequeued counter is enabled.
// It is enabled by default.
func WithDequeuedCounter(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disableDequeuedCounter = !enabled
	})
}

// WithCanceledCounter returns an Option that sets if the canceled counter is enabled.
// It is enabled by default.
func WithCanceledCounter(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disableCanceledCounter = !enabled
	})
}

// WithRejectedCounter returns an Option that sets if the rejected counter is enabled.
// It is enabled by default.
func WithRejectedCounter(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disableRejectedCounter = !enabled
	})
}

// WithAbandonedCounter returns an Option that sets if the rejected counter is enabled.
// It is enabled by default.
func WithAbandonedCounter(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.disableAbandonedCounter = !enabled
	})
}

type Observer interface {
	limit.Observer
	prometheus.Collector
}

type observer struct {
	pending        prometheus.Gauge
	pendingTotal   prometheus.Counter
	reportedTotal  prometheus.Counter
	queued         prometheus.Gauge
	queuedTotal    prometheus.Counter
	dequeuedTotal  prometheus.Counter
	canceledTotal  prometheus.Counter
	rejectedTotal  prometheus.Counter
	abandonedTotal prometheus.Counter

	collectors []prometheus.Collector
}

// A Policy is a type of limit policy.
type Policy string

const (
	MaxConcurrentPolicy = Policy("MaxConcurrent")
	TokenBucketPolicy   = Policy("TokenBucket")
)

// NewObserver returns a new Observer with the given name, policy, and options.
func NewObserver(name string, policy Policy, options ...Option) Observer {
	var cfg config
	for _, o := range options {
		o.apply(&cfg)
	}
	constLabels := prometheus.Labels{
		"name":   name,
		"policy": string(policy),
	}
	obs := &observer{
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_pending",
			Help:        "The current number of pending limited operation.",
			ConstLabels: constLabels,
		}),
		pendingTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_pending_total",
			Help:        "The total number of allowed limited operations.",
			ConstLabels: constLabels,
		}),
		reportedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_reported_total",
			Help:        "The total number of reported limited operations.",
			ConstLabels: constLabels,
		}),
		queued: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_queued",
			Help:        "The current number of queued limited operation.",
			ConstLabels: constLabels,
		}),
		queuedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_queued_total",
			Help:        "The total number of queued limited operations.",
			ConstLabels: constLabels,
		}),
		dequeuedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_dequeued_total",
			Help:        "The total number of dequeued limited operations.",
			ConstLabels: constLabels,
		}),
		canceledTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_canceled_total",
			Help:        "The total number of canceled limited operations.",
			ConstLabels: constLabels,
		}),
		rejectedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_rejected_total",
			Help:        "The total number of rejected limited operations.",
			ConstLabels: constLabels,
		}),
		abandonedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   cfg.namespace,
			Name:        "limited_operations_abandoned_total",
			Help:        "The total number of abandoned limited operations.",
			ConstLabels: constLabels,
		}),
	}
	if !cfg.disablePendingGauge {
		obs.collectors = append(obs.collectors, obs.pending)
	}
	if !cfg.disablePendingCounter {
		obs.collectors = append(obs.collectors, obs.pendingTotal)
	}
	if !cfg.disableReportedCounter {
		obs.collectors = append(obs.collectors, obs.reportedTotal)
	}
	if !cfg.disableQueuedGauge {
		obs.collectors = append(obs.collectors, obs.queued)
	}
	if !cfg.disableQueuedCounter {
		obs.collectors = append(obs.collectors, obs.queuedTotal)
	}
	if !cfg.disableDequeuedCounter {
		obs.collectors = append(obs.collectors, obs.dequeuedTotal)
	}
	if !cfg.disableCanceledCounter {
		obs.collectors = append(obs.collectors, obs.canceledTotal)
	}
	if !cfg.disableRejectedCounter {
		obs.collectors = append(obs.collectors, obs.rejectedTotal)
	}
	if !cfg.disableAbandonedCounter {
		obs.collectors = append(obs.collectors, obs.abandonedTotal)
	}
	return obs
}

func (o *observer) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range o.collectors {
		c.Describe(ch)
	}
}

func (o *observer) Collect(ch chan<- prometheus.Metric) {
	for _, c := range o.collectors {
		c.Collect(ch)
	}
}

func (o *observer) ObserveAllow(wait time.Duration) {
	o.pending.Inc()
	o.pendingTotal.Inc()
}

func (o *observer) ObserveReport(latency time.Duration, err error) {
	if err == limit.ErrAbandoned {
		o.abandonedTotal.Inc()
	}
	o.reportedTotal.Inc()
	o.pending.Dec()
}

func (o *observer) ObserveEnqueue() {
	o.queued.Inc()
	o.queuedTotal.Inc()
}

func (o *observer) ObserveDequeue() {
	o.queued.Dec()
	o.dequeuedTotal.Inc()
}

func (o *observer) ObserveCancel() {
	o.canceledTotal.Inc()
}

func (o *observer) ObserveReject() {
	o.rejectedTotal.Inc()
}
