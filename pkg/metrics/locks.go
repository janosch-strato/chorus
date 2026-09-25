/*
 * Copyright © 2026 STRATO GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Lock outcome classes for the "result" label on lock_acquire_total.
const (
	LockResultAcquired    = "acquired"
	LockResultNotObtained = "not_obtained"
	LockResultError       = "error"
)

var lockAcquireTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "lock_acquire_total",
		Help: "Number of distributed lock acquisition attempts by outcome.",
	},
	[]string{"kind", "result"},
)

var lockAcquireDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "lock_acquire_duration_seconds",
		Help:    "Time spent obtaining a distributed lock.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"kind"},
)

var lockHeld = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "lock_held",
		Help: "Number of distributed locks currently held.",
	},
	[]string{"kind"},
)

var lockReleaseRetryTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "lock_release_retry_total",
		Help: "Number of failed background lock release attempts.",
	},
	[]string{"kind"},
)

// LockAcquire records one acquisition attempt: its outcome class and
// the time spent in the obtain call (recorded for every outcome so a
// rise in time-to-fail is visible too).
func LockAcquire(kind, result string, seconds float64) {
	lockAcquireTotal.WithLabelValues(kind, result).Inc()
	lockAcquireDuration.WithLabelValues(kind).Observe(seconds)
}

func LockHeldInc(kind string) {
	lockHeld.WithLabelValues(kind).Inc()
}

func LockHeldDec(kind string) {
	lockHeld.WithLabelValues(kind).Dec()
}

func LockReleaseRetry(kind string) {
	lockReleaseRetryTotal.WithLabelValues(kind).Inc()
}
