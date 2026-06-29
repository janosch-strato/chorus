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

// Rate-limit acquire outcome classes for the "result" label.
const (
	RateLimitAcquiredResult = "acquired"
	RateLimitRejectedResult = "rejected"
	RateLimitErrorResult    = "error"
)

var rateLimitAcquireTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ratelimit_acquire_total",
		Help: "Number of rate-limit acquire attempts by outcome.",
	},
	[]string{"name", "result"},
)

var rateLimitInUse = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "ratelimit_in_use",
		Help: "Rate-limit units currently held.",
	},
	[]string{"name"},
)

// RateLimitAcquired records a successful acquisition of n units.
func RateLimitAcquired(name string, n int64) {
	rateLimitAcquireTotal.WithLabelValues(name, RateLimitAcquiredResult).Inc()
	rateLimitInUse.WithLabelValues(name).Add(float64(n))
}

// RateLimitReleased records the release of n previously held units.
func RateLimitReleased(name string, n int64) {
	rateLimitInUse.WithLabelValues(name).Sub(float64(n))
}

// RateLimitRejected records an attempt rejected because the limit was
// reached (backpressure).
func RateLimitRejected(name string) {
	rateLimitAcquireTotal.WithLabelValues(name, RateLimitRejectedResult).Inc()
}

// RateLimitError records an attempt that failed for a reason other
// than backpressure (e.g. misconfiguration or backend error).
func RateLimitError(name string) {
	rateLimitAcquireTotal.WithLabelValues(name, RateLimitErrorResult).Inc()
}
