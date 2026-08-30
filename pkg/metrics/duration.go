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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// storageLatencyBuckets span everything from a storage answering in
// milliseconds to one taking minutes per request. The default prometheus
// buckets end at 10s, which is not enough to tell a slow storage from a very
// slow one.
var storageLatencyBuckets = prometheus.ExponentialBucketsRange(0.005, 120, 14)

var proxyRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "proxy_request_duration_seconds",
	Help:    "Time a proxied request took, including both transfers to the client, so the speed of the client is part of it.",
	Buckets: storageLatencyBuckets,
}, []string{"method", "storage"})

var proxyRequestInternalDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "proxy_request_internal_duration_seconds",
	Help:    "Part of proxy_request_duration_seconds that chorus is answerable for: from the last byte of the request read from the client to the first byte of the response written back to it, by the storage that answered.",
	Buckets: storageLatencyBuckets,
}, []string{"method", "storage"})

var migrationCopyPhaseDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "migration_copy_phase_duration_seconds",
	Help:    "Time spent in the phases of an object copy task: content copy, acl sync, tag sync.",
	Buckets: storageLatencyBuckets,
}, []string{"phase", "from", "to"})

var storageInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "storage_requests_in_flight",
	Help: "Number of api calls currently waiting for an s3 storage to answer.",
}, []string{"storage", "method"})

// StorageInFlight returns the in flight gauge of a storage and method. Keeping
// the gauge instead of looking it up twice avoids a label lookup per request.
func StorageInFlight(storage, method string) prometheus.Gauge {
	return storageInFlight.WithLabelValues(storage, method)
}

var storageHTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "storage_http_duration_seconds",
	Help:    "Duration of the http requests the s3 sdk clients make, by storage and http method. Covers the calls that do not go through the proxy, such as acl and tag sync.",
	Buckets: storageLatencyBuckets,
}, []string{"storage", "method"})

// StorageHTTPDuration records one sdk request to a storage.
func StorageHTTPDuration(storage, method string, d time.Duration) {
	storageHTTPDuration.WithLabelValues(storage, method).Observe(d.Seconds())
}

// ProxyRequestDuration records how long a proxied request took as the client
// saw it. An empty storage name means the request never reached a storage.
func ProxyRequestDuration(method, storage string, d time.Duration) {
	if storage == "" {
		storage = "none"
	}
	proxyRequestDuration.WithLabelValues(method, storage).Observe(d.Seconds())
}

// ProxyRequestInternalDuration records the time a proxied request spent inside
// chorus, which is the request without the two transfers to and from the
// client.
func ProxyRequestInternalDuration(method, storage string, d time.Duration) {
	if storage == "" {
		storage = "none"
	}
	proxyRequestInternalDuration.WithLabelValues(method, storage).Observe(d.Seconds())
}

// MigrationCopyPhase records the duration of one phase of an object copy.
func MigrationCopyPhase(phase, from, to string, d time.Duration) {
	migrationCopyPhaseDuration.WithLabelValues(phase, from, to).Observe(d.Seconds())
}
