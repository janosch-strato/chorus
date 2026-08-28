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

var destinationReads = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxy_destination_reads_total",
		Help: "Number of reads the proxy sent to the replication destination instead of the source storage.",
	},
	[]string{"storage"},
)

var destinationReadFallbacks = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxy_destination_read_fallbacks_total",
		Help: "Number of reads that were retried on the source storage after the replication destination failed to answer them.",
	},
	[]string{"storage"},
)

var destinationReadsSkipped = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxy_destination_reads_skipped_total",
		Help: "Number of reads kept on the source storage while reading from the replication destination was enabled, by reason.",
	},
	[]string{"reason"},
)

// ProxyDestinationRead counts a read served by the replication destination.
func ProxyDestinationRead(storage string) {
	destinationReads.WithLabelValues(storage).Inc()
}

// ProxyDestinationReadFallback counts a read the replication destination could
// not answer and that was retried on the source storage.
func ProxyDestinationReadFallback(storage string) {
	destinationReadFallbacks.WithLabelValues(storage).Inc()
}

// ProxyDestinationReadSkipped counts a read that stayed on the source storage
// although reading from the destination was enabled. The reason is one of the
// fixed set of reasons the proxy can decide on, see the router.
func ProxyDestinationReadSkipped(reason string) {
	destinationReadsSkipped.WithLabelValues(reason).Inc()
}
