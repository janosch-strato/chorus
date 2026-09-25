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

// Progress and backlog of the replications, which is the state the api
// reports per replication and nothing exported so far. It is read from redis
// on an interval rather than per scrape: the state of one replication costs a
// handful of round trips, and a scrape must not depend on how many
// replications exist.
//
// The numbers come from the queues of a replication, so they are labelled the
// way a queue is named, i.e. without the user: two replications of the same
// buckets that differ only in their user share their queues, and only the
// first of them is exported to keep the series unique.

package metrics

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/entity"
)

// Phases of a replication, which have a queue set and therefore a backlog of
// their own: the initial migration that copies what the listing found, and
// the events that keep the destination up to date afterwards.
const (
	PhaseInit  = "init"
	PhaseEvent = "event"
)

var replicationLabels = []string{"from", "from_bucket", "to", "to_bucket"}

var replicationCollectErrors = promauto.NewCounter(prometheus.CounterOpts{
	Name: "replication_collect_errors_total",
	Help: "Failed attempts to read the state of the replications from redis. The replication_* metrics keep the state of the last successful read while this grows.",
})

var (
	queuePendingDesc = prometheus.NewDesc("replication_queue_pending",
		"Tasks waiting in the queues of a replication phase: new, in progress, and waiting for a retry.",
		append([]string{"phase"}, replicationLabels...), nil)
	queueProcessedDesc = prometheus.NewDesc("replication_queue_processed_total",
		"Tasks of a replication phase that the workers finished. Resets when the queues are deleted.",
		append([]string{"phase"}, replicationLabels...), nil)
	queueRetryDesc = prometheus.NewDesc("replication_queue_retry",
		"Tasks of a replication phase that failed and wait for a retry.",
		append([]string{"phase"}, replicationLabels...), nil)
	queueFailedDesc = prometheus.NewDesc("replication_queue_failed",
		"Tasks of a replication phase that gave up after their last retry. These are not retried again and need someone to look at them.",
		append([]string{"phase"}, replicationLabels...), nil)
	queueLatencyDesc = prometheus.NewDesc("replication_queue_latency_seconds",
		"Age of the oldest task waiting in the queues of a replication phase. For the event phase this is how far the destination is behind the source.",
		append([]string{"phase"}, replicationLabels...), nil)
	queueMemoryDesc = prometheus.NewDesc("replication_queue_memory_bytes",
		"Memory the queues of a replication phase take in redis, as redis estimates it.",
		append([]string{"phase"}, replicationLabels...), nil)

	replicationPausedDesc = prometheus.NewDesc("replication_paused",
		"1 while at least one queue of the replication is paused.",
		replicationLabels, nil)
	replicationListingStartedDesc = prometheus.NewDesc("replication_listing_started",
		"1 once the listing of the source bucket has begun, which tells a replication that has not started yet from one that is still copying.",
		replicationLabels, nil)
	replicationInitDoneDesc = prometheus.NewDesc("replication_init_done",
		"1 once the listing has started and the initial migration has nothing left to copy. This is the state of the queues at the moment of the read, see replication_live_sync for the durable fact.",
		replicationLabels, nil)
	replicationLiveSyncDesc = prometheus.NewDesc("replication_live_sync",
		"1 once the copy that finished the initial sync recorded it. Unlike replication_init_done this stays true while events keep the destination up to date.",
		replicationLabels, nil)
	replicationArchivedDesc = prometheus.NewDesc("replication_archived",
		"1 for a replication that is archived and neither emits nor syncs events any more.",
		replicationLabels, nil)
)

// ReplicationLister is the part of the policy service the collector needs.
type ReplicationLister interface {
	ListReplicationPolicyInfo(ctx context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error)
}

// NewReplicationCollector returns a collector that exports the state the
// lister reports, refreshed on the given interval. It has to be registered
// and its Run started for it to export anything.
func NewReplicationCollector(lister ReplicationLister, interval time.Duration) *ReplicationCollector {
	return &ReplicationCollector{
		lister:   lister,
		interval: interval,
	}
}

type ReplicationCollector struct {
	lister   ReplicationLister
	interval time.Duration
	// snapshot is what the last successful read found, replaced as a whole so
	// that a scrape never sees a half-built set of series and a replication
	// that is gone stops being exported.
	snapshot atomic.Pointer[[]replicationSample]
}

var _ prometheus.Collector = (*ReplicationCollector)(nil)

type replicationSample struct {
	labels   []string
	paused   bool
	listing  bool
	initDone bool
	liveSync bool
	archived bool
	init     entity.QueueStats
	event    entity.QueueStats
}

// Run refreshes the snapshot until the context is done. It reads once before
// waiting, so the first scrape after a start has the state already.
func (c *ReplicationCollector) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		c.refresh(ctx)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *ReplicationCollector) refresh(ctx context.Context) {
	replications, err := c.lister.ListReplicationPolicyInfo(ctx)
	if err != nil {
		if ctx.Err() == nil {
			replicationCollectErrors.Inc()
			zerolog.Ctx(ctx).Err(err).Msg("metrics: unable to read replication state")
		}
		return
	}

	samples := make([]replicationSample, 0, len(replications))
	seen := make(map[entity.ReplicationStatusID]struct{}, len(replications))
	for id, replication := range replications {
		// the user is not part of a queue name, so the numbers of two
		// replications that differ only in it are the same numbers
		queueID := entity.ReplicationStatusID{
			FromStorage: id.FromStorage,
			FromBucket:  id.FromBucket,
			ToStorage:   id.ToStorage,
			ToBucket:    id.ToBucket,
		}
		if _, ok := seen[queueID]; ok {
			continue
		}
		seen[queueID] = struct{}{}
		samples = append(samples, replicationSample{
			labels:   []string{id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket},
			paused:   replication.IsPaused,
			listing:  replication.ListingStarted,
			initDone: replication.InitDone(),
			liveSync: replication.LiveSync,
			archived: replication.IsArchived,
			init:     replication.InitMigration,
			event:    replication.EventMigration,
		})
	}
	c.snapshot.Store(&samples)
}

func (c *ReplicationCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		queuePendingDesc, queueProcessedDesc, queueRetryDesc, queueFailedDesc,
		queueLatencyDesc, queueMemoryDesc,
		replicationPausedDesc, replicationListingStartedDesc, replicationInitDoneDesc,
		replicationLiveSyncDesc, replicationArchivedDesc,
	} {
		ch <- desc
	}
}

func (c *ReplicationCollector) Collect(ch chan<- prometheus.Metric) {
	samples := c.snapshot.Load()
	if samples == nil {
		return
	}
	for _, sample := range *samples {
		gauge(ch, replicationPausedDesc, boolValue(sample.paused), sample.labels)
		gauge(ch, replicationListingStartedDesc, boolValue(sample.listing), sample.labels)
		gauge(ch, replicationInitDoneDesc, boolValue(sample.initDone), sample.labels)
		gauge(ch, replicationLiveSyncDesc, boolValue(sample.liveSync), sample.labels)
		gauge(ch, replicationArchivedDesc, boolValue(sample.archived), sample.labels)
		collectQueue(ch, PhaseInit, sample.init, sample.labels)
		collectQueue(ch, PhaseEvent, sample.event, sample.labels)
	}
}

func collectQueue(ch chan<- prometheus.Metric, phase string, stats entity.QueueStats, labels []string) {
	withPhase := append([]string{phase}, labels...)
	gauge(ch, queuePendingDesc, float64(stats.Pending), withPhase)
	gauge(ch, queueRetryDesc, float64(stats.Rescheduled), withPhase)
	gauge(ch, queueFailedDesc, float64(stats.Failed), withPhase)
	gauge(ch, queueLatencyDesc, stats.Latency.Seconds(), withPhase)
	gauge(ch, queueMemoryDesc, float64(stats.MemoryUsage), withPhase)
	ch <- prometheus.MustNewConstMetric(queueProcessedDesc, prometheus.CounterValue, float64(stats.Done), withPhase...)
}

func gauge(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labels []string) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
