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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
)

type listerFunc func(ctx context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error)

func (f listerFunc) ListReplicationPolicyInfo(ctx context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error) {
	return f(ctx)
}

func replication(pending int, latency time.Duration) entity.ReplicationStatusExtended {
	return entity.ReplicationStatusExtended{
		ReplicationStatus: &entity.ReplicationStatus{ListingStarted: true},
		InitMigration: entity.QueueStats{
			Pending: pending,
			Done:    7,
			Latency: latency,
		},
		EventMigration: entity.QueueStats{
			Pending:     2,
			Done:        3,
			Rescheduled: 1,
			Failed:      4,
			Latency:     5 * time.Minute,
			MemoryUsage: 1024,
		},
	}
}

func Test_ReplicationCollector(t *testing.T) {
	r := require.New(t)
	id := entity.NewReplicationStatusID("user1", "source", "bucket", "destination", "to-bucket")
	collector := NewReplicationCollector(listerFunc(func(_ context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error) {
		return map[entity.ReplicationStatusID]entity.ReplicationStatusExtended{
			id: replication(11, time.Minute),
		}, nil
	}), time.Hour)
	collector.refresh(context.Background())

	// what is left to do and how far behind each phase is
	r.InDelta(11, phaseValue(t, collector, "replication_queue_pending", PhaseInit), 0.001)
	r.InDelta(2, phaseValue(t, collector, "replication_queue_pending", PhaseEvent), 0.001)
	r.InDelta(60, phaseValue(t, collector, "replication_queue_latency_seconds", PhaseInit), 0.001)
	r.InDelta(300, phaseValue(t, collector, "replication_queue_latency_seconds", PhaseEvent), 0.001)

	r.InDelta(7, phaseValue(t, collector, "replication_queue_processed_total", PhaseInit), 0.001)
	r.InDelta(3, phaseValue(t, collector, "replication_queue_processed_total", PhaseEvent), 0.001)
	r.InDelta(1, phaseValue(t, collector, "replication_queue_retry", PhaseEvent), 0.001)
	r.InDelta(4, phaseValue(t, collector, "replication_queue_failed", PhaseEvent), 0.001)
	r.InDelta(1024, phaseValue(t, collector, "replication_queue_memory_bytes", PhaseEvent), 0.001)

	// listing has started, but the initial migration has 11 tasks left
	r.InDelta(1, phaseValue(t, collector, "replication_listing_started", ""), 0.001)
	r.InDelta(0, phaseValue(t, collector, "replication_init_done", ""), 0.001)
	r.InDelta(0, phaseValue(t, collector, "replication_live_sync", ""), 0.001)
	r.InDelta(0, phaseValue(t, collector, "replication_paused", ""), 0.001)
	r.InDelta(0, phaseValue(t, collector, "replication_archived", ""), 0.001)

	// the replication is labelled the way its queues are named, without the user
	metric := collected(t, collector, "replication_queue_pending", PhaseInit)
	r.True(hasLabel(metric, "from", "source"))
	r.True(hasLabel(metric, "from_bucket", "bucket"))
	r.True(hasLabel(metric, "to", "destination"))
	r.True(hasLabel(metric, "to_bucket", "to-bucket"))
	r.Len(metric.GetLabel(), 5, "phase and the four queue labels, no user")
}

func Test_ReplicationCollector_initDone(t *testing.T) {
	r := require.New(t)
	id := entity.NewReplicationStatusID("user1", "source", "bucket", "destination", "to-bucket")
	status := replication(0, 0)
	status.IsPaused = true
	status.IsArchived = true
	status.LiveSync = true
	collector := NewReplicationCollector(listerFunc(func(_ context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error) {
		return map[entity.ReplicationStatusID]entity.ReplicationStatusExtended{id: status}, nil
	}), time.Hour)
	collector.refresh(context.Background())

	r.InDelta(1, phaseValue(t, collector, "replication_init_done", ""), 0.001)
	r.InDelta(1, phaseValue(t, collector, "replication_live_sync", ""), 0.001)
	r.InDelta(1, phaseValue(t, collector, "replication_paused", ""), 0.001)
	r.InDelta(1, phaseValue(t, collector, "replication_archived", ""), 0.001)
}

func Test_ReplicationCollector_keepsLastStateOnError(t *testing.T) {
	r := require.New(t)
	id := entity.NewReplicationStatusID("user1", "source", "bucket", "destination", "to-bucket")
	fail := false
	collector := NewReplicationCollector(listerFunc(func(_ context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error) {
		if fail {
			return nil, errors.New("redis is down")
		}
		return map[entity.ReplicationStatusID]entity.ReplicationStatusExtended{
			id: replication(11, time.Minute),
		}, nil
	}), time.Hour)

	// nothing read yet, so nothing exported
	r.Zero(countSeries(t, collector, "replication_queue_pending"))

	collector.refresh(context.Background())
	r.Equal(2, countSeries(t, collector, "replication_queue_pending"), "one series per phase")

	before := counterValue(t, replicationCollectErrors)
	fail = true
	collector.refresh(context.Background())
	r.Equal(2, countSeries(t, collector, "replication_queue_pending"),
		"a failed read must not blank out the state")
	r.InDelta(before+1, counterValue(t, replicationCollectErrors), 0.001)

	// a read that fails because the process is going down is not an error
	before = counterValue(t, replicationCollectErrors)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	collector.refresh(ctx)
	r.InDelta(before, counterValue(t, replicationCollectErrors), 0.001)
}

// Test_ReplicationCollector_sharedQueues pins that two replications sharing
// their queues are exported once: the user is not part of a queue name, so
// both would report the same numbers under the same labels.
func Test_ReplicationCollector_sharedQueues(t *testing.T) {
	r := require.New(t)
	collector := NewReplicationCollector(listerFunc(func(_ context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error) {
		return map[entity.ReplicationStatusID]entity.ReplicationStatusExtended{
			entity.NewReplicationStatusID("user1", "source", "bucket", "destination", "to-bucket"): replication(11, time.Minute),
			entity.NewReplicationStatusID("user2", "source", "bucket", "destination", "to-bucket"): replication(11, time.Minute),
		}, nil
	}), time.Hour)
	collector.refresh(context.Background())

	r.Equal(2, countSeries(t, collector, "replication_queue_pending"), "one series per phase")

	// and a registry accepts what the collector produces
	registry := prometheus.NewRegistry()
	r.NoError(registry.Register(collector))
	_, err := registry.Gather()
	r.NoError(err)
}

// collect returns everything a collector exports, as dto metrics paired with
// the name of their family.
func collect(t *testing.T, collector prometheus.Collector) map[string][]*dto.Metric {
	t.Helper()
	ch := make(chan prometheus.Metric, 256)
	collector.Collect(ch)
	close(ch)
	out := map[string][]*dto.Metric{}
	for metric := range ch {
		var written dto.Metric
		require.NoError(t, metric.Write(&written))
		// the name is only reachable through the string form of the desc
		desc := metric.Desc().String()
		_, rest, ok := strings.Cut(desc, `fqName: "`)
		require.True(t, ok, "unexpected desc %q", desc)
		name, _, ok := strings.Cut(rest, `"`)
		require.True(t, ok, "unexpected desc %q", desc)
		out[name] = append(out[name], &written)
	}
	return out
}

// collected returns the exported series of a metric, of the given phase for
// the metrics that have one.
func collected(t *testing.T, collector prometheus.Collector, name, phase string) *dto.Metric {
	t.Helper()
	for _, metric := range collect(t, collector)[name] {
		if phase == "" || hasLabel(metric, "phase", phase) {
			return metric
		}
	}
	t.Fatalf("no series of %q for phase %q", name, phase)
	return nil
}

func phaseValue(t *testing.T, collector prometheus.Collector, name, phase string) float64 {
	t.Helper()
	metric := collected(t, collector, name, phase)
	if metric.GetCounter() != nil {
		return metric.GetCounter().GetValue()
	}
	return metric.GetGauge().GetValue()
}

func countSeries(t *testing.T, collector prometheus.Collector, name string) int {
	t.Helper()
	return len(collect(t, collector)[name])
}

func hasLabel(metric *dto.Metric, name, value string) bool {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name && label.GetValue() == value {
			return true
		}
	}
	return false
}
