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
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
)

// QueueInspector is the slice of *asynq.Inspector the queue collector
// needs. Declaring it as an interface keeps the collector testable and
// avoids forcing a concrete dependency on callers beyond what they
// already construct.
type QueueInspector interface {
	Queues() ([]string, error)
	GetQueueInfo(qname string) (*asynq.QueueInfo, error)
}

// queueCollector reports asynq queue depth at scrape time. Pulling the
// numbers on demand (rather than polling in the background) keeps the
// data fresh without an extra goroutine and means a slow/unavailable
// redis only stalls the scrape, never the worker.
type queueCollector struct {
	insp    QueueInspector
	tasks   *prometheus.Desc
	latency *prometheus.Desc
	memory  *prometheus.Desc
	paused  *prometheus.Desc
}

func newQueueCollector(insp QueueInspector) *queueCollector {
	return &queueCollector{
		insp: insp,
		tasks: prometheus.NewDesc(
			"queue_tasks",
			"Number of tasks in an asynq queue by state.",
			[]string{"queue", "state"}, nil,
		),
		latency: prometheus.NewDesc(
			"queue_oldest_pending_seconds",
			"Age of the oldest pending task in the queue.",
			[]string{"queue"}, nil,
		),
		memory: prometheus.NewDesc(
			"queue_memory_usage_bytes",
			"Approximate redis memory used by the queue.",
			[]string{"queue"}, nil,
		),
		paused: prometheus.NewDesc(
			"queue_paused",
			"Whether the queue is paused (1) or running (0).",
			[]string{"queue"}, nil,
		),
	}
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasks
	ch <- c.latency
	ch <- c.memory
	ch <- c.paused
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	queues, err := c.insp.Queues()
	if err != nil {
		return
	}
	for _, q := range queues {
		info, err := c.insp.GetQueueInfo(q)
		if err != nil {
			continue
		}
		states := []struct {
			state string
			count int
		}{
			{"pending", info.Pending},
			{"active", info.Active},
			{"scheduled", info.Scheduled},
			{"retry", info.Retry},
			{"archived", info.Archived},
			{"completed", info.Completed},
			{"aggregating", info.Aggregating},
		}
		for _, s := range states {
			ch <- prometheus.MustNewConstMetric(
				c.tasks, prometheus.GaugeValue, float64(s.count), q, s.state,
			)
		}
		ch <- prometheus.MustNewConstMetric(
			c.latency, prometheus.GaugeValue, info.Latency.Seconds(), q,
		)
		ch <- prometheus.MustNewConstMetric(
			c.memory, prometheus.GaugeValue, float64(info.MemoryUsage), q,
		)
		paused := 0.0
		if info.Paused {
			paused = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			c.paused, prometheus.GaugeValue, paused, q,
		)
	}
}

// RegisterQueueCollector registers a scrape-time queue-depth collector
// on the default registry (the one promhttp serves). Safe to call once
// per process from whichever service actually serves /metrics; a
// duplicate registration returns an error rather than panicking.
func RegisterQueueCollector(insp QueueInspector) error {
	return prometheus.Register(newQueueCollector(insp))
}
