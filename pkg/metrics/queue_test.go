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
	"errors"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// fakeInspector answers the way a redis that is there, or is not, makes the
// real one answer.
type fakeInspector struct {
	queues    []string
	queuesErr error
	infoErr   error
}

func (f fakeInspector) Queues() ([]string, error) {
	return f.queues, f.queuesErr
}

func (f fakeInspector) GetQueueInfo(queue string) (*asynq.QueueInfo, error) {
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	return &asynq.QueueInfo{Queue: queue, Pending: 2}, nil
}

// collected returns how many series a collector wrote in one scrape.
func collectedCount(collector prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 256)
	collector.Collect(ch)
	close(ch)
	count := 0
	for range ch {
		count++
	}
	return count
}

func Test_queueCollector_countsWhatItCannotRead(t *testing.T) {
	r := require.New(t)

	// a queue that answers is written and counts no error
	before := counterValue(t, queueCollectErrors)
	r.Positive(collectedCount(newQueueCollector(fakeInspector{queues: []string{"q"}})))
	r.InDelta(before, counterValue(t, queueCollectErrors), 0.001)

	// redis gone: the scrape writes nothing, which is what an empty queue
	// looks like, so the failure has to be counted instead
	before = counterValue(t, queueCollectErrors)
	r.Zero(collectedCount(newQueueCollector(fakeInspector{queuesErr: errors.New("no redis")})))
	r.InDelta(before+1, counterValue(t, queueCollectErrors), 0.001)

	// one queue of many unreadable: the rest is still written, and the one
	// that failed is counted
	before = counterValue(t, queueCollectErrors)
	r.Zero(collectedCount(newQueueCollector(fakeInspector{
		queues:  []string{"a", "b"},
		infoErr: errors.New("gone"),
	})))
	r.InDelta(before+2, counterValue(t, queueCollectErrors), 0.001)
}

// counterValue reads one counter, which is what the tests of a metric that
// only ever goes up compare against.
func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	var written dto.Metric
	require.NoError(t, counter.Write(&written))
	return written.GetCounter().GetValue()
}
