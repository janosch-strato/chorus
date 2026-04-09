// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/dom"
)

type QueueService interface {
	UnprocessedCount(ctx context.Context, ignoreNotFound bool, queues ...string) (int, error)
	IsPaused(ctx context.Context, queueName string) (bool, error)
	Resume(ctx context.Context, queueName string) error
	Pause(ctx context.Context, queueName string) error
	Delete(ctx context.Context, queueName string, force bool) error
	Stats(ctx context.Context, queueName string) (*QueueStats, error)
	ListFailedTasks(ctx context.Context, queueName string) ([]*asynq.TaskInfo, error)
}

type QueueStats struct {
	// Pending is the number of tasks to be processed in the queue.
	// Includes in_progress, not_started, and retried tasks.
	// In other words, all tasks except failed and processed tasks.
	Pending int
	// Total number of tasks processed.
	ProcessedTotal int
	// Rescheduled is the total number of tasks that have failed at least once and are pending retry.
	Rescheduled int

	// Failed tasks are those that have been retried the maximum number of times and removed from the queue.
	Failed int

	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue will not be processed.
	Paused bool

	// Total number of bytes that the queue and its tasks require to be stored in redis.
	// It is an approximate memory usage value in bytes since the value is computed by sampling.
	MemoryUsage int64

	// Latency of the queue, measured by the oldest pending task in the queue.
	Latency time.Duration
}

func NewQueueService(inspector *asynq.Inspector) *queueService {
	return &queueService{
		inspector: inspector,
	}
}

var _ QueueService = (*queueService)(nil)

type queueService struct {
	inspector *asynq.Inspector
}

func (q *queueService) Stats(ctx context.Context, queueName string) (*QueueStats, error) {
	info, err := q.getInfo(ctx, queueName)
	if err != nil {
		return nil, err
	}
	return &QueueStats{
		Pending:        unprocessedCount(info),
		ProcessedTotal: info.ProcessedTotal,
		Rescheduled:    info.FailedTotal - info.Archived,
		Failed:         info.Archived,
		Paused:         info.Paused,
		MemoryUsage:    info.MemoryUsage,
		Latency:        info.Latency,
	}, nil
}

func (q *queueService) Delete(ctx context.Context, queueName string, force bool) error {
	err := q.inspector.DeleteQueue(queueName, force)
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return fmt.Errorf("unable to delete queue %s: %w", queueName, dom.ErrNotFound)
	}
	if errors.Is(err, asynq.ErrQueueNotEmpty) {
		return fmt.Errorf("unable to delete non-empty queue %s: %w", queueName, dom.ErrInvalidArg)
	}
	return err
}

func (q *queueService) UnprocessedCount(ctx context.Context, ignoreNotFound bool, queues ...string) (int, error) {
	if len(queues) == 0 {
		return 0, fmt.Errorf("%w: no queues specified", dom.ErrInvalidArg)
	}
	count := 0
	for _, queue := range queues {
		info, err := q.getInfo(ctx, queue)
		if ignoreNotFound && errors.Is(err, dom.ErrNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		count += unprocessedCount(info)
	}
	return count, nil
}

func unprocessedCount(info *asynq.QueueInfo) int {
	return info.Pending + info.Active + info.Scheduled + info.Retry
}

func (q *queueService) IsPaused(ctx context.Context, queueName string) (bool, error) {
	info, err := q.getInfo(ctx, queueName)
	if err != nil {
		return false, err
	}
	return info.Paused, nil
}

func (q *queueService) getInfo(_ context.Context, queueName string) (*asynq.QueueInfo, error) {
	info, err := q.inspector.GetQueueInfo(queueName)
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return nil, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
		}
		return nil, fmt.Errorf("get queue info for %s: %w", queueName, err)
	}
	return info, nil
}

func (q *queueService) Pause(ctx context.Context, queueName string) error {
	err := q.inspector.PauseQueue(queueName)
	if err != nil {
		// ignore error if queue is already paused
		if strings.HasSuffix(err.Error(), "is already paused") {
			return nil
		}
	}
	return err
}

func (q *queueService) Resume(ctx context.Context, queueName string) error {
	err := q.inspector.UnpauseQueue(queueName)
	if err != nil {
		// ignore error if queue is not paused
		if strings.HasSuffix(err.Error(), "is not paused") {
			return nil
		}
	}
	return err
}

func (q *queueService) ListFailedTasks(_ context.Context, queueName string) ([]*asynq.TaskInfo, error) {
	infos, err := q.inspector.ListFailedTasks(queueName)
	if err != nil {
		return nil, fmt.Errorf("list failed tasks for queue %s: %w", queueName, err)
	}
	return infos, nil
}
