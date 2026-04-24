/*
 * Copyright © 2026 Strato GmbH
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

package status

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

type e2eEnv struct {
	server      *httptest.Server
	asynqClient *asynq.Client
	inspector   *asynq.Inspector
	redisClient redis.UniversalClient
	policySvc   policy.Service
	replID      entity.ReplicationStatusID
}

func setupE2E(t *testing.T) *e2eEnv {
	t.Helper()

	redisClient := testutil.SetupRedis(t)
	inspector := asynq.NewInspectorFromRedisClient(redisClient)
	asynqClient := asynq.NewClientFromRedisClient(redisClient)
	t.Cleanup(func() {
		inspector.Close()
		asynqClient.Close()
	})

	queueSvc := tasks.NewQueueService(inspector)
	policySvc := policy.NewService(redisClient, queueSvc, nil)

	replID := entity.ReplicationStatusID{
		User:        "testuser",
		FromStorage: "main",
		FromBucket:  "my-bucket",
		ToStorage:   "backup",
		ToBucket:    "my-bucket",
	}

	_, err := policySvc.AddBucketReplicationPolicy(t.Context(), replID, nil)
	require.NoError(t, err, "seed replication policy")

	conf := Config{
		Prefix: "test_prefix",
		MaintApi: MaintApiConfig{
			Enabled: true,
			Prefix:  "maint_api",
		},
	}

	handler, err := Handler(conf, zerolog.Nop(), nil, policySvc, queueSvc)
	require.NoError(t, err)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return &e2eEnv{
		server:      server,
		asynqClient: asynqClient,
		inspector:   inspector,
		redisClient: redisClient,
		policySvc:   policySvc,
		replID:      replID,
	}
}

// reset flushes Redis and re-seeds the replication policy.
func (e *e2eEnv) reset(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, e.redisClient.FlushAll(ctx).Err())
	_, err := e.policySvc.AddBucketReplicationPolicy(ctx, e.replID, nil)
	require.NoError(t, err, "re-seed replication policy")
}

// failToRetry enqueues a task and uses a temporary asynq server to fail it
// into the retry queue with the given error message.
func (e *e2eEnv) failToRetry(t *testing.T, queue string, task *asynq.Task, errMsg string) {
	t.Helper()
	ctx := t.Context()
	r := require.New(t)

	// Count existing retry tasks to know our target.
	retryBefore := 0
	if info, err := e.inspector.GetQueueInfo(queue); err == nil {
		retryBefore = info.Retry
	}

	_, err := e.asynqClient.EnqueueContext(ctx, task, asynq.Queue(queue))
	r.NoError(err, "enqueue task")

	srv := asynq.NewServerFromRedisClient(e.redisClient, asynq.Config{
		Concurrency: 1,
		BaseContext: func() context.Context {
			return ctx
		},
		IsFailure:       func(err error) bool { return true },
		Queues:          map[string]int{queue: 1},
		ShutdownTimeout: time.Microsecond,
	})
	err = srv.Start(asynq.HandlerFunc(func(_ context.Context, _ *asynq.Task) error {
		return fmt.Errorf("%s", errMsg)
	}))
	r.NoError(err, "start asynq server")

	r.Eventually(func() bool {
		info, err := e.inspector.GetQueueInfo(queue)
		if err != nil {
			return false
		}
		return info.Retry > retryBefore
	}, 5*time.Second, 50*time.Millisecond, "task should enter retry state")

	srv.Shutdown()
}

// failAndArchive enqueues a task with MaxRetry(0) and uses a temporary asynq
// server to fail it directly into the archived state. Using MaxRetry(0) ensures
// the processor's Archive path runs, which updates the failed_idx hash.
func (e *e2eEnv) failAndArchive(t *testing.T, queue string, task *asynq.Task, errMsg string) {
	t.Helper()
	ctx := t.Context()
	r := require.New(t)

	archivedBefore := 0
	if info, err := e.inspector.GetQueueInfo(queue); err == nil {
		archivedBefore = info.Archived
	}

	_, err := e.asynqClient.EnqueueContext(ctx, task, asynq.Queue(queue), asynq.MaxRetry(0))
	r.NoError(err, "enqueue task")

	srv := asynq.NewServerFromRedisClient(e.redisClient, asynq.Config{
		Concurrency: 1,
		BaseContext: func() context.Context {
			return ctx
		},
		IsFailure:       func(err error) bool { return true },
		Queues:          map[string]int{queue: 1},
		ShutdownTimeout: time.Microsecond,
	})
	err = srv.Start(asynq.HandlerFunc(func(_ context.Context, _ *asynq.Task) error {
		return fmt.Errorf("%s", errMsg)
	}))
	r.NoError(err, "start asynq server")

	r.Eventually(func() bool {
		info, err := e.inspector.GetQueueInfo(queue)
		if err != nil {
			return false
		}
		return info.Archived > archivedBefore
	}, 5*time.Second, 50*time.Millisecond, "task should enter archived state")

	srv.Shutdown()
}

func TestE2EMaintFailedTasks(t *testing.T) {
	env := setupE2E(t)
	baseURL := env.server.URL + "/maint_api/bucket/"
	bucket := env.replID.FromBucket
	queues := tasks.AllReplicationQueues(env.replID)

	makePayload := func(t *testing.T, object string) []byte {
		t.Helper()
		return makeTaskPayload(t, bucket, object, env.replID.FromStorage, env.replID.ToStorage, env.replID.ToBucket)
	}

	t.Run("missing bucket returns 404", func(t *testing.T) {
		resp, err := http.Get(baseURL)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("unknown bucket returns 500 mismatch", func(t *testing.T) {
		// A replication exists for "my-bucket", so requesting a different bucket
		// hits the FromBucket != bucket check and returns "replication bucket mismatch".
		resp, err := http.Get(baseURL + "nonexistent-bucket/failed-tasks")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		var body map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		assert.Contains(t, body["error"], "internal error")
	})

	t.Run("no tasks returns empty array", func(t *testing.T) {
		env.reset(t)
		resp, err := http.Get(baseURL + bucket + "/failed-tasks")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var result []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		assert.Empty(t, result)
		assert.NotNil(t, result)
	})

	t.Run("archived tasks returned", func(t *testing.T) {
		env.reset(t)
		queue := queues[0]
		env.failAndArchive(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "obj1.txt")),
			"upload failed: connection reset")
		env.failAndArchive(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "obj2.txt")),
			"upload failed: timeout")

		resp, err := http.Get(baseURL + bucket + "/failed-tasks")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result, 2)
		for _, ft := range result {
			assert.NotEmpty(t, ft.ErrorMessage)
			assert.Equal(t, tasks.TypeObjectSync, ft.TaskType)
			assert.Equal(t, bucket, ft.Bucket)
		}
	})

	t.Run("retry tasks returned", func(t *testing.T) {
		env.reset(t)
		queue := queues[0]
		env.failToRetry(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "retrying.txt")),
			"temporary failure")

		resp, err := http.Get(baseURL + bucket + "/failed-tasks")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result, 1)
		assert.Greater(t, result[0].RetryCount, 0)
	})

	t.Run("tasks aggregated across all queues", func(t *testing.T) {
		env.reset(t)
		for i, queue := range queues {
			env.failAndArchive(t, queue,
				asynq.NewTask(tasks.TypeObjectSync, makePayload(t, fmt.Sprintf("obj-q%d.txt", i))),
				fmt.Sprintf("error in queue %d", i))
		}

		resp, err := http.Get(baseURL + bucket + "/failed-tasks")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		assert.Len(t, result, len(queues))
	})

	t.Run("error_filter keeps only matching tasks", func(t *testing.T) {
		env.reset(t)
		queue := queues[0]
		env.failAndArchive(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "a.txt")),
			"connection reset")
		env.failAndArchive(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "b.txt")),
			"access denied")
		env.failAndArchive(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "c.txt")),
			"connection timeout")

		resp, err := http.Get(baseURL + bucket + "/failed-tasks?error_filter=connection")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result, 2)
		for _, ft := range result {
			assert.Contains(t, ft.ErrorMessage, "connection")
		}
	})

	t.Run("error_filter no match returns empty array", func(t *testing.T) {
		env.reset(t)
		queue := queues[0]
		env.failAndArchive(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "x.txt")),
			"some error")

		resp, err := http.Get(baseURL + bucket + "/failed-tasks?error_filter=nomatch")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		assert.Empty(t, result)
	})

	t.Run("paused queue retried tasks visible after forwarding to pending", func(t *testing.T) {
		env.reset(t)
		queue := queues[0]

		// 1. Fail a task into the retry queue.
		env.failToRetry(t, queue,
			asynq.NewTask(tasks.TypeObjectSync, makePayload(t, "paused-obj.txt")),
			"temporary failure")

		// Verify it shows up while in retry state.
		resp, err := http.Get(baseURL + bucket + "/failed-tasks")
		require.NoError(t, err)
		var before []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&before))
		resp.Body.Close()
		require.Len(t, before, 1, "task should be visible in retry state")

		// 2. Pause the queue (workers stop dequeuing).
		require.NoError(t, env.inspector.PauseQueue(queue))
		t.Cleanup(func() { _ = env.inspector.UnpauseQueue(queue) })

		// 3. Simulate the forwarder moving retry → pending
		//    (this happens over time as retry delays expire).
		n, err := env.inspector.RunAllRetryTasks(queue)
		require.NoError(t, err)
		require.Equal(t, 1, n, "one task should move from retry to pending")

		// Confirm the task is now in pending, not retry.
		info, err := env.inspector.GetQueueInfo(queue)
		require.NoError(t, err)
		assert.Equal(t, 0, info.Retry, "retry queue should be empty")
		assert.Equal(t, 1, info.Pending, "task should be in pending")

		// 4. The API must still return the task.
		resp, err = http.Get(baseURL + bucket + "/failed-tasks")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var after []*failedTask
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&after))
		require.Len(t, after, 1, "task must remain visible after moving to pending while paused")
		assert.Contains(t, after[0].ErrorMessage, "temporary failure")
	})

	t.Run("response has application/json content type", func(t *testing.T) {
		env.reset(t)
		resp, err := http.Get(baseURL + bucket + "/failed-tasks")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	})
}

func TestE2EOldStatusAPI(t *testing.T) {
	env := setupE2E(t)
	oldStatusURL := env.server.URL + "/test_prefix/bucket/" + env.replID.FromBucket

	t.Run("old API endpoint still works", func(t *testing.T) {
		env.reset(t)
		resp, err := http.Get(oldStatusURL)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
		var body struct {
			Bucket string `json:"bucket"`
			Status string `json:"status"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		assert.Equal(t, env.replID.FromBucket, body.Bucket)
		assert.NotEmpty(t, body.Status)
	})
}

func TestE2EMaintMigStatus(t *testing.T) {
	env := setupE2E(t)
	migStatusURL := env.server.URL + "/maint_api/bucket/" + env.replID.FromBucket + "/mig-status"

	t.Run("returns 200 with status field", func(t *testing.T) {
		env.reset(t)
		resp, err := http.Get(migStatusURL)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
		var body struct {
			Bucket string `json:"bucket"`
			Status string `json:"status"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		assert.Equal(t, env.replID.FromBucket, body.Bucket)
		assert.NotEmpty(t, body.Status)
	})

	t.Run("unknown bucket returns 500 mismatch", func(t *testing.T) {
		unknownURL := env.server.URL + "/maint_api/bucket/nonexistent-bucket/mig-status"
		resp, err := http.Get(unknownURL)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("missing bucket returns 404", func(t *testing.T) {
		resp, err := http.Get(env.server.URL + "/maint_api/bucket/")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}
