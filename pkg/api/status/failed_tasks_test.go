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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
)

func makeTaskPayload(t *testing.T, bucket, object, fromStorage, toStorage, toBucket string) []byte {
	t.Helper()
	p := tasks.ObjectSyncPayload{
		Object: dom.Object{Bucket: bucket, Name: object},
		Sync: tasks.Sync{
			FromStorage: fromStorage,
			ToStorage:   toStorage,
			ToBucket:    toBucket,
		},
	}
	data, err := json.Marshal(p)
	require.NoError(t, err)
	return data
}

func TestTaskInfoToFailedTask(t *testing.T) {
	failedAt := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	payload := makeTaskPayload(t, "src-bucket", "some/object.bin", "main", "backup", "dst-bucket")

	t.Run("populates all fields from task info", func(t *testing.T) {
		info := &asynq.TaskInfo{
			ID:           "some-task-id",
			Type:         tasks.TypeObjectSync,
			Payload:      payload,
			LastErr:      "unable to upload object: connection reset",
			Retried:      2,
			MaxRetry:     5,
			LastFailedAt: failedAt,
		}

		got, err := taskInfoToFailedTask(info)
		require.NoError(t, err)

		assert.Equal(t, "some-task-id", got.TaskID)
		assert.Equal(t, tasks.TypeObjectSync, got.TaskType)
		assert.Equal(t, "unable to upload object: connection reset", got.ErrorMessage)
		assert.Equal(t, 2, got.RetryCount)
		assert.Equal(t, 5, got.MaxRetry)
		assert.Equal(t, "some/object.bin", got.Object)
		assert.Equal(t, "src-bucket", got.Bucket)
		assert.Equal(t, "dst-bucket", got.ToBucket)
		assert.NotNil(t, got.LastFailedAt)
		assert.Equal(t, failedAt.Unix(), got.LastFailedAt.Unix())
	})

	t.Run("zero LastFailedAt produces nil timestamp", func(t *testing.T) {
		info := &asynq.TaskInfo{
			Type:    tasks.TypeObjectSync,
			Payload: payload,
			LastErr: "",
		}

		got, err := taskInfoToFailedTask(info)
		require.NoError(t, err)

		assert.Nil(t, got.LastFailedAt)
	})
}

// --- handleMaintFailedTasks tests ---

const testPathPrefix = "/maint_api/bucket/"

var testReplID = entity.ReplicationStatusID{
	User:        "",
	FromStorage: "main",
	FromBucket:  "my-bucket",
	ToStorage:   "backup",
	ToBucket:    "my-bucket",
}

func setupMocks(t *testing.T) (*tasks.QueueServiceMock, *policyServiceMock) {
	t.Helper()
	qMock := &tasks.QueueServiceMock{
		Queues: make(map[string]int),
		Paused: make(map[string]bool),
	}
	pMock := &policyServiceMock{
		replications: map[entity.ReplicationStatusID]entity.ReplicationStatusExtended{
			testReplID: {},
		},
	}
	return qMock, pMock
}

// seedFailedTask adds a task to the mock's FailedTasks for the given queue.
func seedFailedTask(qMock *tasks.QueueServiceMock, queue string, info *asynq.TaskInfo) {
	if qMock.FailedTasks == nil {
		qMock.FailedTasks = make(map[string][]*asynq.TaskInfo)
	}
	qMock.FailedTasks[queue] = append(qMock.FailedTasks[queue], info)
}

func callHandler(qMock *tasks.QueueServiceMock, pMock *policyServiceMock, bucket string, queryParts ...string) *httptest.ResponseRecorder {
	logger := zerolog.Nop()
	urlPath := testPathPrefix + bucket + "/failed-tasks"
	if len(queryParts) > 0 {
		urlPath += "?" + queryParts[0]
	}
	req := httptest.NewRequest(http.MethodGet, urlPath, nil)
	rec := httptest.NewRecorder()
	handleMaintFailedTasks(logger, qMock, pMock, bucket, rec, req)
	return rec
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) []*failedTask {
	t.Helper()
	var resp []*failedTask
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

func TestHandleMaintFailedTasks(t *testing.T) {
	payload := makeTaskPayload(t, "my-bucket", "obj.txt", "main", "backup", "my-bucket")

	t.Run("policy service error returns 500", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		pMock.err = errors.New("redis down")
		pMock.replications = nil
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("no replication found returns 404", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		pMock.replications = map[entity.ReplicationStatusID]entity.ReplicationStatusExtended{}
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Body.String(), "no replication found")
	})

	t.Run("multiple replications returns 500", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		otherID := testReplID
		otherID.ToStorage = "archive"
		pMock.replications[otherID] = entity.ReplicationStatusExtended{}
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("replication bucket mismatch returns 500", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		wrongID := testReplID
		wrongID.FromBucket = "other-bucket"
		pMock.replications = map[entity.ReplicationStatusID]entity.ReplicationStatusExtended{
			wrongID: {},
		}
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "internal error")
	})

	t.Run("retry task included", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		queues := tasks.AllReplicationQueues(testReplID)
		seedFailedTask(qMock, queues[0], &asynq.TaskInfo{
			ID: "t1", Type: tasks.TypeObjectSync, Payload: payload,
			State: asynq.TaskStateRetry, Retried: 2, MaxRetry: 5, LastErr: "err1",
		})
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusOK, rec.Code)
		resp := decodeResponse(t, rec)
		require.Len(t, resp, 1)
		assert.Equal(t, 2, resp[0].RetryCount)
	})

	t.Run("archived task included", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		queues := tasks.AllReplicationQueues(testReplID)
		seedFailedTask(qMock, queues[0], &asynq.TaskInfo{
			ID: "t1", Type: tasks.TypeObjectSync, Payload: payload,
			State: asynq.TaskStateArchived, Retried: 5, MaxRetry: 5, LastErr: "fatal",
		})
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusOK, rec.Code)
		resp := decodeResponse(t, rec)
		require.Len(t, resp, 1)
	})

	t.Run("tasks aggregated across all replication queues", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		queues := tasks.AllReplicationQueues(testReplID)
		for i, q := range queues {
			seedFailedTask(qMock, q, &asynq.TaskInfo{
				ID: fmt.Sprintf("t%d", i), Type: tasks.TypeObjectSync, Payload: payload,
				State: asynq.TaskStateRetry, Retried: 1, MaxRetry: 5, LastErr: fmt.Sprintf("err-%d", i),
			})
		}
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusOK, rec.Code)
		resp := decodeResponse(t, rec)
		assert.Len(t, resp, len(queues))
	})

	t.Run("error_filter keeps only matching tasks", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		queues := tasks.AllReplicationQueues(testReplID)
		seedFailedTask(qMock, queues[0], &asynq.TaskInfo{
			ID: "t1", Type: tasks.TypeObjectSync, Payload: payload,
			State: asynq.TaskStateArchived, LastErr: "connection reset",
		})
		seedFailedTask(qMock, queues[0], &asynq.TaskInfo{
			ID: "t2", Type: tasks.TypeObjectSync, Payload: payload,
			State: asynq.TaskStateArchived, LastErr: "access denied",
		})
		seedFailedTask(qMock, queues[0], &asynq.TaskInfo{
			ID: "t3", Type: tasks.TypeObjectSync, Payload: payload,
			State: asynq.TaskStateArchived, LastErr: "connection timeout",
		})
		rec := callHandler(qMock, pMock, "my-bucket", "error_filter=connection")
		assert.Equal(t, http.StatusOK, rec.Code)
		resp := decodeResponse(t, rec)
		require.Len(t, resp, 2)
		for _, ft := range resp {
			assert.Contains(t, ft.ErrorMessage, "connection")
		}
	})

	t.Run("error_filter matches nothing returns empty array", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		queues := tasks.AllReplicationQueues(testReplID)
		seedFailedTask(qMock, queues[0], &asynq.TaskInfo{
			ID: "t1", Type: tasks.TypeObjectSync, Payload: payload,
			State: asynq.TaskStateArchived, LastErr: "some error",
		})
		rec := callHandler(qMock, pMock, "my-bucket", "error_filter=nomatch")
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `[]`)
	})

	t.Run("no tasks returns empty array not null", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `[]`)
	})

	t.Run("queue service error returns 500", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		qMock.ListErr = errors.New("redis connection lost")
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("response has application/json content type", func(t *testing.T) {
		qMock, pMock := setupMocks(t)
		rec := callHandler(qMock, pMock, "my-bucket")
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	})

}
