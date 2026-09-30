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

package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

type fakeClient struct {
	do func(*http.Request) (*http.Response, bool, error)
}

func (f *fakeClient) Do(req *http.Request) (*http.Response, bool, error) { return f.do(req) }
func (f *fakeClient) Config() s3.Storage                                 { return s3.Storage{} }
func (f *fakeClient) Name() string                                       { return testSource }
func (f *fakeClient) S3() *s3client.S3                                   { return nil }
func (f *fakeClient) AWS() *s3client.AWS                                 { return nil }
func (f *fakeClient) SNS() *sns.Client                                   { return nil }
func (f *fakeClient) IsOnline() bool                                     { return true }

type fakeClients struct{ c s3client.Client }

func (f fakeClients) GetByName(context.Context, string) (s3client.Client, error) { return f.c, nil }
func (f fakeClients) DefaultRegion() string                                      { return "" }

func multiDeleteBody(keys ...string) string {
	var b strings.Builder
	b.WriteString(`<Delete>`)
	for _, k := range keys {
		fmt.Fprintf(&b, `<Object><Key>%s</Key></Object>`, k)
	}
	b.WriteString(`</Delete>`)
	return b.String()
}

func deleteObjectsReq(keys ...string) *http.Request {
	body := multiDeleteBody(keys...)
	req := httptest.NewRequest(http.MethodPost, "/"+testBucket+"?delete", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	ctx := req.Context()
	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetMethod(ctx, s3.DeleteObjects)
	return req.WithContext(ctx)
}

func deletedKeys(t *testing.T, taskList []tasks.SyncTask) (keys []string, unconfirmed int) {
	t.Helper()
	for _, task := range taskList {
		payload, ok := task.(*tasks.ObjectSyncPayload)
		require.True(t, ok, "unexpected task type %T", task)
		require.True(t, payload.Deleted, "a delete task list holds a task that is not a delete")
		keys = append(keys, payload.Object.Name)
		if payload.Unconfirmed {
			unconfirmed++
		}
	}
	return keys, unconfirmed
}

func newTestRouter(t *testing.T, client s3client.Client) *router {
	t.Helper()
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	policySvc := policy.NewService(c, nil, nil)
	require.NoError(t, policySvc.AddBucketRoutingPolicy(ctx,
		entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, false))
	versionSvc := meta.NewVersionService(c)
	routerRedis[t.Name()] = c
	t.Cleanup(func() { delete(routerRedis, t.Name()) })
	return &router{
		clients:       fakeClients{c: client},
		policySvc:     policySvc,
		versionSvc:    versionSvc,
		storageSvc:    storage.New(c),
		destPause:     newDestinationPause(),
		replSvc:       replication.New(asynq.NewClientFromRedisClient(c), versionSvc, policySvc),
		deleteIntents: meta.NewDeleteIntents(c),
		instanceID:    testInstance,
	}
}

// What the router under test calls itself, see meta.DeleteIntent.
const testInstance = "proxy-under-test"

// The redis a test's router was built on.
var routerRedis = map[string]redis.UniversalClient{}

func queuedDeletes(t *testing.T, c redis.UniversalClient) []string {
	t.Helper()
	inspector := asynq.NewInspectorFromRedisClient(c)
	queues, err := inspector.Queues()
	require.NoError(t, err)
	var objects []string
	for _, queue := range queues {
		infos, err := inspector.ListPendingTasks(queue)
		require.NoError(t, err)
		for _, info := range infos {
			var payload tasks.ObjectSyncPayload
			if err := json.Unmarshal(info.Payload, &payload); err != nil || !payload.Deleted {
				continue
			}
			objects = append(objects, payload.Object.Name)
		}
	}
	return objects
}

func deleteObjectReq(object, versionID string) *http.Request {
	target := "/" + testBucket + "/" + object
	if versionID != "" {
		target += "?versionId=" + versionID
	}
	req := httptest.NewRequest(http.MethodDelete, target, nil)
	ctx := req.Context()
	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetObject(ctx, object)
	ctx = xctx.SetMethod(ctx, s3.DeleteObject)
	return req.WithContext(ctx)
}

func answeringRouter(t *testing.T) *router {
	t.Helper()
	return newTestRouter(t, &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		}, false, nil
	}})
}
