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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

// versioningEnabled puts the feature flags in a known state for the tests that
// reach a feature check, and puts them back afterwards.
func versioningEnabled(t *testing.T, on bool) {
	t.Helper()
	features.Set(&features.Config{Versioning: on})
	t.Cleanup(func() { features.Set(nil) })
}

// replicatedRouter is a router whose bucket is replicated to testDest and
// whose storage answers everything with 204.
func replicatedRouter(t *testing.T) *router {
	t.Helper()
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	policySvc := policy.NewService(c, nil, nil)
	require.NoError(t, policySvc.AddBucketRoutingPolicy(ctx,
		entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, false))
	_, err := policySvc.AddBucketReplicationPolicy(ctx,
		entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket), nil)
	require.NoError(t, err)
	return &router{
		clients: fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     http.Header{},
			}, false, nil
		}}},
		policySvc:  policySvc,
		versionSvc: meta.NewVersionService(c),
		queueSvc:   &tasks.QueueServiceMock{},
	}
}

func TestRouteDeleteObjectWithoutAVersionNamesNone(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	rt := answeringRouter(t)
	_, taskList, _, _, err := rt.Route(deleteObjectReq("obj", ""))
	r.NoError(err)

	r.Len(taskList, 1)
	payload, ok := taskList[0].(*tasks.ObjectSyncPayload)
	r.True(ok)
	r.Empty(payload.Object.Version, "an ordinary delete is of the object as a whole")
}

// The version id the client names is the source storage's own, and nothing
// maps it to the copy the destination made. Carrying the delete out on the
// source alone would tell the client the version is gone while the destination
// still holds it, so the client is told instead.
func TestRouteDeleteObjectRefusesAVersion(t *testing.T) {
	r := require.New(t)

	var reached bool
	rt := answeringRouter(t)
	rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
		reached = true
		return nil, false, nil
	}}}

	resp, taskList, _, _, err := rt.Route(deleteObjectReq("obj", "3HL4kqtGWpM"))

	r.ErrorIs(err, dom.ErrNotImplemented, "a delete that cannot be replicated was accepted")
	r.False(reached, "the delete reached the storage, so it can no longer be refused")
	r.Nil(resp)
	r.Empty(taskList)
}

// The storage answers with the version id of the delete marker it just made,
// for keys the client named no version for. The keys come from the request, so
// nothing the storage says is taken for one the client named.
func TestRouteDeleteObjectsReplicatesWhenOnlyTheAnswerNamesVersions(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	rt := newTestRouter(t, &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
		body := `<DeleteResult>` +
			`<Deleted><Key>one</Key><VersionId>3HL4kqtGWpM</VersionId></Deleted>` +
			`<Deleted><Key>two</Key><VersionId>QUpfdndhfd8</VersionId></Deleted>` +
			`</DeleteResult>`
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{},
		}, false, nil
	}})

	_, taskList, _, _, err := rt.Route(deleteObjectsReq("one", "two"))
	r.NoError(err)

	keys, _ := deletedKeys(t, taskList)
	r.ElementsMatch([]string{"one", "two"}, keys,
		"the delete markers the storage minted were taken for versions the client named")
}

func TestRouteDeleteObjectVersionDoesNotDropTheMigratedRecord(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	versioningEnabled(t, true)

	c := testutil.SetupRedis(t)
	policySvc := policy.NewService(c, nil, nil)
	r.NoError(policySvc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, false))
	_, err := policySvc.AddBucketReplicationPolicy(ctx,
		entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket), nil)
	r.NoError(err)

	queueSvc := &tasks.QueueServiceMock{}
	rt := &router{
		clients:             fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) { return nil, false, nil }}},
		policySvc:           policySvc,
		versionSvc:          meta.NewVersionService(c),
		queueSvc:            queueSvc,
		readFromDestination: true,
	}

	setCopyState(queueSvc, "obj", asynq.TaskStateCompleted)
	queue, taskID := migrationCopyTask(testSource, testUser, testBucket, "obj",
		entity.NewBucketReplicationPolicyDestination(testDest, testBucket))
	_, seeded := queueSvc.Tasks[queue][taskID]
	r.True(seeded, "the copy record was not seeded, so this test proves nothing")

	_, _, _, _, err = rt.Route(deleteObjectReq("obj", "3HL4kqtGWpM"))
	r.ErrorIs(err, dom.ErrNotImplemented)

	_, stillThere := queueSvc.Tasks[queue][taskID]
	r.True(stillThere,
		"the object stopped counting as migrated although nothing was deleted, so reads leave the destination for no reason")
}

func TestRouteDeleteObjects(t *testing.T) {
	// The whole point of waiting for the answer: what the storage refused is
	// named there and nowhere else.
	t.Run("Only the keys the storage says it deleted are replicated", func(t *testing.T) {
		r := require.New(t)

		var sawResponseNeeded bool
		rt := newTestRouter(t, &fakeClient{do: func(req *http.Request) (*http.Response, bool, error) {
			sawResponseNeeded = xctx.GetResponseNeeded(req.Context())
			body := `<DeleteResult>` +
				`<Deleted><Key>gone</Key></Deleted>` +
				`<Error><Key>refused</Key><Code>AccessDenied</Code></Error>` +
				`</DeleteResult>`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     http.Header{},
			}, false, nil
		}})

		_, taskList, storage, isApiErr, err := rt.Route(deleteObjectsReq("gone", "refused"))
		r.NoError(err)
		r.False(isApiErr)
		r.Equal(testSource, storage)
		r.True(sawResponseNeeded, "the delete did not wait for the storage's answer")

		keys, unconfirmed := deletedKeys(t, taskList)
		r.ElementsMatch([]string{"gone"}, keys, "a key the storage refused was replicated as deleted")
		r.Zero(unconfirmed, "an answered delete is not a guess")
	})

	// The fallback, now reachable only when the storage took the batch and
	// then died: which of the keys went is unknown, so all of them are looked
	// at rather than trusted.
	t.Run("A storage that never answered puts every key up unconfirmed", func(t *testing.T) {
		r := require.New(t)

		rt := newTestRouter(t, &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			return nil, false, fmt.Errorf("%w: connection reset", dom.ErrStorageAnswerLost)
		}})

		_, taskList, _, _, err := rt.Route(deleteObjectsReq("one", "two"))
		r.ErrorIs(err, dom.ErrStorageAnswerLost)

		keys, unconfirmed := deletedKeys(t, taskList)
		r.ElementsMatch([]string{"one", "two"}, keys)
		r.Equal(len(keys), unconfirmed, "a guess was passed on as a record of what happened")
	})

	t.Run("A client that left before the request was written changes nothing", func(t *testing.T) {
		r := require.New(t)

		rt := newTestRouter(t, &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			return nil, false, fmt.Errorf("%w: context canceled", dom.ErrClientGone)
		}})

		_, taskList, _, _, err := rt.Route(deleteObjectsReq("one", "two"))
		r.ErrorIs(err, dom.ErrClientGone)
		r.Empty(taskList, "nothing reached the storage, so there is nothing to replicate")
	})
}

func TestRouteDeleteObjectWaitsForTheAnswer(t *testing.T) {
	r := require.New(t)

	var sawResponseNeeded bool
	rt := newTestRouter(t, &fakeClient{do: func(req *http.Request) (*http.Response, bool, error) {
		sawResponseNeeded = xctx.GetResponseNeeded(req.Context())
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		}, false, nil
	}})

	req := httptest.NewRequest(http.MethodDelete, "/"+testBucket+"/obj", nil)
	ctx := req.Context()
	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetObject(ctx, "obj")
	ctx = xctx.SetMethod(ctx, s3.DeleteObject)

	_, taskList, _, _, err := rt.Route(req.WithContext(ctx))
	r.NoError(err)
	r.True(sawResponseNeeded, "the delete did not wait for the storage's answer")

	keys, unconfirmed := deletedKeys(t, taskList)
	r.Equal([]string{"obj"}, keys)
	r.Zero(unconfirmed)
}

func objectReq(req *http.Request, method s3.Method, object string) *http.Request {
	ctx := req.Context()
	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetObject(ctx, object)
	ctx = xctx.SetMethod(ctx, method)
	return req.WithContext(ctx)
}

// See Route: each is carried out by the storage on its own, so nothing else
// says it is done.
func TestRouteWaitsForTheAnswer(t *testing.T) {
	copyReq := httptest.NewRequest(http.MethodPut, "/"+testBucket+"/obj", nil)
	copyReq.Header.Set("x-amz-copy-source", "/"+testBucket+"/src")
	completeReq := httptest.NewRequest(http.MethodPost, "/"+testBucket+"/obj?uploadId=u1",
		strings.NewReader(`<CompleteMultipartUpload></CompleteMultipartUpload>`))

	for _, tc := range []struct {
		name  string
		req   *http.Request
		waits bool
	}{
		{"DeleteObject", deleteObjectReq("obj", ""), true},
		{"DeleteObjects", deleteObjectsReq("obj"), true},
		{"CopyObject", objectReq(copyReq, s3.CopyObject, "obj"), true},
		{"CompleteMultipartUpload", objectReq(completeReq, s3.CompleteMultipartUpload, "obj"), true},
		// A put is on the storage as soon as its body is out.
		{"PutObject", objectReq(httptest.NewRequest(http.MethodPut, "/"+testBucket+"/obj",
			strings.NewReader("body")), s3.PutObject, "obj"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)

			var saw bool
			rt := newTestRouter(t, &fakeClient{do: func(req *http.Request) (*http.Response, bool, error) {
				saw = xctx.GetResponseNeeded(req.Context())
				return nil, false, errors.New("storage unreachable in this test")
			}})

			_, _, _, _, err := rt.Route(tc.req)
			r.Error(err)
			if tc.waits {
				r.True(saw, "the task made for this is only right once the storage is done, and nothing else says when that is")
			} else {
				r.False(saw, "a request needing no answer was put on the pool kept for the ones that do")
			}
		})
	}
}
