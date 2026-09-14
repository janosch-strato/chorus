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

func versioningEnabled(t *testing.T, on bool) {
	t.Helper()
	features.Set(&features.Config{Versioning: on})
	t.Cleanup(func() { features.Set(nil) })
}

func replicatedRouter(t *testing.T) *router {
	t.Helper()
	rt := answeringRouter(t)
	_, err := rt.policySvc.AddBucketReplicationPolicy(t.Context(),
		entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket), nil)
	require.NoError(t, err)
	return rt
}

// The delete queues its own replication, so Route hands no task back.
func TestRouteDeleteObjectQueuesItsOwnReplication(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	rt := replicatedRouter(t)
	c := routerRedis[t.Name()]
	_, taskList, _, _, err := rt.Route(deleteObjectReq("obj", ""))
	r.NoError(err)

	r.Empty(taskList, "the delete was handed back to be queued a second time")
	r.Equal([]string{"obj"}, queuedDeletes(t, c), "nothing was queued to replicate the delete")
}

func TestRouteDeleteObjectWritesItDownBeforeSending(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	rt := replicatedRouter(t)
	c := routerRedis[t.Name()]
	var writtenDownWhenSent []meta.DeleteIntent
	rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
		writtenDownWhenSent, _ = meta.NewDeleteIntents(c).OwnedBy(t.Context(), testInstance)
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		}, false, nil
	}}}

	_, _, _, _, err := rt.Route(deleteObjectReq("obj", ""))
	r.NoError(err)

	r.Len(writtenDownWhenSent, 1,
		"the storage was asked to delete the object before anything said the delete was happening")
	r.Equal("obj", writtenDownWhenSent[0].Object.Name)

	left, err := meta.NewDeleteIntents(c).OwnedBy(t.Context(), testInstance)
	r.NoError(err)
	r.Empty(left, "the delete is queued and still reads as unfinished, so a restart would make it again")
}

// The write's versions are its own record. Dropping them with the delete would
// leave the destination without an object the source has and nothing saying so.
func TestRouteDeleteObjectKeepsVersionsWrittenWhileItRan(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	rt := replicatedRouter(t)
	c := routerRedis[t.Name()]
	versionSvc := meta.NewVersionService(c)
	obj := dom.Object{Bucket: testBucket, Name: "obj"}

	// a write lands while the delete is on its way to the storage
	rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
		_, err := versionSvc.IncrementObj(t.Context(), obj, meta.ToDest(testSource, ""))
		require.NoError(t, err)
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		}, false, nil
	}}}

	_, _, _, _, err := rt.Route(deleteObjectReq("obj", ""))
	r.NoError(err)

	versions, err := versionSvc.GetObj(t.Context(), obj)
	r.NoError(err)
	r.NotEmpty(versions, "the delete threw away the record of a write that landed beside it")
}

func TestRouteDeleteObjectDropsTheVersionsItConfirmed(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	rt := replicatedRouter(t)
	c := routerRedis[t.Name()]
	versionSvc := meta.NewVersionService(c)
	obj := dom.Object{Bucket: testBucket, Name: "obj"}
	_, err := versionSvc.IncrementObj(t.Context(), obj, meta.ToDest(testSource, ""))
	r.NoError(err)

	_, _, _, _, err = rt.Route(deleteObjectReq("obj", ""))
	r.NoError(err)

	versions, err := versionSvc.GetObj(t.Context(), obj)
	r.NoError(err)
	r.Empty(versions, "the storage said it deleted the object and the worker will still go and ask")
}

// Nothing maps the source's version id to the copy the destination made.
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

func versionedDeleteObjectsReq(keys ...string) *http.Request {
	var b strings.Builder
	b.WriteString(`<Delete>`)
	for i, k := range keys {
		if i == len(keys)-1 {
			fmt.Fprintf(&b, `<Object><Key>%s</Key><VersionId>3HL4kqtGWpM</VersionId></Object>`, k)
			continue
		}
		fmt.Fprintf(&b, `<Object><Key>%s</Key></Object>`, k)
	}
	b.WriteString(`</Delete>`)
	body := b.String()

	req := httptest.NewRequest(http.MethodPost, "/"+testBucket+"?delete", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	ctx := req.Context()
	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetMethod(ctx, s3.DeleteObjects)
	return req.WithContext(ctx)
}

// The storage carries a batch out in one go, so one key that cannot be
// replicated cannot be held back once it is sent.
func TestRouteDeleteObjectsRefusesTheWholeBatch(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	var reached bool
	rt := replicatedRouter(t)
	// A batch let through fails on the assertions, not on a missing answer.
	rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
		reached = true
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`<DeleteResult><Deleted><Key>plain</Key></Deleted></DeleteResult>`)),
			Header:     http.Header{},
		}, false, nil
	}}}

	_, taskList, _, _, err := rt.Route(versionedDeleteObjectsReq("plain", "versioned"))

	r.ErrorIs(err, dom.ErrNotImplemented, "a batch naming a version was accepted")
	r.False(reached, "the batch reached the storage, so the versioned key can no longer be held back")
	r.Empty(taskList)
}

// The storage answers with the version id of the delete marker it just made,
// for keys the client named no version for. The keys come from the request, so
// nothing the storage says is taken for one the client named.
func TestRouteDeleteObjectsReplicatesWhenOnlyTheAnswerNamesVersions(t *testing.T) {
	r := require.New(t)
	versioningEnabled(t, true)

	rt := replicatedRouter(t)
	c := routerRedis[t.Name()]
	rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
		body := `<DeleteResult>` +
			`<Deleted><Key>one</Key><VersionId>3HL4kqtGWpM</VersionId></Deleted>` +
			`<Deleted><Key>two</Key><VersionId>QUpfdndhfd8</VersionId></Deleted>` +
			`</DeleteResult>`
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{},
		}, false, nil
	}}}

	_, _, _, _, err := rt.Route(deleteObjectsReq("one", "two"))
	r.NoError(err)

	r.ElementsMatch([]string{"one", "two"}, queuedDeletes(t, c),
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
	answering := func(t *testing.T, rt *router, body string) {
		t.Helper()
		rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     http.Header{},
			}, false, nil
		}}}
	}

	// The batch goes in one go, so there is no telling beforehand which keys
	// the storage will take.
	t.Run("Every key is written down before the batch is sent", func(t *testing.T) {
		r := require.New(t)
		versioningEnabled(t, true)

		rt := replicatedRouter(t)
		c := routerRedis[t.Name()]
		var writtenDown []string
		rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			intents, _ := meta.NewDeleteIntents(c).OwnedBy(t.Context(), testInstance)
			for _, intent := range intents {
				writtenDown = append(writtenDown, intent.Object.Name)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`<DeleteResult><Deleted><Key>gone</Key></Deleted></DeleteResult>`)),
				Header:     http.Header{},
			}, false, nil
		}}}

		_, taskList, _, _, err := rt.Route(deleteObjectsReq("gone", "refused"))
		r.NoError(err)
		r.Empty(taskList, "the keys were handed back to be queued a second time")
		r.ElementsMatch([]string{"gone", "refused"}, writtenDown,
			"the batch was sent before every one of its keys was written down")
	})

	t.Run("Only the keys the storage says it deleted are replicated", func(t *testing.T) {
		r := require.New(t)
		versioningEnabled(t, true)

		rt := replicatedRouter(t)
		c := routerRedis[t.Name()]
		answering(t, rt, `<DeleteResult>`+
			`<Deleted><Key>gone</Key></Deleted>`+
			`<Error><Key>refused</Key><Code>AccessDenied</Code></Error>`+
			`</DeleteResult>`)

		_, _, storage, isApiErr, err := rt.Route(deleteObjectsReq("gone", "refused"))
		r.NoError(err)
		r.False(isApiErr)
		r.Equal(testSource, storage)

		r.Equal([]string{"gone"}, queuedDeletes(t, c),
			"a key the storage refused was replicated as deleted")
		left, err := meta.NewDeleteIntents(c).OwnedBy(t.Context(), testInstance)
		r.NoError(err)
		r.Empty(left, "keys the storage refused still read as unfinished, so a restart would delete them")
	})

	t.Run("A storage that never answered puts every key up unconfirmed", func(t *testing.T) {
		r := require.New(t)
		versioningEnabled(t, true)

		rt := replicatedRouter(t)
		c := routerRedis[t.Name()]
		rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			return nil, false, fmt.Errorf("%w: connection reset", dom.ErrStorageAnswerLost)
		}}}

		_, _, _, _, err := rt.Route(deleteObjectsReq("one", "two"))
		r.ErrorIs(err, dom.ErrStorageAnswerLost)

		r.ElementsMatch([]string{"one", "two"}, queuedDeletes(t, c))
		left, err := meta.NewDeleteIntents(c).OwnedBy(t.Context(), testInstance)
		r.NoError(err)
		r.Empty(left, "the keys are queued and still read as unfinished")
	})

	t.Run("A client that left before the request was written changes nothing", func(t *testing.T) {
		r := require.New(t)
		versioningEnabled(t, true)

		rt := replicatedRouter(t)
		c := routerRedis[t.Name()]
		rt.clients = fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			return nil, false, fmt.Errorf("%w: context canceled", dom.ErrClientGone)
		}}}

		_, _, _, _, err := rt.Route(deleteObjectsReq("one", "two"))
		r.ErrorIs(err, dom.ErrClientGone)

		r.Empty(queuedDeletes(t, c), "nothing reached the storage, so there is nothing to replicate")
		left, err := meta.NewDeleteIntents(c).OwnedBy(t.Context(), testInstance)
		r.NoError(err)
		r.Empty(left, "a delete that never happened was left to be made again at startup")
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

	_, _, _, _, err := rt.Route(req.WithContext(ctx))
	r.NoError(err)
	r.True(sawResponseNeeded, "the delete did not wait for the storage's answer")
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
