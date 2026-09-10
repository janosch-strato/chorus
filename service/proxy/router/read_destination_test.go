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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/settings"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

const (
	testUser   = "user"
	testSource = "src"
	testDest   = "dst"
	testBucket = "buck"
)

func readReq(method s3.Method, object, target string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	ctx := req.Context()
	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetObject(ctx, object)
	ctx = xctx.SetMethod(ctx, method)
	return req.WithContext(ctx)
}

// setMigrated records the object as copied by the migration.
func setMigrated(t *testing.T, storageSvc storage.Service, object string) {
	id := entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket)
	require.NoError(t, storageSvc.SetMigratedObj(t.Context(), id, object))
}

// liveSync ends the initial sync of the replication the way the last copy of
// it does, and puts it back for the cases that follow.
func liveSync(t *testing.T, ctx context.Context, c redis.UniversalClient, policySvc policy.Service, id entity.ReplicationStatusID) {
	require.NoError(t, policySvc.LiveSyncStarted(ctx, id))
	key, err := store.NewReplicationStatusStore(c).MakeKey(id)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.HSet(ctx, key, "live_sync", false).Err()) })
}

func Test_router_destinationReadRoute(t *testing.T) {
	ctx := t.Context()
	c := testutil.SetupRedis(t)

	replID := entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket)
	policySvc := policy.NewService(c, nil, nil)
	require.NoError(t, policySvc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, false))
	_, err := policySvc.AddBucketReplicationPolicy(ctx, replID, nil)
	require.NoError(t, err)
	// the initial sync is running: the migrated records decide
	require.NoError(t, policySvc.ListingDone(ctx, replID))

	versionSvc := meta.NewVersionService(c)
	storageSvc := storage.New(c)
	require.NoError(t, settings.ReadFromDestination.Set(true))
	t.Cleanup(func() { require.NoError(t, settings.ReadFromDestination.Set(false)) })
	rt := &router{
		policySvc:  policySvc,
		versionSvc: versionSvc,
		storageSvc: storageSvc,
	}

	t.Run("migrated object is read from destination", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"
		setMigrated(t, storageSvc, object)

		destStorage, destBucket, ok := rt.destinationReadRoute(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.True(ok)
		r.Equal(testDest, destStorage)
		r.Equal(testBucket, destBucket)
	})

	t.Run("head of migrated object is read from destination", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"

		_, _, reason := rt.destinationReadDecision(readReq(s3.HeadObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Empty(reason)
	})

	t.Run("object without copy record is read from source", func(t *testing.T) {
		r := require.New(t)
		object := "not-listed-yet"

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Equal(skipNotCopied, reason)
	})

	t.Run("a deleted object is dropped and its deletion noted", func(t *testing.T) {
		r := require.New(t)
		object := "deleted"
		setMigrated(t, storageSvc, object)

		ctx := xctx.SetBucket(xctx.SetUser(t.Context(), testUser), testBucket)
		rt.objectDeleted(ctx, testSource, object)

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Equal(skipNotCopied, reason)

		id := entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket)
		pending, err := storageSvc.IsPendingDeleteObj(ctx, id, object)
		r.NoError(err)
		r.True(pending, "the destination holds it until the deletion is replicated")
	})

	t.Run("with the initial sync done everything is read from the destination", func(t *testing.T) {
		r := require.New(t)
		object := "never-recorded"
		liveSync(t, ctx, c, policySvc, replID)

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Empty(reason, "no record needed once everything is copied")
	})

	t.Run("with the initial sync done a pending delete is read from source", func(t *testing.T) {
		r := require.New(t)
		object := "deleted-in-live-sync"
		r.NoError(storageSvc.ObjectDeleted(ctx, replID, object))
		t.Cleanup(func() { r.NoError(storageSvc.DelPendingDeleteObj(ctx, replID, object)) })
		liveSync(t, ctx, c, policySvc, replID)

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Equal(skipPendingDelete, reason)
	})

	t.Run("listings are read from source", func(t *testing.T) {
		r := require.New(t)

		_, _, reason := rt.destinationReadDecision(readReq(s3.ListObjectsV2, "", "/"+testBucket), testSource, testUser, testBucket, false)
		r.Equal(skipNoObject, reason)
	})

	t.Run("object metadata is read from source", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObjectTagging, object, "/"+testBucket+"/"+object+"?tagging"), testSource, testUser, testBucket, false)
		r.Equal(skipMethod, reason)
	})

	t.Run("versioned read is read from source", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object+"?versionId=abc"), testSource, testUser, testBucket, false)
		r.Equal(skipVersionRequested, reason)
	})

	t.Run("no redirect while a switch is in progress", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, true)
		r.Equal(skipSwitchInProgress, reason)
	})

	t.Run("object written after the copy is read from source", func(t *testing.T) {
		r := require.New(t)
		object := "overwritten"
		setMigrated(t, storageSvc, object)
		obj := dom.Object{Bucket: testBucket, Name: object}
		version, err := versionSvc.IncrementObj(ctx, obj, meta.ToDest(testSource, ""))
		r.NoError(err)

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Equal(skipDestinationBehind, reason)

		// once the write has been replicated the destination may answer again
		r.NoError(versionSvc.UpdateIfGreater(ctx, obj, meta.ToDest(testDest, testBucket), version))
		_, _, reason = rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Empty(reason)
	})

	t.Run("disabled option reads from source", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"
		r.NoError(settings.ReadFromDestination.Set(false))
		defer func() { r.NoError(settings.ReadFromDestination.Set(true)) }()

		_, _, ok := rt.destinationReadRoute(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.False(ok)
	})

	t.Run("read of a bucket without migration is read from source", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"
		req := readReq(s3.GetObject, object, "/other/"+object)
		req = req.WithContext(xctx.SetBucket(req.Context(), "other"))

		_, _, reason := rt.destinationReadDecision(req, testSource, testUser, "other", false)
		r.Equal(skipNoReplication, reason)
	})

	t.Run("read from a storage that is not the migration source", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), "other-storage", testUser, testBucket, false)
		r.Equal(skipOtherSource, reason)
	})

	// keep last: adds a second destination to the bucket
	t.Run("bucket with several destinations is read from source", func(t *testing.T) {
		r := require.New(t)
		object := "migrated"
		_, err := policySvc.AddBucketReplicationPolicy(ctx, entity.NewReplicationStatusID(testUser, testSource, testBucket, "dst2", testBucket), nil)
		r.NoError(err)

		_, _, reason := rt.destinationReadDecision(readReq(s3.GetObject, object, "/"+testBucket+"/"+object), testSource, testUser, testBucket, false)
		r.Equal(skipManyDestinations, reason)
	})

}

func Test_rewriteBucket(t *testing.T) {
	tests := []struct {
		name         string
		host         string
		target       string
		fromBucket   string
		toBucket     string
		expectedHost string
		expectedPath string
		expectedRaw  string
		expectedErr  bool
	}{
		{
			name:         "path style",
			host:         "s3.example.com",
			target:       "/buck/dir/obj",
			fromBucket:   "buck",
			toBucket:     "other",
			expectedHost: "s3.example.com",
			expectedPath: "/other/dir/obj",
		},
		{
			name:         "path style keeps object encoding",
			host:         "s3.example.com",
			target:       "/buck/dir%2Fobj+1",
			fromBucket:   "buck",
			toBucket:     "other",
			expectedHost: "s3.example.com",
			expectedPath: "/other/dir/obj+1",
			expectedRaw:  "/other/dir%2Fobj+1",
		},
		{
			name:         "virtual host style",
			host:         "buck.s3.example.com",
			target:       "/dir/obj",
			fromBucket:   "buck",
			toBucket:     "other",
			expectedHost: "other.s3.example.com",
			expectedPath: "/dir/obj",
		},
		{
			name:         "same bucket is left alone",
			host:         "s3.example.com",
			target:       "/buck/dir/obj",
			fromBucket:   "buck",
			toBucket:     "buck",
			expectedHost: "s3.example.com",
			expectedPath: "/buck/dir/obj",
		},
		{
			name:        "bucket neither in host nor in path",
			host:        "s3.example.com",
			target:      "/dir/obj",
			fromBucket:  "buck",
			toBucket:    "other",
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			req := httptest.NewRequest(http.MethodGet, tc.target, nil)
			req.Host = tc.host

			err := rewriteBucket(req, tc.fromBucket, tc.toBucket)
			if tc.expectedErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			r.Equal(tc.expectedHost, req.Host)
			r.Equal(tc.expectedPath, req.URL.Path)
			r.Equal(tc.expectedRaw, req.URL.RawPath)
		})
	}
}

// A zero downtime switch owns the routing of its bucket. The route only moves
// once the destination holds a newer version of the object, so the switch has
// to be reported for its whole duration, not only when the route moved.
func Test_router_adjustObjReadRoute_reportsSwitchInProgress(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	c := testutil.SetupRedis(t)

	queueSvc := &tasks.QueueServiceMock{}
	tasks.Reset(queueSvc)
	policySvc := policy.NewService(c, queueSvc, nil)
	replID := entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket)
	r.NoError(policySvc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, true))
	_, err := policySvc.AddBucketReplicationPolicy(ctx, replID, nil)
	r.NoError(err)
	queueSvc.InitReplicationInProgress(replID)
	r.NoError(policySvc.ListingDone(ctx, replID))
	queueSvc.InitReplicationDone(replID)

	rt := &router{policySvc: policySvc, versionSvc: meta.NewVersionService(c)}
	req := readReq(s3.GetObject, "migrated", "/"+testBucket+"/migrated")

	storage, switchInProgress, err := rt.adjustObjReadRoute(req.Context(), testSource, testUser, testBucket)
	r.NoError(err)
	r.Equal(testSource, storage)
	r.False(switchInProgress)

	r.NoError(policySvc.AddZeroDowntimeReplicationSwitch(ctx, replID, &entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute}))

	// no object versions anywhere, so the switch leaves the route alone
	storage, switchInProgress, err = rt.adjustObjReadRoute(req.Context(), testSource, testUser, testBucket)
	r.NoError(err)
	r.Equal(testSource, storage)
	r.True(switchInProgress)
}

func TestWithAnswerTimeout(t *testing.T) {
	t.Run("A storage that does not begin to answer is given up on", func(t *testing.T) {
		r := require.New(t)

		answering := make(chan struct{})
		unblock := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(answering)
			<-unblock // a destination storage that took the read and is silent
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(unblock) })

		ctx, guard := withAnswerTimeout(t.Context(), 200*time.Millisecond)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/buck/obj", nil)
		r.NoError(err)

		start := time.Now()
		resp, err := srv.Client().Do(req) //nolint:bodyclose // there is no response
		if resp != nil {
			_ = resp.Body.Close()
		}
		<-answering
		r.ErrorIs(err, context.Canceled)
		r.True(guard.ranOut(), "the read was not given up on for want of an answer")
		r.Less(time.Since(start), 5*time.Second, "the read waited longer than the timeout")
	})

	t.Run("A storage that answers keeps the connection for the answer", func(t *testing.T) {
		r := require.New(t)

		body := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-body
			_, _ = w.Write([]byte("object"))
		}))
		t.Cleanup(srv.Close)

		ctx, guard := withAnswerTimeout(t.Context(), 200*time.Millisecond)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/buck/obj", nil)
		r.NoError(err)

		resp, err := srv.Client().Do(req)
		r.NoError(err)
		defer resp.Body.Close()

		time.AfterFunc(500*time.Millisecond, func() { close(body) })
		read, err := io.ReadAll(resp.Body)
		r.NoError(err, "reading the answer ran into the timeout")
		r.EqualValues("object", string(read))
		r.False(guard.ranOut())
	})

	t.Run("Getting a connection is part of the wait", func(t *testing.T) {
		r := require.New(t)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		ctx, guard := withAnswerTimeout(t.Context(), 50*time.Millisecond)
		// As it would while the connection is still being established.
		time.Sleep(100 * time.Millisecond)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/buck/obj", nil)
		r.NoError(err)

		resp, err := srv.Client().Do(req) //nolint:bodyclose // there is no response
		if resp != nil {
			_ = resp.Body.Close()
		}
		r.ErrorIs(err, context.Canceled)
		r.True(guard.ranOut())
	})

	t.Run("A timeout that is off waits as any other read", func(t *testing.T) {
		r := require.New(t)

		for _, timeout := range []time.Duration{0, -1} {
			ctx, guard := withAnswerTimeout(t.Context(), timeout)
			r.Nil(httptrace.ContextClientTrace(ctx), "an unbounded read needs no hooks")
			r.False(guard.ranOut())
		}
	})

	t.Run("An answer and the wait running out cannot both take effect", func(t *testing.T) {
		r := require.New(t)

		answeredFirst := &answerGuard{}
		r.True(answeredFirst.answered())
		r.False(answeredFirst.expire(), "the wait took away an answer that had arrived")
		r.False(answeredFirst.ranOut())

		expiredFirst := &answerGuard{}
		r.True(expiredFirst.expire())
		r.False(expiredFirst.answered(), "an answer overrode a read already given up on")
		r.True(expiredFirst.ranOut())
	})

	t.Run("A read that was answered is not cancelled by its own wait", func(t *testing.T) {
		r := require.New(t)

		ctx, guard := withAnswerTimeout(context.Background(), 50*time.Millisecond)
		httptrace.ContextClientTrace(ctx).GotFirstResponseByte()

		time.Sleep(200 * time.Millisecond)
		r.NoError(ctx.Err(), "the answer was cancelled after it had arrived")
		r.False(guard.ranOut())
	})

	// What must never happen is both winning.
	t.Run("An answer racing the wait settles on one of the two", func(t *testing.T) {
		r := require.New(t)

		for range 300 {
			ctx, guard := withAnswerTimeout(context.Background(), time.Millisecond)
			trace := httptrace.ContextClientTrace(ctx)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Millisecond)
				trace.GotFirstResponseByte()
			}()
			wg.Wait()
			time.Sleep(2 * time.Millisecond) // let a cancel that won land

			if !guard.ranOut() {
				r.NoError(ctx.Err(), "a read that was not given up on was cancelled anyway")
			}
		}
	})

	t.Run("A read that is given up on names the destination storage", func(t *testing.T) {
		r := require.New(t)

		err := fmt.Errorf("%w: %w", errDestinationSlow, context.Canceled)
		r.ErrorIs(err, errDestinationSlow)
		r.ErrorIs(err, context.Canceled)
	})
}

// The wait runs out and the destination answers all the same: the fake storage
// ignores the context, which makes that race repeatable.

// answerTimeout sets the wait for the test and puts it back afterwards.
func answerTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := settings.DestinationAnswerTimeout.Get()
	require.NoError(t, settings.DestinationAnswerTimeout.Set(d.Milliseconds()))
	t.Cleanup(func() { require.NoError(t, settings.DestinationAnswerTimeout.Set(prev)) })
}

func TestReadDestinationGivenUpOn(t *testing.T) {
	r := require.New(t)
	answerTimeout(t, 20*time.Millisecond)

	answered := make(chan struct{})
	rt := &router{
		clients: fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			time.Sleep(100 * time.Millisecond)
			close(answered)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("late")),
				Header:     http.Header{},
			}, false, nil
		}}},
	}

	// HeadObject: only GetObject consults the rate limiter.
	req := readReq(s3.HeadObject, "obj", "/"+testBucket+"/obj")
	resp, isApiErr, err := rt.readDestination(req, testDest, testBucket)
	if resp != nil {
		_ = resp.Body.Close()
	}

	<-answered
	r.False(isApiErr)
	r.ErrorIs(err, errDestinationSlow, "a read that was given up on was reported as answered")
}

// The storage clients never return one, but the proxy builds them itself, see
// the head bucket cache.
func TestCommonReadFallsBackOnABodilessAnswer(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()

	c := testutil.SetupRedis(t)
	policySvc := policy.NewService(c, nil, nil)
	r.NoError(policySvc.AddBucketRoutingPolicy(ctx,
		entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, false))
	replID := entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket)
	_, err := policySvc.AddBucketReplicationPolicy(ctx, replID, nil)
	r.NoError(err)
	r.NoError(policySvc.ListingDone(ctx, replID))

	storageSvc := storage.New(c)
	setMigrated(t, storageSvc, "obj") // so the destination is asked at all
	r.NoError(settings.ReadFromDestination.Set(true))
	t.Cleanup(func() { r.NoError(settings.ReadFromDestination.Set(false)) })

	var asked int
	rt := &router{
		clients: fakeClients{c: &fakeClient{do: func(*http.Request) (*http.Response, bool, error) {
			asked++
			// An api error, so the caller discards it - and no body to discard.
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}}, true, nil
		}}},
		policySvc:  policySvc,
		versionSvc: meta.NewVersionService(c),
		storageSvc: storageSvc,
	}

	r.NotPanics(func() {
		_, _, _, _ = rt.commonRead(readReq(s3.HeadObject, "obj", "/"+testBucket+"/obj"))
	}, "an answer without a body was reached into on the way to the source")
	r.Equal(2, asked, "the read did not fall back to the source storage")
}
