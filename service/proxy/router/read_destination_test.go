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
	require.NoError(t, policySvc.ObjListStarted(ctx, replID))

	versionSvc := meta.NewVersionService(c)
	storageSvc := storage.New(c)
	rt := &router{
		policySvc:           policySvc,
		versionSvc:          versionSvc,
		storageSvc:          storageSvc,
		readFromDestination: true,
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
		r.NoError(storageSvc.SetPendingDeleteObj(ctx, replID, object))
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
		rt.readFromDestination = false
		defer func() { rt.readFromDestination = true }()

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
	r.NoError(policySvc.ObjListStarted(ctx, replID))
	queueSvc.InitReplicationDone(replID)

	rt := &router{policySvc: policySvc, versionSvc: meta.NewVersionService(c), readFromDestination: true}
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
