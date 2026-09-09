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

package replication

import (
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

// The object is still there, and its metadata is what tells the worker so.
func Test_svc_Replicate_versionedDeleteKeepsTheObject(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	c := testutil.SetupRedis(t)

	policySvc := policy.NewService(c, nil, nil)
	r.NoError(policySvc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, false))
	_, err := policySvc.AddBucketReplicationPolicy(ctx, entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket), nil)
	r.NoError(err)

	versionSvc := meta.NewVersionService(c)
	svc := New(asynq.NewClientFromRedisClient(c), versionSvc, policySvc)
	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetStorage(ctx, testSource)

	obj := dom.Object{Bucket: testBucket, Name: "o"}
	_, err = versionSvc.IncrementObj(ctx, obj, meta.ToDest(testSource, ""))
	r.NoError(err)
	r.NoError(versionSvc.UpdateIfGreater(ctx, obj, meta.ToDest(testDest, testBucket), 1))

	versioned := obj
	versioned.Version = "3HL4kqtGWpM"
	r.NoError(svc.Replicate(ctx, &tasks.ObjectSyncPayload{
		Object:  versioned,
		Sync:    tasks.Sync{FromStorage: testSource},
		Deleted: true,
	}))

	versions, err := versionSvc.GetObj(ctx, obj)
	r.NoError(err)
	r.NotEmpty(versions,
		"a delete of one version threw away the version metadata of the object, which is still there")
	r.EqualValues(1, versions[meta.ToDest(testSource, "")])
}

func deleteTask(object string, unconfirmed bool) *tasks.ObjectSyncPayload {
	return &tasks.ObjectSyncPayload{
		Object:      dom.Object{Bucket: testBucket, Name: object},
		Sync:        tasks.Sync{FromStorage: testSource},
		Deleted:     true,
		Unconfirmed: unconfirmed,
	}
}

func Test_svc_Replicate_deleteKeepsUnconfirmedVersions(t *testing.T) {
	ctx := t.Context()
	c := testutil.SetupRedis(t)

	policySvc := policy.NewService(c, nil, nil)
	require.NoError(t, policySvc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(testUser, testBucket), testSource, false))
	_, err := policySvc.AddBucketReplicationPolicy(ctx, entity.NewReplicationStatusID(testUser, testSource, testBucket, testDest, testBucket), nil)
	require.NoError(t, err)

	versionSvc := meta.NewVersionService(c)
	svc := New(asynq.NewClientFromRedisClient(c), versionSvc, policySvc)

	ctx = xctx.SetUser(ctx, testUser)
	ctx = xctx.SetBucket(ctx, testBucket)
	ctx = xctx.SetStorage(ctx, testSource)

	seed := func(t *testing.T, object string) {
		t.Helper()
		obj := dom.Object{Bucket: testBucket, Name: object}
		_, err := versionSvc.IncrementObj(ctx, obj, meta.ToDest(testSource, ""))
		require.NoError(t, err)
		require.NoError(t, versionSvc.UpdateIfGreater(ctx, obj, meta.ToDest(testDest, testBucket), 1))
		versions, err := versionSvc.GetObj(ctx, obj)
		require.NoError(t, err)
		require.NotEmpty(t, versions, "seed")
	}

	t.Run("A delete the storage confirmed drops the versions with the object", func(t *testing.T) {
		r := require.New(t)
		object := "confirmed"
		seed(t, object)

		r.NoError(svc.Replicate(ctx, deleteTask(object, false)))

		versions, err := versionSvc.GetObj(ctx, dom.Object{Bucket: testBucket, Name: object})
		r.NoError(err)
		r.Empty(versions, "a confirmed delete leaves no versions behind")
	})

	// The versions are what tells the worker whether the object was really
	// deleted, so a delete nobody confirmed must not destroy them.
	t.Run("A delete nobody confirmed keeps the versions", func(t *testing.T) {
		r := require.New(t)
		object := "unconfirmed"
		seed(t, object)

		r.NoError(svc.Replicate(ctx, deleteTask(object, true)))

		versions, err := versionSvc.GetObj(ctx, dom.Object{Bucket: testBucket, Name: object})
		r.NoError(err)
		r.NotEmpty(versions, "an unconfirmed delete threw away the versions it was meant to keep")
		r.EqualValues(1, versions[meta.ToDest(testSource, "")])
	})

	t.Run("Both are queued for the worker either way", func(t *testing.T) {
		r := require.New(t)
		inspector := asynq.NewInspectorFromRedisClient(c)
		queues, err := inspector.Queues()
		r.NoError(err)

		var queued int
		for _, queue := range queues {
			info, err := inspector.GetQueueInfo(queue)
			r.NoError(err)
			queued += info.Size
		}
		r.EqualValues(2, queued, "both deletes reach the worker")
	})
}
