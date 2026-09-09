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
