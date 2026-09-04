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

package handler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_endInitialSync(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	c := testutil.SetupRedis(t)

	id := entity.NewReplicationStatusID("user", "src", "buck", "dst", "dst-buck")
	queueSvc := &tasks.QueueServiceMock{}
	tasks.Reset(queueSvc)
	policySvc := policy.NewService(c, queueSvc, nil)
	storageSvc := storage.New(c)
	r.NoError(policySvc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(id.User, id.FromBucket), id.FromStorage, false))
	_, err := policySvc.AddBucketReplicationPolicy(ctx, id, nil)
	r.NoError(err)
	r.NoError(storageSvc.SetMigratedObj(ctx, id, "copied"))

	s := &svc{queueSvc: queueSvc, policySvc: policySvc, storageSvc: storageSvc}

	queueSvc.InitReplicationInProgress(id)
	r.NoError(s.endInitialSync(ctx, id))
	status, err := policySvc.GetReplicationPolicyInfo(ctx, id)
	r.NoError(err)
	r.False(status.LiveSync, "copies are still waiting")
	migrated, err := storageSvc.IsMigratedObj(ctx, id, "copied")
	r.NoError(err)
	r.True(migrated, "and their records are still needed")

	queueSvc.InitReplicationDone(id)
	r.NoError(s.endInitialSync(ctx, id))
	status, err = policySvc.GetReplicationPolicyInfo(ctx, id)
	r.NoError(err)
	r.False(status.LiveSync, "the queue is empty, but the listing is not done")
	migrated, err = storageSvc.IsMigratedObj(ctx, id, "copied")
	r.NoError(err)
	r.True(migrated, "and their records are still needed")

	r.NoError(policySvc.ListingDone(ctx, id))
	r.NoError(s.endInitialSync(ctx, id))
	status, err = policySvc.GetReplicationPolicyInfo(ctx, id)
	r.NoError(err)
	r.True(status.LiveSync, "the last copy ends the initial sync")
	migrated, err = storageSvc.IsMigratedObj(ctx, id, "copied")
	r.NoError(err)
	r.False(migrated, "and the records go with it")
}
