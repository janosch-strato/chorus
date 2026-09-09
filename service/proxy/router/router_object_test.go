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
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
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

	keys := deletedKeys(t, taskList)
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
