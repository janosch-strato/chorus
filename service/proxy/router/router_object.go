/*
 * Copyright © 2024 Clyso GmbH
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
	"errors"
	"fmt"
	"net/http"
	"slices"

	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/xid"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

func (r *router) putObject(req *http.Request) (resp *http.Response, taskList []tasks.SyncTask, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)
	routingPolicyID := entity.NewBucketRoutingPolicyID(user, bucket)
	storage, err = r.policySvc.GetRoutingPolicy(ctx, routingPolicyID)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil, nil, "", false, fmt.Errorf("%w: routing policy not configured: %w", dom.ErrPolicy, err)
		}
		return nil, nil, "", false, err
	}
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	if isApiErr || !dom.StorageMayHaveActed(err) {
		return
	}
	if resp != nil {
		zerolog.Ctx(ctx).Info().Str("etag", resp.Header.Get("ETag")).Msg("uploaded object with etag")
	}

	obj := dom.Object{Bucket: bucket, Name: object}
	// The upload is on the storage whether or not the client stayed for the
	// answer, see cancelUntilWritten, so asking after it must not depend on
	// that client either. Without a size the copy is later admitted to the
	// worker's memory budget as though it were empty.
	statCtx := context.WithoutCancel(ctx)
	var objSize int64
	objInfo, statErr := client.S3().StatObject(statCtx, bucket, object, mclient.StatObjectOptions{})
	if statErr != nil {
		zerolog.Ctx(ctx).Err(statErr).Msg("unable to get uploaded object size")
	} else {
		objSize = objInfo.Size
	}

	taskList = append(taskList, &tasks.ObjectSyncPayload{
		Object:  obj,
		Sync:    tasks.Sync{FromStorage: storage},
		ObjSize: objSize,
	})
	if hasACLChanged(req) {
		taskList = append(taskList, &tasks.ObjSyncACLPayload{
			Object: obj,
			Sync:   tasks.Sync{FromStorage: storage},
		})
	}
	if hasTagsChanged(req) {
		taskList = append(taskList, &tasks.ObjSyncTagsPayload{
			Object: obj,
			Sync:   tasks.Sync{FromStorage: storage},
		})
	}

	return
}

// deleteObject carries out a delete of one object, writing it down first and
// taking that back once its replication is queued. A delete sent and not
// written down is the one loss nothing can find afterwards, see
// meta.DeleteIntents.
func (r *router) deleteObject(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)
	storage, err = r.policySvc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(user, bucket))
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil, "", false, fmt.Errorf("%w: routing policy not configured: %w", dom.ErrPolicy, err)
		}
		return nil, "", false, err
	}
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, "", false, err
	}

	// The delete goes to the storage whether or not the client stays, see
	// cancelUntilWritten.
	recordCtx := context.WithoutCancel(ctx)
	intents, err := r.intendToDelete(recordCtx, user, storage, []dom.Object{{Bucket: bucket, Name: object}})
	if err != nil {
		return nil, storage, false, err
	}

	resp, isApiErr, err = client.Do(req)
	if isApiErr || !dom.StorageMayHaveActed(err) {
		// Refused, or it never reached the storage: nothing happened.
		r.deleteIntentsDone(recordCtx, intents)
		return resp, storage, isApiErr, err
	}

	// The destination keeps the object until the delete is replicated, so
	// reads of it belong on the source.
	r.objectDeleted(recordCtx, storage, object)
	r.queueDeletes(recordCtx, intents, err != nil)
	return resp, storage, isApiErr, err
}

// The versions are recorded so that a proxy finishing the job later can tell a
// write that landed since from the object it was asked to delete.
func (r *router) intendToDelete(ctx context.Context, user, storage string, objs []dom.Object) ([]meta.DeleteIntent, error) {
	request := xid.New().String()
	// In one go: a multi delete names up to a thousand objects, and the client
	// is waiting on every round trip made before the storage is even asked.
	versions, err := r.versionSvc.GetObjMany(ctx, objs)
	if err != nil {
		return nil, fmt.Errorf("delete: unable to read the object versions: %w", err)
	}
	intents := make([]meta.DeleteIntent, 0, len(objs))
	for i, obj := range objs {
		intents = append(intents, meta.DeleteIntent{
			Request: request, Owner: r.instanceID, User: user, Storage: storage,
			Object: obj, Versions: versions[i],
		})
	}
	if err := r.deleteIntents.Record(ctx, intents); err != nil {
		return nil, fmt.Errorf("delete: unable to write down the delete before making it: %w", err)
	}
	return intents, nil
}

// An intent left behind is finished by the next proxy to start.
//
// A confirmed delete also takes the object's versions with it, which is what
// tells the worker it needs to ask nobody - but only while they still hold what
// the intent read. A write that landed while the delete was in flight put its
// own versions there, and those record that write, not this delete.
func (r *router) queueDeletes(ctx context.Context, intents []meta.DeleteIntent, unconfirmed bool) {
	done := make([]meta.DeleteIntent, 0, len(intents))
	for _, intent := range intents {
		if !unconfirmed {
			dropped, err := r.versionSvc.DeleteObjIfUnchanged(ctx, intent.Object, intent.Versions)
			switch {
			case err != nil:
				zerolog.Ctx(ctx).Err(err).Str(log.Object, intent.Object.Name).
					Msg("delete: unable to drop the object versions")
			case !dropped:
				zerolog.Ctx(ctx).Info().Str(log.Object, intent.Object.Name).
					Msg("delete: object versions changed while it was being deleted: left in place")
			}
		}
		if err := r.replSvc.Replicate(ctx, &tasks.ObjectSyncPayload{
			Object:      intent.Object,
			Sync:        tasks.Sync{FromStorage: intent.Storage},
			Deleted:     true,
			Unconfirmed: unconfirmed,
		}); err != nil {
			zerolog.Ctx(ctx).Err(err).Str(log.Object, intent.Object.Name).
				Msg("delete: unable to queue the replication, leaving it to be finished at startup")
			continue
		}
		done = append(done, intent)
	}
	r.deleteIntentsDone(ctx, done)
}

func (r *router) deleteIntentsDone(ctx context.Context, intents []meta.DeleteIntent) {
	if err := r.deleteIntents.Done(ctx, intents); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("delete: unable to take back the record of a delete that is finished")
	}
}

// Every key is written down before the batch is sent, as in deleteObject.
func (r *router) deleteObjects(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	routingPolicyID := entity.NewBucketRoutingPolicyID(user, bucket)
	storage, err = r.policySvc.GetRoutingPolicy(ctx, routingPolicyID)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil, "", false, fmt.Errorf("%w: routing policy not configured: %w", dom.ErrPolicy, err)
		}
		return nil, "", false, err
	}
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, "", false, err
	}
	reqBody := deleteObjectsRequest{}
	if err = s3client.ExtractReqBody(req, &reqBody); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to unmarshal deleteObjectsRequest request body")
		return nil, storage, false, err
	}

	// One such key refuses the batch: the storage carries a multi delete out
	// in one go, so there is no holding a single key back.
	if slices.ContainsFunc(reqBody.Objects, func(o objectID) bool { return o.VersionID != "" }) {
		return nil, storage, false, errDeleteVersionRefused
	}

	recordCtx := context.WithoutCancel(ctx)
	objs := make([]dom.Object, 0, len(reqBody.Objects))
	for _, object := range reqBody.Objects {
		objs = append(objs, object.toDom(bucket))
	}
	intents, err := r.intendToDelete(recordCtx, user, storage, objs)
	if err != nil {
		return nil, storage, false, err
	}

	resp, isApiErr, err = client.Do(req)
	if isApiErr || !dom.StorageMayHaveActed(err) {
		r.deleteIntentsDone(recordCtx, intents)
		return resp, storage, isApiErr, err
	}
	if err != nil {
		// Which of the keys went is unknown, so every one is put up to be
		// looked at and none may still count as migrated.
		for _, intent := range intents {
			r.objectDeleted(recordCtx, storage, intent.Object.Name)
		}
		r.queueDeletes(recordCtx, intents, true)
		return resp, storage, isApiErr, err
	}

	respBody := multiDeleteResult{}
	if xmlErr := s3client.ExtractRespBody(resp, &respBody); xmlErr != nil {
		zerolog.Ctx(ctx).Err(xmlErr).Msg("unable to unmarshal multiDeleteResult response body")
		// An unreadable answer says as little as no answer at all.
		r.queueDeletes(recordCtx, intents, true)
		return resp, storage, isApiErr, xmlErr
	}

	deleted := deletedKeysOf(reqBody, respBody)
	went := make([]meta.DeleteIntent, 0, len(deleted))
	kept := make([]meta.DeleteIntent, 0, len(intents))
	for _, intent := range intents {
		if slices.Contains(deleted, intent.Object.Name) {
			went = append(went, intent)
			// The destination has not caught up yet.
			r.objectDeleted(recordCtx, storage, intent.Object.Name)
			continue
		}
		kept = append(kept, intent)
	}
	// The storage named what it refused, so those keys were not deleted.
	r.deleteIntentsDone(recordCtx, kept)
	r.queueDeletes(recordCtx, went, false)
	return resp, storage, isApiErr, err
}

// A quiet batch is answered with the failures only, so the rest of what was
// asked for is what went.
func deletedKeysOf(reqBody deleteObjectsRequest, respBody multiDeleteResult) []string {
	if !reqBody.Quiet {
		keys := make([]string, 0, len(respBody.Deleted))
		for _, object := range respBody.Deleted {
			keys = append(keys, object.Key)
		}
		return keys
	}
	refused := make(map[string]struct{}, len(respBody.Error))
	for _, delErr := range respBody.Error {
		refused[delErr.Key] = struct{}{}
	}
	keys := make([]string, 0, len(reqBody.Objects))
	for _, object := range reqBody.Objects {
		if _, ok := refused[object.Key]; ok {
			continue
		}
		keys = append(keys, object.Key)
	}
	return keys
}
