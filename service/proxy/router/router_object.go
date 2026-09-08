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
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
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

func (r *router) deleteObjects(req *http.Request) (resp *http.Response, taskList []tasks.SyncTask, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
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
	reqBody := deleteObjectsRequest{}
	err = s3client.ExtractReqBody(req, &reqBody)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to unmarshal deleteObjectsRequest request body")
		return
	}

	// The storage carries a batch out in one go, so one key that cannot be
	// replicated cannot be held back once it is sent.
	if slices.ContainsFunc(reqBody.Objects, func(o objectID) bool { return o.VersionID != "" }) {
		return nil, nil, storage, false, errDeleteVersionRefused
	}

	resp, isApiErr, err = client.Do(req)
	if isApiErr || !dom.StorageMayHaveActed(err) {
		return
	}
	if err != nil {
		// Without the answer it is unknown which deletions were carried out.
		// The worker leaves the objects alone that are still on the source.
		for _, object := range reqBody.Objects {
			taskList = append(taskList, &tasks.ObjectSyncPayload{
				Object:  object.toDom(bucket),
				Sync:    tasks.Sync{FromStorage: storage},
				Deleted: true,
			})
		}
		return
	}

	respBody := multiDeleteResult{}
	if xmlErr := s3client.ExtractRespBody(resp, &respBody); xmlErr != nil {
		zerolog.Ctx(ctx).Err(xmlErr).Msg("unable to unmarshal multiDeleteResult response body")
		err = xmlErr
		return
	}

	if reqBody.Quiet {
		// quiet mode: filter request objects by response errors
		errSet := make(map[string]struct{}, len(respBody.Error))
		for _, delErr := range respBody.Error {
			errSet[delErr.Key] = struct{}{}
		}
		for _, object := range reqBody.Objects {
			if _, ok := errSet[object.Key]; ok {
				continue
			}
			taskList = append(taskList, &tasks.ObjectSyncPayload{
				Object:  object.toDom(bucket),
				Sync:    tasks.Sync{FromStorage: storage},
				Deleted: true,
			})
		}
	} else {
		// normal mode: use deleted obj list from response
		taskList = make([]tasks.SyncTask, len(respBody.Deleted))
		for i, object := range respBody.Deleted {
			taskList[i] = &tasks.ObjectSyncPayload{
				Object:  object.toDom(bucket),
				Sync:    tasks.Sync{FromStorage: storage},
				Deleted: true,
			}
		}
	}
	return
}
