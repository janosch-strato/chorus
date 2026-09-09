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

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleObjectSync(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.ObjectSyncPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectSyncPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Object.Bucket)
	ctx = log.WithObjName(ctx, p.Object.Name)
	logger := zerolog.Ctx(ctx)

	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}

	objectLockID := entity.NewVersionedObjectLockID(p.ToStorage, p.ToBucket, p.Object.Name, p.Object.Version)
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	objMeta, err := s.versionSvc.GetObj(ctx, p.Object)
	if err != nil {
		return err
	}
	// Empty version metadata is the other way to learn it: a delete replicated
	// before this task was handled took the metadata with it.
	isObjDeleted := p.Deleted || len(objMeta) == 0
	if isObjDeleted {
		return s.objectDelete(ctx, p, deleteIsConfirmed(p, objMeta))
	}

	destVersionKey := meta.ToDest(p.ToStorage, p.ToBucket)
	fromVer, toVer := objMeta[meta.ToDest(p.FromStorage, "")], objMeta[destVersionKey]
	if fromVer <= toVer {
		logger.Info().Int64("from_ver", fromVer).Int64("to_ver", toVer).Msg("object sync: identical from/to obj version: skip copy")
		return nil
	}

	fromBucket, toBucket := p.Object.Bucket, p.Object.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}
	err = lock.Do(ctx, time.Second*2, func() error {
		return s.rc.CopyTo(ctx, rclone.File{
			Storage: p.FromStorage,
			Bucket:  fromBucket,
			Name:    p.Object.Name,
		}, rclone.File{
			Storage: p.ToStorage,
			Bucket:  toBucket,
			Name:    p.Object.Name,
		}, p.ObjSize)
	})
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			logger.Warn().Msg("object sync: skip object sync: object missing in source")
			return nil
		}
		return err
	}
	logger.Info().Msg("object sync: done")

	if fromVer != 0 {
		return s.versionSvc.UpdateIfGreater(ctx, p.Object, destVersionKey, fromVer)
	}

	return nil
}

// sourceHolds reports whether the source storage still holds the object, and
// whether its answer said so at all. Only the storage naming the object or its
// bucket as gone is an answer; a storage that is down, rate limiting or
// refusing the credentials fails the same call without saying anything about
// the object, and must not be read as its absence.
func sourceHolds(statErr error) (holds, answered bool) {
	if statErr == nil {
		return true, true
	}
	switch mclient.ToErrorResponse(statErr).Code {
	case mclient.NoSuchKey, mclient.NoSuchBucket:
		return false, true
	}
	return false, false
}

// deleteIsConfirmed reports whether the task says by itself that the object is
// gone from the source storage, so that the source needs not be asked.
//
// It does when the storage confirmed the delete and its version metadata went
// with it: nothing has overtaken it since. Asking anyway would cost a stat of
// the source per deleted object, a thousand of them for one multi delete.
func deleteIsConfirmed(p tasks.ObjectSyncPayload, objMeta map[meta.Destination]int64) bool {
	return p.Deleted && !p.Unconfirmed && len(objMeta) == 0
}

// An unconfirmed task is only a reason to go and look, so there the source
// decides, and an unanswered question is retried rather than guessed.
func (s *svc) objectDelete(ctx context.Context, p tasks.ObjectSyncPayload, confirmed bool) (err error) {
	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return err
	}
	if !confirmed {
		_, statErr := fromClient.S3().StatObject(ctx, p.Object.Bucket, p.Object.Name, mclient.StatObjectOptions{})
		holds, answered := sourceHolds(statErr)
		if !answered {
			return fmt.Errorf("obj delete: source storage did not say whether it still holds the object: %w", statErr)
		}
		if holds {
			zerolog.Ctx(ctx).Warn().Msg("skip obj delete: obj still exists in source storage")
			return nil
		}
	}

	toBucket := p.Object.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}
	return toClient.S3().RemoveObject(ctx, toBucket, p.Object.Name, mclient.RemoveObjectOptions{VersionID: p.Object.Version})
}
