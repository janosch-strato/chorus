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
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/settings"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleMigrationObjCopy(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.MigrateObjCopyPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("MigrateObjCopyPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	ctx = log.WithObjName(ctx, p.Obj.Name)
	logger := zerolog.Ctx(ctx)

	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}

	domObj := dom.Object{
		Bucket:  p.Bucket,
		Name:    p.Obj.Name,
		Version: p.Obj.VersionID,
	}

	objectLockID := entity.NewVersionedObjectLockID(p.ToStorage, p.ToBucket, p.Obj.Name, p.Obj.VersionID)
	lockStart := time.Now()
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	lockDuration := time.Since(lockStart)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	objMeta, err := s.versionSvc.GetObj(ctx, domObj)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get obj meta: %w", err)
	}
	destVersionKey := meta.ToDest(p.ToStorage, p.ToBucket)
	fromVer, toVer := objMeta[meta.ToDest(p.FromStorage, "")], objMeta[destVersionKey]

	if fromVer != 0 && fromVer <= toVer {
		logger.Info().Int64("from_ver", fromVer).Int64("to_ver", toVer).Msg("migration obj copy: identical from/to obj version: skip copy")
		return nil
	}
	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}
	// 1. sync obj meta and content
	copyStart := time.Now()
	err = lock.Do(ctx, time.Second*2, func() error {
		return s.rc.CopyTo(ctx, rclone.File{
			Storage: p.FromStorage,
			Bucket:  fromBucket,
			Name:    p.Obj.Name,
		}, rclone.File{
			Storage: p.ToStorage,
			Bucket:  toBucket,
			Name:    p.Obj.Name,
		}, p.Obj.Size)
	})
	copyDuration := time.Since(copyStart)
	metrics.MigrationCopyPhase("copy", p.FromStorage, p.ToStorage, copyDuration)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			logger.Info().Msg("migration obj copy: skip object sync: object missing in source")
			return nil
		}
		return fmt.Errorf("migration obj copy: unable to copy with rclone: %w", err)
	}

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get %q s3 client: %w: %w", p.FromStorage, err, asynq.SkipRetry)
	}

	// 2. sync obj ACL
	aclStart := time.Now()
	err = s.syncObjectACL(ctx, fromClient, toClient, p.Bucket, p.Obj.Name, p.ToBucket)
	aclDuration := time.Since(aclStart)
	metrics.MigrationCopyPhase("acl", p.FromStorage, p.ToStorage, aclDuration)
	if err != nil {
		return err
	}

	// 3. sync obj tags
	tagsStart := time.Now()
	err = s.syncObjectTagging(ctx, fromClient, toClient, p.Bucket, p.Obj.Name, p.ToBucket)
	tagsDuration := time.Since(tagsStart)
	metrics.MigrationCopyPhase("tags", p.FromStorage, p.ToStorage, tagsDuration)
	if err != nil {
		return err
	}

	if fromVer != 0 {
		err = s.versionSvc.UpdateIfGreater(ctx, domObj, destVersionKey, fromVer)
		if err != nil {
			return fmt.Errorf("migration obj copy: unable to update obj meta: %w", err)
		}
	}
	// The record the proxy reads to serve this object from the destination
	// while the readFromDestination mode is on. Nothing else reads it, so
	// without that mode there is nothing to record. Versioned copies are not
	// served from the destination either.
	if settings.ReadFromDestination.Get() && p.Obj.VersionID == "" {
		replicationID := entity.ReplicationStatusID{
			User:        xctx.GetUser(ctx),
			FromStorage: p.FromStorage,
			FromBucket:  p.Bucket,
			ToStorage:   p.ToStorage,
			ToBucket:    p.ToBucket,
		}
		if err = s.storageSvc.SetMigratedObj(ctx, replicationID, p.Obj.Name); err != nil {
			return fmt.Errorf("migration obj copy: unable to record the copied object: %w", err)
		}
		if err = s.endInitialSync(ctx, replicationID); err != nil {
			return err
		}
	}
	logger.Info().
		Dur("lock_duration", lockDuration).
		Dur("copy_duration", copyDuration).
		Dur("acl_duration", aclDuration).
		Dur("tags_duration", tagsDuration).
		Int64("obj_size", p.Obj.Size).
		Msg("migration obj copy: done")

	return nil
}

// endInitialSync ends the initial sync of the migration if this copy was the
// last one waiting for it. From then on the destination holds everything the
// listing found, so the proxy reads it from there without asking for the per
// object records, and the records go.
//
// Copies still running are not waited for. A read of one of those objects
// finds it missing on the destination and falls back to the source, which is
// what the fallback is for, and there are only as many of them as the worker
// runs at once.
func (s *svc) endInitialSync(ctx context.Context, replicationID entity.ReplicationStatusID) error {
	empty, err := s.queueSvc.QueuedEmpty(ctx, tasks.MigrateObjCopyQueue(replicationID))
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to check the copy queue: %w", err)
	}
	if !empty {
		return nil
	}
	if err = s.policySvc.LiveSyncStarted(ctx, replicationID); err != nil {
		return fmt.Errorf("migration obj copy: unable to start live sync: %w", err)
	}
	if err = s.storageSvc.DelAllMigratedObjs(ctx, replicationID); err != nil {
		return fmt.Errorf("migration obj copy: unable to drop the migrated object records: %w", err)
	}
	zerolog.Ctx(ctx).Info().Msg("migration obj copy: initial sync done, reading everything from the destination")
	return nil
}
