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

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/entity"

	// "github.com/clyso/chorus/pkg/features"

	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/settings"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	// At listing speed auto the listing stops while this many copy tasks are
	// queued and continues listingRecheckDelay later. A fixed depth throttles
	// the listing to the speed of the copies by itself: it only ever refills
	// what the copies took out. The two together bound the copy rate the
	// listing can keep up with, at listingQueueLimit / listingRecheckDelay.
	listingQueueLimit   = 20000
	listingCheckEvery   = 1000
	listingRecheckDelay = 10 * time.Second
)

func (s *svc) HandleMigrationBucketListObj(ctx context.Context, t *asynq.Task) error {
	// todo: aggregate task to not list multiple times
	var p tasks.MigrateBucketListObjectsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("HandleMigrationBucketListObj Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)

	replicationID := entity.ReplicationStatusID{
		User:        xctx.GetUser(ctx),
		FromStorage: p.FromStorage,
		FromBucket:  p.Bucket,
		ToStorage:   p.ToStorage,
		ToBucket:    p.ToBucket,
	}

	if err := s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}

	fromClient, err := s.clients.GetByName(ctx, p.FromStorage)
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to get %q s3 client: %w: %w", p.FromStorage, err, asynq.SkipRetry)
	}

	lastObjName, err := s.storageSvc.GetLastListedObj(ctx, p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket)
	if err != nil {
		return err
	}

	// Nothing to list while the copies are behind. Checked before the storage
	// is asked for anything, so a listing that comes back to a queue that is
	// still full costs one lookup.
	copyQueue := tasks.MigrateObjCopyQueue(replicationID)
	if queued, park := s.listingMustPark(ctx, copyQueue, logger); park {
		return s.parkListing(ctx, replicationID, p, queued, logger)
	}

	// One listing for the whole bucket. A bucket has no directories to walk
	// into: what looks like one is an object whose name ends in a slash, and
	// it is copied like any other.
	objects := fromClient.S3().ListObjects(ctx, p.Bucket, mclient.ListObjectsOptions{
		StartAfter: lastObjName,
		Recursive:  true,
	})
	listed := 0
	for object := range objects {
		// The count covers retried tasks too, so a listing also stops when the
		// copies are failing. It resumes from the stored cursor, so it can
		// stop anywhere.
		listed++
		if listed%listingCheckEvery == 0 {
			if queued, park := s.listingMustPark(ctx, copyQueue, logger); park {
				return s.parkListing(ctx, replicationID, p, queued, logger)
			}
		}
		if object.Err != nil {
			return fmt.Errorf("migration bucket list obj: list objects error %w", object.Err)
		}
		logger.Debug().Str(log.Object, object.Key).Str("obj_version_id", object.VersionID).
			Msg("migration bucket list obj: start processing object from the list")
		p.Sync.InitDate()

		var task *asynq.Task
		if p.Versioned {
			task, err = tasks.NewReplicationTask(ctx, replicationID, tasks.ListObjectVersionsPayload{
				Sync:   p.Sync,
				Bucket: p.Bucket,
				Prefix: object.Key,
			})
			if err != nil {
				return fmt.Errorf("unable to create list object versions task: %w", err)
			}
		} else {
			task, err = tasks.NewReplicationTask(ctx, replicationID, tasks.MigrateObjCopyPayload{
				Sync:   p.Sync,
				Bucket: p.Bucket,
				Obj: tasks.ObjPayload{
					Name:        object.Key,
					VersionID:   object.VersionID,
					ETag:        object.ETag,
					Size:        object.Size,
					ContentType: object.ContentType,
				},
			})
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to create copy obj task: %w", err)
			}
		}
		if _, err = s.taskClient.EnqueueContext(ctx, task); err != nil {
			if !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
				return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
			}
			logger.Info().Msg("cannot enqueue task with duplicate id")
		}
		// the cursor moves for a duplicate as well, or a listing resumed into
		// objects it has already queued would not get past them
		if err = s.storageSvc.SetLastListedObj(ctx, p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, object.Key); err != nil {
			return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
		}
	}
	_ = s.storageSvc.DelLastListedObj(ctx, p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket)

	if err = s.policySvc.ObjListStarted(ctx, replicationID); err != nil {
		logger.Err(err).Msg("migration bucket list obj: unable to set ObjListStarted")
	}

	logger.Info().Msg("migration bucket list obj: done")
	return nil
}

// listingMustPark reports the depth of the copy queue and whether the listing
// has to stop for it.
func (s *svc) listingMustPark(ctx context.Context, copyQueue string, logger *zerolog.Logger) (int, bool) {
	if settings.ListingSpeed.Get() != settings.ListingAuto {
		return 0, false
	}
	queued, err := s.queueSvc.UnprocessedCount(ctx, true, copyQueue)
	if err != nil {
		logger.Err(err).Msg("migration bucket list obj: unable to check the copy queue")
		return 0, false
	}
	return queued, queued >= listingQueueLimit
}

// parkListing stops the listing and queues its continuation listingRecheckDelay
// later. It resumes from the stored cursor, so the task is the only state the
// pause needs.
//
// The continuation carries the number of stops it took to get here, and its id
// with it. Naming it after the cursor instead would name the running task
// whenever the listing parks without having listed anything, and asynq refuses
// that id as a duplicate of a task that is still there.
func (s *svc) parkListing(ctx context.Context, replicationID entity.ReplicationStatusID,
	p tasks.MigrateBucketListObjectsPayload, queued int, logger *zerolog.Logger,
) error {
	p.Parked++
	id := fmt.Sprintf("%s:%d", tasks.MigrateBucketListObjectsTaskID(p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket), p.Parked)
	task, err := tasks.NewReplicationTask(ctx, replicationID, p, asynq.TaskID(id), asynq.ProcessIn(listingRecheckDelay))
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to create continuation task: %w", err)
	}
	logger.Info().Int("queued", queued).Int("parked", p.Parked).
		Msg("migration bucket list obj: copy queue is full, pausing the listing")
	if _, err = s.taskClient.EnqueueContext(ctx, task); err != nil {
		return fmt.Errorf("migration bucket list obj: unable to enqueue continuation task: %w", err)
	}
	return nil
}
