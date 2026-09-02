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
	"hash/crc32"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/entity"

	// "github.com/clyso/chorus/pkg/features"

	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/switches"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	// At listing speed auto the listing stops while this many copy tasks are
	// queued, and continues listingRecheckDelay later. A fixed depth throttles
	// the listing to the speed of the copies by itself: it only ever refills
	// what the copies took out.
	//
	// Both together bound the copy rate the listing can keep up with, at
	// listingQueueLimit / listingRecheckDelay, here 2000 objects per second,
	// which is an order of magnitude above what a migration has reached so
	// far. Buying that headroom with a deeper queue rather than with a shorter
	// delay keeps the polling of a parked listing rare: the queued tasks cost
	// about 1.6 KB of redis each, so the depth is worth little more than the
	// memory it holds.
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

	lastObjName, err := s.storageSvc.GetLastListedObj(ctx, p)
	if err != nil {
		return err
	}

	copyQueue := tasks.MigrateObjCopyQueue(replicationID)
	objects := fromClient.S3().ListObjects(ctx, p.Bucket, mclient.ListObjectsOptions{StartAfter: lastObjName, Prefix: p.Prefix})
	objectsNum := 0
	listed := 0
	for object := range objects {
		// Give the copies room to catch up instead of queueing millions of
		// objects they cannot get to for days. The count covers retried tasks
		// too, so a listing also stops when the copies are failing. The
		// listing resumes from the stored cursor, so it can stop anywhere.
		//
		// The first object of a run is checked as well, so that a listing
		// resumed into a queue that is still full parks again at once instead
		// of adding another listingCheckEvery objects to it. It has to list
		// that one object first: a continuation is identified by the cursor it
		// starts from, and a cursor that has not moved names the running task
		// itself, whose id is still taken.
		listed++
		if listed%listingCheckEvery == 1 && switches.ListingSpeed() == switches.ListingAuto {
			queued, err := s.queueSvc.UnprocessedCount(ctx, true, copyQueue)
			if err != nil {
				logger.Err(err).Msg("migration bucket list obj: unable to check the copy queue")
			} else if queued >= listingQueueLimit {
				return s.rescheduleListing(ctx, replicationID, p, queued, logger)
			}
		}
		if object.Err != nil {
			return fmt.Errorf("migration bucket list obj: list objects error %w", object.Err)
		}
		objectsNum++
		isDir := object.Size == 0 && strings.HasSuffix(object.Key, "/")
		logger.Debug().Str(log.Object, object.Key).Str("obj_version_id", object.VersionID).Bool("is_dir", isDir).Msg("migration bucket list obj: start processing object from the list")
		if isDir {
			subP := p
			subP.Prefix = object.Key
			subTask, err := tasks.NewReplicationTask(ctx, replicationID, subP)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to create list obj sub task: %w", err)
			}
			_, err = s.taskClient.EnqueueContext(ctx, subTask)
			if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
				return fmt.Errorf("migration bucket list obj: unable to enqueue list obj sub task: %w", err)
			} else if err != nil {
				logger.Info().Interface("enqueue_task_payload", subP).Msg("cannot enqueue task with duplicate id")
			}
			err = s.storageSvc.SetLastListedObj(ctx, p, object.Key)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
			}
			continue
		}
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
		_, err = s.taskClient.EnqueueContext(ctx, task)
		if err != nil {
			if errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
				logger.Info().Msg("cannot enqueue task with duplicate id")
				continue
			}
			return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
		}
		err = s.storageSvc.SetLastListedObj(ctx, p, object.Key)
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
		}
	}

	if lastObjName == "" && objectsNum == 0 && p.Prefix != "" {
		p.Sync.InitDate()
		// copy empty dir object
		task, err := tasks.NewReplicationTask(ctx, replicationID, tasks.MigrateObjCopyPayload{
			Sync:   p.Sync,
			Bucket: p.Bucket,
			Obj: tasks.ObjPayload{
				Name: p.Prefix,
			},
		})
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to create copy obj task: %w", err)
		}
		_, err = s.taskClient.EnqueueContext(ctx, task)

		switch {
		case errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict):
			logger.Info().Msg("cannot enqueue task with duplicate id")
		case err != nil:
			return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
		}
	}
	_ = s.storageSvc.DelLastListedObj(ctx, p)

	if p.Prefix == "" {
		err = s.policySvc.ObjListStarted(ctx, replicationID)
		if err != nil {
			logger.Err(err).Msg("migration bucket list obj: unable to set ObjListStarted")
		}
	}

	logger.Info().Msg("migration bucket list obj: done")
	return nil
}

// rescheduleListing stops the current listing task and queues its continuation
// after listingRecheckDelay. The listing resumes from the cursor stored per
// object, so the only state needed is the task itself. The task id carries the
// cursor: the id of the running task is still taken, and a continuation from
// the same position stays deduplicated.
func (s *svc) rescheduleListing(ctx context.Context, replicationID entity.ReplicationStatusID,
	p tasks.MigrateBucketListObjectsPayload, queued int, logger *zerolog.Logger,
) error {
	cursor, err := s.storageSvc.GetLastListedObj(ctx, p)
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to get listing cursor: %w", err)
	}
	id := fmt.Sprintf("%s:%08x", tasks.MigrateBucketListObjectsTaskID(p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, p.Prefix),
		crc32.ChecksumIEEE([]byte(cursor)))
	task, err := tasks.NewReplicationTask(ctx, replicationID, p, asynq.TaskID(id), asynq.ProcessIn(listingRecheckDelay))
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to create continuation task: %w", err)
	}
	logger.Info().Int("queued", queued).Str("cursor", cursor).
		Msg("migration bucket list obj: copy queue is full, pausing the listing")
	if _, err = s.taskClient.EnqueueContext(ctx, task); err != nil {
		if errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
			// a continuation from this position is already queued
			return nil
		}
		return fmt.Errorf("migration bucket list obj: unable to enqueue continuation task: %w", err)
	}
	return nil
}
