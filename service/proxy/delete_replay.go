/*
 * Copyright © 2024 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
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

package proxy

import (
	"context"
	"fmt"
	"maps"

	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

// deleteFinisher carries out what an interrupted delete still needs. It is
// separate so that the deciding can be read and tested without a storage.
type deleteFinisher interface {
	gone(ctx context.Context, intent meta.DeleteIntent) (bool, error)
	remove(ctx context.Context, intent meta.DeleteIntent) error
	versions(ctx context.Context, intent meta.DeleteIntent) (map[meta.Destination]int64, error)
	queue(ctx context.Context, intent meta.DeleteIntent) error
}

// finishInterruptedDeletes carries through the deletes this proxy had written
// down and not finished when it stopped.
//
// Whether the storage got as far as carrying one out cannot be found out, and
// does not have to be: a delete can simply be made again. So an object that is
// gone is replicated, and one that is still there is deleted again.
//
// Unless its versions have moved since, which means a write newer than the
// delete - that is what lets a second proxy serve while this one starts up.
// Only its own intents: another proxy's may be a delete it is making now.
//
// Versions gone altogether are not such a write: a write puts its own there,
// only a delete takes them away. They are this delete's own doing, from a run
// that got that far and no further, and say nothing about it being superseded.
func finishInterruptedDeletes(ctx context.Context, intents *meta.DeleteIntents, owner string, f deleteFinisher) error {
	mine, err := intents.OwnedBy(ctx, owner)
	if err != nil {
		return fmt.Errorf("unable to list the deletes left unfinished: %w", err)
	}
	for _, intent := range mine {
		logger := zerolog.Ctx(ctx).With().
			Str(log.Storage, intent.Storage).
			Str(log.Bucket, intent.Object.Bucket).
			Str(log.Object, intent.Object.Name).
			Logger()

		now, err := f.versions(ctx, intent)
		if err != nil {
			return fmt.Errorf("unable to read the versions of an unfinished delete: %w", err)
		}
		if len(now) != 0 && !maps.Equal(now, intent.Versions) {
			logger.Info().Msg("interrupted delete: the object was written again since, leaving it alone")
			if err := intents.Done(ctx, []meta.DeleteIntent{intent}); err != nil {
				return err
			}
			continue
		}

		isGone, err := f.gone(ctx, intent)
		if err != nil {
			return fmt.Errorf("unable to ask the source storage about an unfinished delete: %w", err)
		}
		if !isGone {
			// Either it never reached the storage or it is running now.
			// Making it again settles both.
			if err := f.remove(ctx, intent); err != nil {
				return fmt.Errorf("unable to finish an interrupted delete: %w", err)
			}
			logger.Info().Msg("interrupted delete: object was still there, deleted again")
		}
		if err := f.queue(ctx, intent); err != nil {
			return fmt.Errorf("unable to queue the replication of an interrupted delete: %w", err)
		}
		if err := intents.Done(ctx, []meta.DeleteIntent{intent}); err != nil {
			return err
		}
	}
	return nil
}

type storageFinisher struct {
	clients    s3client.Service
	versionSvc meta.VersionService
	replSvc    replication.Service
}

func (s storageFinisher) client(ctx context.Context, intent meta.DeleteIntent) (s3client.Client, error) {
	return s.clients.GetByName(xctx.SetUser(ctx, intent.User), intent.Storage)
}

func (s storageFinisher) gone(ctx context.Context, intent meta.DeleteIntent) (bool, error) {
	client, err := s.client(ctx, intent)
	if err != nil {
		return false, err
	}
	_, statErr := client.S3().StatObject(ctx, intent.Object.Bucket, intent.Object.Name, mclient.StatObjectOptions{})
	if statErr == nil {
		return false, nil
	}
	switch mclient.ToErrorResponse(statErr).Code {
	case mclient.NoSuchKey, mclient.NoSuchBucket:
		return true, nil
	}
	// Anything else is a storage failing to answer, not an absent object.
	return false, statErr
}

func (s storageFinisher) remove(ctx context.Context, intent meta.DeleteIntent) error {
	client, err := s.client(ctx, intent)
	if err != nil {
		return err
	}
	return client.S3().RemoveObject(ctx, intent.Object.Bucket, intent.Object.Name, mclient.RemoveObjectOptions{})
}

func (s storageFinisher) versions(ctx context.Context, intent meta.DeleteIntent) (map[meta.Destination]int64, error) {
	return s.versionSvc.GetObj(ctx, intent.Object)
}

func (s storageFinisher) queue(ctx context.Context, intent meta.DeleteIntent) error {
	// Only while they still hold what the intent read, see queueDeletes.
	if _, err := s.versionSvc.DeleteObjIfUnchanged(ctx, intent.Object, intent.Versions); err != nil {
		return err
	}
	ctx = xctx.SetUser(ctx, intent.User)
	ctx = xctx.SetBucket(ctx, intent.Object.Bucket)
	ctx = xctx.SetStorage(ctx, intent.Storage)
	return s.replSvc.Replicate(ctx, &tasks.ObjectSyncPayload{
		Object:  dom.Object{Bucket: intent.Object.Bucket, Name: intent.Object.Name},
		Sync:    tasks.Sync{FromStorage: intent.Storage},
		Deleted: true,
	})
}
