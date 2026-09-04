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

// Reading migrated objects from the replication destination.
//
// While a migration runs, all requests are routed to the source storage until
// the replication is switched. If reads from the source storage are painfully
// slow, the readFromDestination option lets the proxy answer reads of objects
// that the migration has already copied from the destination storage instead.
//
// While the initial sync runs, an object counts as migrated when the worker has
// recorded its copy, which it does per replication in a set of object names.
// Once it is done, everything counts as migrated and the records are dropped by
// the copy that ends the initial sync, see liveSync(). Everything the proxy wrote
// afterwards is visible in the object version metadata, so a destination
// version behind the source version disqualifies the object again. Objects
// deleted through the proxy have their record removed, see objectDeleted().
//
// The decision is per object and never trusted blindly: a destination that
// cannot answer the read makes the request fall back to the source storage.

package router

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

// Reasons for keeping a read on the source storage although reading from the
// destination is enabled. They are exported as the reason label of the
// proxy_destination_reads_skipped_total metric, so the set stays small and
// stable.
const (
	skipSwitchInProgress  = "switch_in_progress"
	skipNoObject          = "no_object"
	skipMethod            = "method"
	skipVersionRequested  = "version_requested"
	skipNoReplication     = "no_replication"
	skipOtherSource       = "not_migration_source"
	skipManyDestinations  = "multiple_destinations"
	skipPolicyError       = "policy_error"
	skipMetaError         = "meta_error"
	skipDestinationBehind = "destination_behind"
	skipRecordError       = "record_error"
	skipNotCopied         = "not_copied"
	skipPendingDelete     = "pending_delete"
)

// destinationReadRoute tells whether the request may be served by the
// replication destination instead of the source storage, and returns the
// destination storage and bucket to use. A switch in progress owns the routing
// of the bucket and is left alone.
func (r *router) destinationReadRoute(req *http.Request, source, user, bucket string, switchInProgress bool) (destStorage, destBucket string, ok bool) {
	if !r.readFromDestination {
		return "", "", false
	}
	destStorage, destBucket, reason := r.destinationReadDecision(req, source, user, bucket, switchInProgress)
	if reason != "" {
		metrics.ProxyDestinationReadSkipped(reason)
		return "", "", false
	}
	return destStorage, destBucket, true
}

// destinationReadDecision is destinationReadRoute with the reason for keeping
// the read on the source storage. An empty reason means the destination may
// answer the read.
func (r *router) destinationReadDecision(req *http.Request, source, user, bucket string, switchInProgress bool) (destStorage, destBucket, reason string) {
	if switchInProgress {
		return "", "", skipSwitchInProgress
	}
	ctx := req.Context()
	object := xctx.GetObject(ctx)
	if object == "" {
		return "", "", skipNoObject
	}
	switch xctx.GetMethod(ctx) {
	case s3.GetObject, s3.HeadObject:
	default:
		// Listings must not be answered by a destination that is still being
		// filled, and metadata of an object may be replicated separately from
		// its content.
		return "", "", skipMethod
	}
	if req.URL.Query().Has("versionId") {
		// A copy has its own version ids on the destination storage, so the
		// requested version cannot be asked for there.
		return "", "", skipVersionRequested
	}

	dest, reason := r.migrationDestination(ctx, source, user, bucket)
	if reason != "" {
		return "", "", reason
	}
	if reason = r.objectMigrated(ctx, source, user, bucket, object, dest); reason != "" {
		return "", "", reason
	}

	destBucket = dest.Bucket
	if destBucket == "" {
		destBucket = bucket
	}
	return dest.Storage, destBucket, ""
}

// migrationDestination returns the single replication destination the bucket is
// migrated to. Fanning out to several destinations gives no reason to prefer
// any of them, so no destination is returned in that case. A non empty reason
// means that there is no destination to read from.
func (r *router) migrationDestination(ctx context.Context, source, user, bucket string) (entity.ReplicationPolicyDestination, string) {
	policies, err := r.policySvc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(user, bucket))
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return entity.ReplicationPolicyDestination{}, skipNoReplication
		}
		zerolog.Ctx(ctx).Err(err).Msg("read from destination: unable to get replication policies")
		return entity.ReplicationPolicyDestination{}, skipPolicyError
	}
	if policies.FromStorage != source {
		return entity.ReplicationPolicyDestination{}, skipOtherSource
	}
	if len(policies.Destinations) != 1 {
		zerolog.Ctx(ctx).Info().Int("destinations", len(policies.Destinations)).
			Msg("read from destination: skip bucket with more than one replication destination")
		return entity.ReplicationPolicyDestination{}, skipManyDestinations
	}
	return policies.Destinations[0], ""
}

// objectMigrated returns an empty reason if the object can be read from the
// destination: it has been copied there and has not been written or deleted
// through the proxy since. Any other reason keeps the read on the source
// storage.
func (r *router) objectMigrated(ctx context.Context, source, user, bucket, object string, dest entity.ReplicationPolicyDestination) string {
	objMeta, err := r.versionSvc.GetObj(ctx, dom.Object{Bucket: bucket, Name: object})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("read from destination: unable to get object version metadata")
		return skipMetaError
	}
	if objMeta[meta.ToDest(source, "")] > objMeta[meta.ToDest(dest.Storage, dest.Bucket)] {
		// written through the proxy and not replicated yet
		return skipDestinationBehind
	}

	id := migrationID(source, user, bucket, dest)
	if r.liveSync(ctx, id) {
		// everything the listing found has been copied, so the destination
		// holds the object unless its deletion is still on its way there
		pending, err := r.storageSvc.IsPendingDeleteObj(ctx, id, object)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("read from destination: unable to read the pending delete record")
			return skipRecordError
		}
		if pending {
			return skipPendingDelete
		}
		return ""
	}

	migrated, err := r.storageSvc.IsMigratedObj(ctx, id, object)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("read from destination: unable to read the migrated object record")
		return skipRecordError
	}
	if !migrated {
		return skipNotCopied
	}
	return ""
}

// liveSync reports whether the migration has copied everything its listing
// found and only replicates what happens since. The copy that ended the
// initial sync records it and drops the per object records with it, so from
// then on every object counts as copied and what the destination does not have
// yet is a deletion or a write in flight, both of which say so elsewhere.
func (r *router) liveSync(ctx context.Context, id entity.ReplicationStatusID) bool {
	status, err := r.policySvc.GetReplicationPolicyInfo(ctx, id)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("read from destination: unable to get replication status")
		return false
	}
	return status.LiveSync
}

// migrationID names the replication the bucket is migrated by.
func migrationID(source, user, bucket string, dest entity.ReplicationPolicyDestination) entity.ReplicationStatusID {
	return entity.NewReplicationStatusID(user, source, bucket, dest.Storage, dest.Bucket)
}

// objectDeleted records that the object was deleted through the proxy. The
// object is gone from the source storage while the destination still holds it
// until the deletion has been replicated, so it must not be read from there:
// its migrated record goes and a pending delete takes its place. Dropping the
// migrated record also allows the object to be copied again should it
// reappear.
func (r *router) objectDeleted(ctx context.Context, source, object string) {
	if !r.readFromDestination {
		return
	}
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	dest, reason := r.migrationDestination(ctx, source, user, bucket)
	if reason != "" {
		return
	}
	id := migrationID(source, user, bucket, dest)
	if err := r.storageSvc.SetPendingDeleteObj(ctx, id, object); err != nil {
		zerolog.Ctx(ctx).Err(err).Str(log.Object, object).Msg("read from destination: unable to record the pending delete")
	}
	if err := r.storageSvc.DelMigratedObj(ctx, id, object); err != nil {
		zerolog.Ctx(ctx).Err(err).Str(log.Object, object).Msg("read from destination: unable to drop migrated object record")
	}
}

// readDestination sends the read request to the destination storage. The
// request is restored to its original form before returning so that the caller
// can retry it on the source storage.
func (r *router) readDestination(req *http.Request, destStorage, destBucket string) (resp *http.Response, isApiErr bool, err error) {
	ctx := xctx.SetStorage(req.Context(), destStorage)
	client, err := r.clients.GetByName(ctx, destStorage)
	if err != nil {
		return nil, false, err
	}

	// The url is shared with the original request, and both the bucket rewrite
	// and s3client.Do change it in place.
	origURL := *req.URL
	defer func() {
		*req.URL = origURL
	}()
	destReq := req.WithContext(ctx)
	if err = rewriteBucket(destReq, xctx.GetBucket(ctx), destBucket); err != nil {
		return nil, false, err
	}

	if xctx.GetMethod(ctx) == s3.GetObject {
		_ = r.limit.StorReq(ctx, client.Name()) //todo: refactor rate-limiting
	}
	resp, isApiErr, err = client.Do(destReq)
	if err == nil && !isApiErr {
		metrics.ProxyDestinationRead(destStorage)
	}
	return resp, isApiErr, err
}

// rewriteBucket points the request at another bucket, keeping the addressing
// style the client used. The destination client signs the request itself, so
// the url is all that has to change.
func rewriteBucket(req *http.Request, fromBucket, toBucket string) error {
	if fromBucket == toBucket {
		return nil
	}
	if host, found := strings.CutPrefix(req.Host, fromBucket+"."); found {
		req.Host = toBucket + "." + host
		return nil
	}
	// The escaped path is edited separately to keep the object name encoded
	// exactly as the client sent it.
	prefix := "/" + fromBucket
	path, found := strings.CutPrefix(req.URL.Path, prefix)
	if !found {
		return fmt.Errorf("%w: bucket %q is neither in request host %q nor in path %q", dom.ErrInternal, fromBucket, req.Host, req.URL.Path)
	}
	req.URL.Path = "/" + toBucket + path
	if req.URL.RawPath != "" {
		rawPath, found := strings.CutPrefix(req.URL.RawPath, prefix)
		if !found {
			return fmt.Errorf("%w: bucket %q is not in escaped request path %q", dom.ErrInternal, fromBucket, req.URL.RawPath)
		}
		req.URL.RawPath = "/" + toBucket + rawPath
	}
	return nil
}
