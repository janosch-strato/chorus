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

// Caching bucket existence checks.
//
// A HEAD bucket asks whether a bucket is there, and in a migration away from a
// storage that is too slow to read from, an application asking it over and
// over pays that latency every time. Nothing changes the answer while such a
// migration runs: the readFromDestination mode is for a source storage that is
// written to through the proxy alone, and buckets are not deleted under it. So
// the answer of the first successful check is kept for the lifetime of the
// process and replayed from memory afterwards. A storage is asked once per
// bucket and user, and never again.
//
// Only a successful check is remembered. A bucket that is not there yet may
// still be created, and a remembered "not there" would never expire.
//
// The reply is the answer of the storage, including the headers it sent of its
// own, such as the bucket region. Only the headers that belong to the one
// request that produced them are left out: the reply carries the time it is
// sent, and no request id, because a repeated id names a request that happened
// once and an invented one names none at all.

package router

import (
	"net/http"
	"time"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/settings"
)

// storageCache is reported as the storage of a request that no storage saw.
const storageCache = "cache"

// headerNotReplayed lists the response headers that belong to the single
// request that produced them.
var headerNotReplayed = map[string]struct{}{
	"Date":             {},
	"X-Amz-Request-Id": {},
	"X-Amz-Id-2":       {},
}

// remember stores what the storage answered, without the headers that were
// true of that one request only.
func remember(user, bucket string, header http.Header) {
	keep := make(http.Header, len(header))
	for name, values := range header {
		if _, skip := headerNotReplayed[name]; skip {
			continue
		}
		keep[name] = values
	}
	settings.HeadBucketResponse.Put(user, bucket, keep)
}

// headBucket answers a bucket existence check from the cache if it holds one,
// and remembers the answer of the storage otherwise.
func (r *router) headBucket(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	if !settings.ReadFromDestination.Get() || !settings.HeadBucketCache.Get() {
		return r.commonRead(req)
	}
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	if cached := settings.HeadBucketResponse.Get(user, bucket); cached != nil {
		metrics.ProxyHeadBucketCache(metrics.CacheHit)
		header := cached.Clone()
		header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       http.NoBody,
			Request:    req,
		}, storageCache, false, nil
	}
	metrics.ProxyHeadBucketCache(metrics.CacheMiss)

	resp, storage, isApiErr, err = r.commonRead(req)
	if err == nil && !isApiErr && resp != nil && resp.StatusCode == http.StatusOK {
		remember(user, bucket, resp.Header)
	}
	return resp, storage, isApiErr, err
}
