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
	"sync"
	"sync/atomic"
	"time"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/switches"
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

// headBucketCache holds the answer of a bucket existence check per user and
// bucket. HEAD answers carry no body, so an entry is a handful of headers.
type headBucketCache struct {
	mu      sync.RWMutex
	answers map[string]http.Header
	// on is what the switch said when a request last looked, so that turning
	// it off clears the answers once instead of on every request while off.
	on atomic.Bool
}

func newHeadBucketCache() *headBucketCache {
	c := &headBucketCache{answers: map[string]http.Header{}}
	c.on.Store(switches.HeadBucketCache())
	return c
}

func headBucketKey(user, bucket string) string {
	// The answer depends on the credentials the request is made with, which
	// the proxy derives from the user.
	return user + ":" + bucket
}

// enabled reports whether answers may be replayed. Turning the cache off
// forgets what it holds: an operator who turns it off wants the storage asked
// again, not a memory that outlives the switch.
func (c *headBucketCache) enabled() bool {
	on := switches.HeadBucketCache()
	if c.on.Swap(on) && !on {
		c.mu.Lock()
		defer c.mu.Unlock()
		clear(c.answers)
	}
	return on
}

// get returns the headers of a remembered check, or nil if there is none.
func (c *headBucketCache) get(user, bucket string) http.Header {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.answers[headBucketKey(user, bucket)]
}

// put remembers the headers of a successful check.
func (c *headBucketCache) put(user, bucket string, header http.Header) {
	keep := make(http.Header, len(header))
	for name, values := range header {
		if _, skip := headerNotReplayed[name]; skip {
			continue
		}
		keep[name] = values
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !switches.HeadBucketCache() {
		// switched off while the storage was answering this request
		return
	}
	c.answers[headBucketKey(user, bucket)] = keep
}

// headBucket answers a bucket existence check from the cache if it holds one,
// and remembers the answer of the storage otherwise.
func (r *router) headBucket(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	if !r.readFromDestination || r.headCache == nil || !r.headCache.enabled() {
		return r.commonRead(req)
	}
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	if cached := r.headCache.get(user, bucket); cached != nil {
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
		r.headCache.put(user, bucket, resp.Header)
	}
	return resp, storage, isApiErr, err
}
