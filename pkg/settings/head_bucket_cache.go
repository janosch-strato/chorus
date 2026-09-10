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

package settings

import (
	"net/http"
	"sync/atomic"

	"github.com/clyso/chorus/pkg/switches"
)

// HeadBucketResponse is the response of a bucket existence check, one for the
// process: a chorus runs one migration, so it is one bucket that is asked
// about over and over. Which bucket the response is of is kept with it, so
// that a check of another one costs the response rather than being told about
// the wrong bucket.
//
// It lives here because the switch that governs it does: turning the switch
// off has to forget it, and the proxy would have to be reached into for that.
// What the response is made of is the business of whoever put it there. A HEAD
// response carries no body, so it is a handful of headers.
var HeadBucketResponse headBucketResponse

type headBucketResponse struct {
	stored atomic.Pointer[headBucketHeaders]
}

type headBucketHeaders struct {
	user, bucket string
	header       http.Header
}

// Get returns the response of that bucket, or nil if what is kept is of
// another one or nothing.
func (r *headBucketResponse) Get(user, bucket string) http.Header {
	kept := r.stored.Load()
	if kept == nil || kept.user != user || kept.bucket != bucket {
		return nil
	}
	return kept.header
}

// Put keeps a response, unless the cache is off, which is looked at again
// after storing: a switch that went off meanwhile forgot the response before
// this one was there.
func (r *headBucketResponse) Put(user, bucket string, header http.Header) {
	if !HeadBucketCache.Get() {
		return
	}
	r.stored.Store(&headBucketHeaders{user: user, bucket: bucket, header: header})
	if !HeadBucketCache.Get() {
		r.forget()
	}
}

func (r *headBucketResponse) forget() { r.stored.Store(nil) }

// HeadBucketCache turns the cache on and off. Off forgets the response, so
// that a suspect one does not come back when it is turned on again.
var HeadBucketCache = switches.NewBool("head-bucket-cache", true, func(enabled bool) error {
	if !enabled {
		HeadBucketResponse.forget()
	}
	return nil
})
