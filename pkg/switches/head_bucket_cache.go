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

package switches

import "sync/atomic"

// headBucketCache is on unless an operator turns it off, and only matters
// where the cache exists at all, which is the readFromDestination mode.
var headBucketCache atomic.Bool

func init() {
	headBucketCache.Store(true)
}

// SetHeadBucketCache turns the cache of bucket existence checks on or off.
func SetHeadBucketCache(enabled bool) {
	headBucketCache.Store(enabled)
}

// HeadBucketCache reports whether bucket existence checks may be answered from
// memory.
func HeadBucketCache() bool {
	return headBucketCache.Load()
}
