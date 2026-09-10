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

package router

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/settings"
	"github.com/clyso/chorus/pkg/switches"
)

func Test_headBucketCache(t *testing.T) {
	answer := http.Header{
		"X-Amz-Bucket-Region": []string{"us-east-1"},
		"Server":              []string{"storage"},
		"Date":                []string{"Tue, 01 Sep 2026 08:00:00 GMT"},
		"X-Amz-Request-Id":    []string{"one-request-only"},
	}
	require.NoError(t, settings.ReadFromDestination.Set(true))
	t.Cleanup(func() {
		require.NoError(t, settings.ReadFromDestination.Set(false))
		// the cache is one per process: empty it for whoever runs next
		require.NoError(t, settings.HeadBucketCache.SetString(switches.Disabled))
		require.NoError(t, settings.HeadBucketCache.SetString(switches.Enabled))
	})

	t.Run("the headers of the storage are kept, those of its request are not", func(t *testing.T) {
		r := require.New(t)
		remember(testUser, testBucket, answer)

		cached := settings.HeadBucketResponse.Get(testUser, testBucket)
		r.Equal("us-east-1", cached.Get("X-Amz-Bucket-Region"))
		r.Equal("storage", cached.Get("Server"))
		r.Empty(cached.Get("Date"))
		r.Empty(cached.Get("X-Amz-Request-Id"))
	})

	t.Run("a remembered bucket is answered without a storage", func(t *testing.T) {
		r := require.New(t)
		remember(testUser, testBucket, answer)
		rt := &router{}

		resp, storage, isApiErr, err := rt.headBucket(readReq(s3.HeadBucket, "", "/"+testBucket))
		r.NoError(err)
		r.False(isApiErr)
		r.Equal(http.StatusOK, resp.StatusCode)
		r.Equal(storageCache, storage)
		r.Equal("us-east-1", resp.Header.Get("X-Amz-Bucket-Region"))

		date, err := time.Parse(http.TimeFormat, resp.Header.Get("Date"))
		r.NoError(err, "the reply carries the time it is sent")
		r.WithinDuration(time.Now(), date, time.Minute)
		r.Empty(settings.HeadBucketResponse.Get(testUser, testBucket).Get("Date"), "the reply does not change the entry")
	})
}
