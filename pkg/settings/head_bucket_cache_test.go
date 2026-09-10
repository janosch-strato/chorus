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
	"github.com/clyso/chorus/pkg/switches"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_HeadBucketCache(t *testing.T) {
	r := require.New(t)
	t.Cleanup(func() { require.NoError(t, HeadBucketCache.SetString(switches.Enabled)) })

	r.True(HeadBucketCache.Get(), "on unless an operator turns it off")

	region := http.Header{"X-Amz-Bucket-Region": []string{"us-east-1"}}
	HeadBucketResponse.Put("user", "bucket", region)
	r.Equal("us-east-1", HeadBucketResponse.Get("user", "bucket").Get("X-Amz-Bucket-Region"))
	r.Nil(HeadBucketResponse.Get("user", "other-bucket"), "the answer is of the bucket it was given for")
	r.Nil(HeadBucketResponse.Get("other-user", "bucket"), "and of the credentials it was obtained with")

	HeadBucketResponse.Put("user", "other-bucket", region)
	r.Nil(HeadBucketResponse.Get("user", "bucket"), "one answer is kept, of whichever bucket was asked last")

	r.NoError(HeadBucketCache.SetString(switches.Disabled))
	r.False(HeadBucketCache.Get())
	r.Nil(HeadBucketResponse.Get("user", "other-bucket"), "turning it off forgets the answer")

	HeadBucketResponse.Put("user", "bucket", region)
	r.Nil(HeadBucketResponse.Get("user", "bucket"), "and it remembers nothing while off")

	r.NoError(HeadBucketCache.SetString(switches.Enabled))
	r.Nil(HeadBucketResponse.Get("user", "bucket"), "switched on again it starts from nothing")
}
