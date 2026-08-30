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

package util_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/clyso/chorus/pkg/util"
)

func Test_TimingHook(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	c.AddHook(util.TimingHook{})

	// a context without a collector is not measured and must still work
	r.NoError(c.Set(t.Context(), "key", "value", 0).Err())

	ctx, timing := xctx.WithTiming(t.Context())
	r.NoError(c.Set(ctx, "key", "value", 0).Err())
	r.NoError(c.Get(ctx, "key").Err())

	duration, calls := timing.Redis()
	r.EqualValues(2, calls, "both calls counted")
	r.Positive(duration, "time recorded")

	// pipelines count as one call
	pipe := c.Pipeline()
	pipe.Set(ctx, "key2", "value", 0)
	pipe.Get(ctx, "key2")
	_, err := pipe.Exec(ctx)
	r.NoError(err)
	_, calls = timing.Redis()
	r.EqualValues(3, calls)
}
