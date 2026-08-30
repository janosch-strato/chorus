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

package util

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	xctx "github.com/clyso/chorus/pkg/ctx"
)

// TimingHook accounts the time redis calls take to the request that made them,
// so that a slow request can be attributed to redis without guessing. Calls
// whose context does not collect timings only pay the context lookup.
type TimingHook struct{}

var _ redis.Hook = TimingHook{}

func (TimingHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (TimingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		timing := xctx.GetTiming(ctx)
		if timing == nil {
			return next(ctx, cmd)
		}
		start := time.Now()
		err := next(ctx, cmd)
		timing.AddRedis(time.Since(start))
		return err
	}
}

func (TimingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		timing := xctx.GetTiming(ctx)
		if timing == nil {
			return next(ctx, cmds)
		}
		start := time.Now()
		err := next(ctx, cmds)
		timing.AddRedis(time.Since(start))
		return err
	}
}
