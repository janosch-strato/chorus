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

package standalone

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/util"
)

// TestFlushRefusedForDBInUse covers the accident the flush guard exists for: a
// second migration on the same redis, with its own db numbers, that happen to
// overlap ours. It does not show up in our queue db, only as a connection.
func TestFlushRefusedForDBInUse(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	// databases of our own, so that neither an instance of another test nor
	// one of a previous run shows up as the user we are looking for
	conf.Redis.MetaDB, conf.Redis.QueueDB = 12, 13
	conf.Redis.LockDB, conf.Redis.ConfigDB = 14, 15

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	used, err := redisDBsInUse(ctx, conf)
	r.NoError(err)
	r.Empty(used, "no other user of our databases before we make one")

	other := util.NewRedis(conf.Redis, conf.Redis.MetaDB)
	defer other.Close()
	r.NoError(other.Ping(ctx).Err())

	used, err = redisDBsInUse(ctx, conf)
	r.NoError(err)
	r.Equal([]int{conf.Redis.MetaDB}, used)

	err = Start(ctx, dom.AppInfo{}, conf, true)
	r.ErrorIs(err, dom.ErrInvalidArg)
	r.Contains(err.Error(), "refusing to flush redis")
}
