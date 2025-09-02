// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/s3"
)

var (
	extRedisAddr s3.ConfAddr
	useExtRedis  bool
)

func init() {
	useExtRedis, _ = strconv.ParseBool(os.Getenv("EXT_REDIS"))
	if env := os.Getenv("EXT_REDIS_ADDR"); env != "" {
		extRedisAddr = s3.NewConfAddr(env)
	} else {
		extRedisAddr = s3.NewConfAddr("localhost:6379")
	}
}

func SetupRedisAddr(t testing.TB) s3.ConfAddr {
	t.Helper()
	if useExtRedis {
		client := redis.NewClient(&redis.Options{Addr: extRedisAddr.Value()})
		err := client.FlushAll(context.Background()).Err()
		if err != nil {
			t.Fatalf("setup: failed to redis.FlushAll %s: %v", extRedisAddr.Value(), err)
		}
		return extRedisAddr
	} else {
		db := miniredis.RunT(t)
		return s3.NewConfAddr(db.Addr())
	}
}

func SetupRedis(t testing.TB) (client redis.UniversalClient) {
	t.Helper()
	if useExtRedis {
		client = redis.NewClient(&redis.Options{Addr: extRedisAddr.Value(), DB: 15})
		err := client.FlushDB(context.Background()).Err()
		if err != nil {
			t.Fatalf("setup: failed to redis.FlushAll %s: %v", extRedisAddr.Value(), err)
		}
	} else {
		db := miniredis.RunT(t)
		client = redis.NewClient(&redis.Options{Addr: db.Addr()})
	}
	t.Cleanup(func() {
		client.Close()
	})
	return client
}
