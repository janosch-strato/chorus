// Copyright 2026 STRATO GmbH
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

package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelease_RetriesOnTransientError(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	locker := redislock.New(client)
	lock, err := locker.Obtain(context.Background(), "test-key", 10*time.Second, nil)
	require.NoError(t, err)
	require.True(t, mr.Exists("test-key"))

	logger := zerolog.New(zerolog.NewTestWriter(t))
	appCtx := context.Background()
	storeLock := NewLock(appCtx, &logger, lock, 0, "test")
	storeLock.releaseRetryDelay = 50 * time.Millisecond

	// Make the first release attempt fail.
	mr.SetError("IOERR simulated i/o timeout")

	// Clear the error after a short delay so the retry succeeds.
	go func() {
		time.Sleep(250 * time.Millisecond)
		mr.SetError("")
	}()

	storeLock.Release(context.Background())

	// Wait for the background retry goroutine to complete.
	assert.Eventually(t, func() bool {
		return !mr.Exists("test-key")
	}, 3*time.Second, 100*time.Millisecond, "lock key should be deleted after retry")
}

func TestRelease_NoRetryOnSuccess(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	locker := redislock.New(client)
	lock, err := locker.Obtain(context.Background(), "test-key", 10*time.Second, nil)
	require.NoError(t, err)
	require.True(t, mr.Exists("test-key"))

	logger := zerolog.New(zerolog.NewTestWriter(t))
	appCtx := context.Background()
	storeLock := NewLock(appCtx, &logger, lock, 0, "test")

	storeLock.Release(context.Background())

	// Key should be gone immediately — no background retry needed.
	assert.False(t, mr.Exists("test-key"))
}

func TestRelease_StopsOnContextCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	locker := redislock.New(client)
	lock, err := locker.Obtain(context.Background(), "test-key", 10*time.Second, nil)
	require.NoError(t, err)

	logger := zerolog.New(zerolog.NewTestWriter(t))
	appCtx, cancel := context.WithCancel(context.Background())
	storeLock := NewLock(appCtx, &logger, lock, 0, "test")
	storeLock.releaseRetryDelay = 50 * time.Millisecond

	// Set a persistent error so the release always fails.
	mr.SetError("IOERR persistent error")

	storeLock.Release(context.Background())

	// Cancel the app context — the background goroutine should stop.
	cancel()

	select {
	case <-storeLock.retryDone:
	case <-time.After(time.Second):
		t.Fatal("retry goroutine did not stop after context cancellation")
	}
}
