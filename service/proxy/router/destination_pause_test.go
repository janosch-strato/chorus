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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestPause(t *testing.T) (*destinationPause, *testClock) {
	t.Helper()
	clock := newTestClock()
	p := newDestinationPause(3, 5*time.Minute)
	p.now = clock.Now
	return p, clock
}

func Test_destinationPause(t *testing.T) {
	t.Run("reads pause once the destination missed the limit of answers", func(t *testing.T) {
		r := require.New(t)
		p, _ := newTestPause(t)

		r.False(p.timedOut(testDest))
		r.False(p.paused(testDest), "one timeout is not a destination worth giving up on")
		r.False(p.timedOut(testDest))
		r.False(p.paused(testDest))

		r.True(p.timedOut(testDest), "the timeout that reached the limit was not reported")
		r.True(p.paused(testDest))
	})

	t.Run("an answer clears the timeouts before them", func(t *testing.T) {
		r := require.New(t)
		p, _ := newTestPause(t)

		p.timedOut(testDest)
		p.timedOut(testDest)
		p.answered(testDest)

		p.timedOut(testDest)
		p.timedOut(testDest)
		r.False(p.paused(testDest), "timeouts of before the answer were counted")
		r.True(p.timedOut(testDest))
		r.True(p.paused(testDest))
	})

	t.Run("an answer ends a pause that is running", func(t *testing.T) {
		r := require.New(t)
		p, _ := newTestPause(t)

		for range 3 {
			p.timedOut(testDest)
		}
		r.True(p.paused(testDest))

		p.answered(testDest)
		r.False(p.paused(testDest))
	})

	t.Run("the pause ends after the configured time", func(t *testing.T) {
		r := require.New(t)
		p, clock := newTestPause(t)

		for range 3 {
			p.timedOut(testDest)
		}
		clock.advance(5*time.Minute - time.Second)
		r.True(p.paused(testDest), "the pause ended early")

		clock.advance(2 * time.Second)
		r.False(p.paused(testDest))
	})

	t.Run("a destination that is still silent pauses again on the first timeout", func(t *testing.T) {
		r := require.New(t)
		p, clock := newTestPause(t)

		for range 3 {
			p.timedOut(testDest)
		}
		clock.advance(6 * time.Minute)
		r.False(p.paused(testDest))

		r.True(p.timedOut(testDest), "the read that found the destination still silent did not pause it")
		r.True(p.paused(testDest))
	})

	t.Run("a pause is reported once, not for every read on its way", func(t *testing.T) {
		r := require.New(t)
		p, _ := newTestPause(t)

		for range 3 {
			p.timedOut(testDest)
		}
		r.False(p.timedOut(testDest))
		r.False(p.timedOut(testDest))
	})

	t.Run("destinations are paused on their own", func(t *testing.T) {
		r := require.New(t)
		p, _ := newTestPause(t)

		for range 3 {
			p.timedOut(testDest)
		}
		r.True(p.paused(testDest))
		r.False(p.paused("dst2"), "a destination that answers was paused with another one")
	})

	t.Run("pausing that is turned off never pauses", func(t *testing.T) {
		r := require.New(t)
		off := []*destinationPause{
			nil,
			newDestinationPause(0, 5*time.Minute),
			newDestinationPause(-1, 5*time.Minute),
			newDestinationPause(3, 0),
			newDestinationPause(3, -1),
		}
		for _, p := range off {
			for range 5 {
				r.False(p.timedOut(testDest))
			}
			r.False(p.paused(testDest))
			p.answered(testDest) // must not panic either
		}
	})
}

func Test_destinationPause_concurrent(t *testing.T) {
	r := require.New(t)
	p := newDestinationPause(3, time.Minute)

	var paused atomic.Int64
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if p.timedOut(testDest) {
					paused.Add(1)
				}
				p.paused(testDest)
				if i%10 == 0 {
					p.answered(testDest)
				}
			}
		}()
	}
	wg.Wait()
	r.Positive(paused.Load(), "no read ever found the destination silent enough to pause it")
}
