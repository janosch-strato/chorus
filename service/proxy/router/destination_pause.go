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

// Keeping reads off a destination storage that stopped answering them.
//
// While it is down every client of a migrated object pays the answer timeout
// on top of a normal read, which is the opposite of what reading from the
// destination is for.
//
// Only an answer clears the count, the end of a pause does not, so a
// destination that is still silent pauses again on the first timeout after it:
// a long outage costs one slow read per pause rather than one per limit.
//
// The state is in memory, so a proxy that restarts offers the destination a
// read again, knowing nothing about it yet.

package router

import (
	"sync"
	"time"
)

// A nil one, and one whose limit or pause is not positive, never pauses.
type destinationPause struct {
	limit int              // answer timeouts in a row that pause reads
	pause time.Duration    // how long they stay paused
	now   func() time.Time // time.Now, replaced by tests

	mu    sync.Mutex
	state map[string]destinationTimeouts
}

type destinationTimeouts struct {
	count int       // answer timeouts since the last answer
	until time.Time // reads are paused until then, zero if they are not
}

func newDestinationPause(limit int, pause time.Duration) *destinationPause {
	return &destinationPause{
		limit: limit,
		pause: pause,
		now:   time.Now,
		state: map[string]destinationTimeouts{},
	}
}

func (p *destinationPause) off() bool {
	return p == nil || p.limit <= 0 || p.pause <= 0
}

func (p *destinationPause) paused(storage string) bool {
	if p.off() {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.now().Before(p.state[storage].until)
}

// Reports whether this timeout is the one that paused the reads, so that the
// pause is logged once and not once per read already on its way.
func (p *destinationPause) timedOut(storage string) (justPaused bool) {
	if p.off() {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.state[storage]
	state.count++
	if state.count >= p.limit {
		now := p.now()
		justPaused = !now.Before(state.until)
		state.until = now.Add(p.pause)
	}
	p.state[storage] = state
	return justPaused
}

func (p *destinationPause) answered(storage string) {
	if p.off() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.state, storage)
}
