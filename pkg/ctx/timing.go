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

package ctx

import (
	"context"
	"sync/atomic"
	"time"
)

// Timing collects where the time of a single request went, so that a slow
// request can be attributed to the storage, to redis or to establishing a
// connection without correlating several data sources afterwards.
//
// Collection is opt in: only requests whose context carries a Timing are
// measured. Everything else pays one context lookup. All fields are written
// from the request goroutine and from http trace callbacks, hence the atomics.
type Timing struct {
	redisNanos atomic.Int64
	redisCalls atomic.Int64

	dnsNanos     atomic.Int64
	connectNanos atomic.Int64
	tlsNanos     atomic.Int64
	ttfbNanos    atomic.Int64

	// connections holds how many storage connections were used, reused how
	// many of those came from the idle pool.
	connections atomic.Int64
	reused      atomic.Int64

	requestID atomic.Pointer[string]
}

type timingKey struct{}

// WithTiming returns a context that collects timings, and the collector.
func WithTiming(ctx context.Context) (context.Context, *Timing) {
	t := &Timing{}
	return context.WithValue(ctx, timingKey{}, t), t
}

// GetTiming returns the collector of the context, or nil if the context does
// not collect timings.
func GetTiming(ctx context.Context) *Timing {
	t, _ := ctx.Value(timingKey{}).(*Timing)
	return t
}

// AddRedis records the duration of one redis call.
func (t *Timing) AddRedis(d time.Duration) {
	if t == nil {
		return
	}
	t.redisNanos.Add(int64(d))
	t.redisCalls.Add(1)
}

// Redis returns the accumulated time spent in redis and the number of calls.
func (t *Timing) Redis() (time.Duration, int64) {
	if t == nil {
		return 0, 0
	}
	return time.Duration(t.redisNanos.Load()), t.redisCalls.Load()
}

func (t *Timing) addDNS(d time.Duration)     { t.dnsNanos.Add(int64(d)) }
func (t *Timing) addConnect(d time.Duration) { t.connectNanos.Add(int64(d)) }
func (t *Timing) addTLS(d time.Duration)     { t.tlsNanos.Add(int64(d)) }

// SetTTFB records the time until the storage sent the first response byte.
func (t *Timing) SetTTFB(d time.Duration) {
	if t == nil {
		return
	}
	t.ttfbNanos.Add(int64(d))
}

// SetRequestID records the request id the storage returned, which is what a
// storage operator needs to find the request on their side.
func (t *Timing) SetRequestID(id string) {
	if t == nil || id == "" {
		return
	}
	t.requestID.Store(&id)
}

// Connection returns how the storage connections were obtained: the number of
// connections used and how many of them were reused from the idle pool.
func (t *Timing) Connection() (used, reused int64) {
	if t == nil {
		return 0, 0
	}
	return t.connections.Load(), t.reused.Load()
}

// Network returns the time spent resolving, connecting, shaking hands, and
// waiting for the first response byte.
func (t *Timing) Network() (dns, connect, tls, ttfb time.Duration) {
	if t == nil {
		return 0, 0, 0, 0
	}
	return time.Duration(t.dnsNanos.Load()), time.Duration(t.connectNanos.Load()),
		time.Duration(t.tlsNanos.Load()), time.Duration(t.ttfbNanos.Load())
}

// RequestID returns the request id of the last storage response.
func (t *Timing) RequestID() string {
	if t == nil {
		return ""
	}
	if id := t.requestID.Load(); id != nil {
		return *id
	}
	return ""
}
