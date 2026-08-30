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
	"crypto/tls"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

// TraceContext returns a context that reports connection level timings of the
// requests made with it into t: whether the connection came from the idle
// pool, and how long name resolution, connecting, the TLS handshake and the
// wait for the first response byte took.
//
// It tells a storage that answers slowly apart from a storage we cannot get a
// connection to, which look identical in the total request duration.
func TraceContext(parent context.Context, t *Timing) context.Context {
	if t == nil {
		return parent
	}
	var dnsStart, connectStart, tlsStart, reqStart atomic.Int64
	reqStart.Store(time.Now().UnixNano())

	since := func(start *atomic.Int64) time.Duration {
		at := start.Load()
		if at == 0 {
			return 0
		}
		return time.Duration(time.Now().UnixNano() - at)
	}

	return httptrace.WithClientTrace(parent, &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { dnsStart.Store(time.Now().UnixNano()) },
		DNSDone:  func(httptrace.DNSDoneInfo) { t.addDNS(since(&dnsStart)) },

		ConnectStart: func(string, string) { connectStart.Store(time.Now().UnixNano()) },
		ConnectDone:  func(string, string, error) { t.addConnect(since(&connectStart)) },

		TLSHandshakeStart: func() { tlsStart.Store(time.Now().UnixNano()) },
		TLSHandshakeDone:  func(tls.ConnectionState, error) { t.addTLS(since(&tlsStart)) },

		GotConn: func(info httptrace.GotConnInfo) {
			t.connections.Add(1)
			if info.Reused {
				t.reused.Add(1)
			}
		},
		GotFirstResponseByte: func() { t.SetTTFB(since(&reqStart)) },
	})
}
