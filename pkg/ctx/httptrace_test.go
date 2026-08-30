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

package ctx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
)

func Test_TraceContext(t *testing.T) {
	r := require.New(t)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	client := srv.Client()

	_, timing := xctx.WithTiming(t.Context())
	do := func() {
		req, err := http.NewRequestWithContext(xctx.TraceContext(context.Background(), timing),
			http.MethodGet, srv.URL, nil)
		r.NoError(err)
		resp, err := client.Do(req)
		r.NoError(err)
		r.NoError(resp.Body.Close())
	}

	do()
	conns, reused := timing.Connection()
	r.EqualValues(1, conns)
	r.EqualValues(0, reused, "first request cannot reuse a connection")
	_, connect, tlsHandshake, ttfb := timing.Network()
	r.Positive(connect, "connect time recorded")
	r.Positive(tlsHandshake, "tls handshake recorded")
	r.Positive(ttfb, "time to first byte recorded")

	// the second request comes from the idle pool: that is what tells a slow
	// storage apart from one we cannot get a connection to
	do()
	conns, reused = timing.Connection()
	r.EqualValues(2, conns)
	r.EqualValues(1, reused)
}

func Test_TimingWithoutCollector(t *testing.T) {
	r := require.New(t)
	// a nil collector must be safe: most contexts do not collect
	var timing *xctx.Timing
	r.Nil(xctx.GetTiming(t.Context()))
	timing.AddRedis(time.Second)
	timing.SetTTFB(time.Second)
	timing.SetRequestID("id")
	d, calls := timing.Redis()
	r.Zero(d)
	r.Zero(calls)
	r.Equal(t.Context(), xctx.TraceContext(t.Context(), nil))
}
