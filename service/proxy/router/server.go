/*
 * Copyright © 2023 Clyso GmbH
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
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/util"
)

func Serve(router Router, replSvc replication.Service) http.Handler {
	// Use a custom handler function instead of ServeMux to avoid automatic redirects
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("").Start(r.Context(), "Route")
		defer span.End()
		ctx, timing := xctx.WithTiming(ctx)
		r = r.WithContext(ctx)
		logger := zerolog.Ctx(r.Context())
		logger.Info().Msg("proxy: new request received")

		start := time.Now()
		// so that a client slow to send or to receive does not make the proxy
		// look slow
		body := newClientRequestBody(r.Body)
		r.Body = body

		resp, taskList, storage, isApiErr, err := router.Route(r)
		routeDuration := time.Since(start)
		if err != nil {
			metrics.ProxyRequestInternalDuration(xctx.GetMethod(ctx).String(), storage, body.internalDuration())
			// a storage that answers 404 or 503 answers with an error, and
			// the request ends here rather than below: without counting it
			// too, the status counter would only ever hold the successes
			status := s3client.ResponseStatus(resp, err)
			if status != 0 {
				metrics.ProxyStorageStatus(storage, status)
			}
			timingFields(logger.Info().Err(err), timing).
				Str(log.Storage, storage).
				Dur("route_duration", routeDuration).
				Int("status", status).
				Msg("proxy: request failed")
			util.WriteError(r.Context(), w, err)
			return
		}
		defer func() {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}()
		var enqueueDuration time.Duration
		ctx = log.WithStorage(ctx, storage)
		// TODO: is it reachable? This branch is active only if err == nil
		if isApiErr {
			zerolog.Ctx(ctx).Info().Err(err).Msg("s3 api error returned")
			// create replication tasks according to replication rules
		} else {
			replCtx, cancel := log.StartNew(ctx)
			defer cancel()
			enqueueStart := time.Now()
			for _, task := range taskList {
				replErr := replSvc.Replicate(replCtx, task)
				if replErr != nil {
					logger.Err(replErr).Msg("unable to handle replication")
				}
			}
			enqueueDuration = time.Since(enqueueStart)
		}
		// Forward response to original client
		for k, v := range resp.Header {
			w.Header().Set(k, v[0])
		}
		internalDuration := body.internalDuration()
		metrics.ProxyRequestInternalDuration(xctx.GetMethod(ctx).String(), storage, internalDuration)
		// counted before the body is passed on, so that a storage answer is
		// recorded even when the client gives up while it is being copied
		metrics.ProxyStorageStatus(storage, resp.StatusCode)
		w.WriteHeader(resp.StatusCode)
		written, err := io.Copy(w, resp.Body)
		if err != nil {
			logger.Err(err).Msg("unable to copy response body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// repl_enqueue_duration is the proxy side of a replication: recording
		// the new version and queueing the task. The replication itself
		// happens later, in a worker, which measures it there. Logged per
		// request so that latency can be followed without a metrics backend.
		duration := time.Since(start)
		metrics.ProxyRequestDuration(xctx.GetMethod(ctx).String(), storage, duration)
		timingFields(logger.Info(), timing).
			Str(log.Storage, storage).
			Dur("route_duration", routeDuration).
			Dur("internal_duration", internalDuration).
			Dur("duration", duration).
			Int("status", resp.StatusCode).
			Int64("bytes", written).
			Dur("repl_enqueue_duration", enqueueDuration).
			Msg("proxy: request done")
	})
}

// clientRequestBody notes when the request was last read from. An upload is
// read by the transport forwarding it, on a goroutine of its own, hence the
// atomic.
type clientRequestBody struct {
	io.ReadCloser
	lastRead atomic.Int64
}

// newClientRequestBody starts the note at arrival, which is where a request
// that nobody reads a body from was fully read.
func newClientRequestBody(body io.ReadCloser) *clientRequestBody {
	b := &clientRequestBody{ReadCloser: body}
	b.lastRead.Store(time.Now().UnixNano())
	return b
}

func (b *clientRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.lastRead.Store(time.Now().UnixNano())
	return n, err
}

// internalDuration is the time since the request was fully read.
func (b *clientRequestBody) internalDuration() time.Duration {
	return time.Since(time.Unix(0, b.lastRead.Load()))
}

// timingFields adds where the time of a request went: redis, name resolution,
// connecting and the wait for the storage. conn_reused tells a storage that is
// slow to answer from one we cannot get a connection to.
func timingFields(e *zerolog.Event, t *xctx.Timing) *zerolog.Event {
	redisDuration, redisCalls := t.Redis()
	dns, connect, tlsHandshake, ttfb := t.Network()
	conns, reused := t.Connection()
	e = e.Dur("redis_duration", redisDuration).Int64("redis_calls", redisCalls).
		Int64("conns", conns).Int64("conns_reused", reused)
	if dns != 0 {
		e = e.Dur("dns_duration", dns)
	}
	if connect != 0 {
		e = e.Dur("connect_duration", connect)
	}
	if tlsHandshake != 0 {
		e = e.Dur("tls_duration", tlsHandshake)
	}
	if ttfb != 0 {
		e = e.Dur("ttfb", ttfb)
	}
	if id := t.RequestID(); id != "" {
		e = e.Str("storage_request_id", id)
	}
	return e
}
