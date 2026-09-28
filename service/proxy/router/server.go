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
	"context"
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
	"github.com/clyso/chorus/pkg/tasks"
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
		// Before anything can return: a route that failed may still hold a
		// connection open, and the paths below leave by more than one door.
		defer func() {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}()
		if err != nil {
			metrics.ProxyRequestInternalDuration(xctx.GetMethod(ctx).String(), storage, body.internalDuration())
			event := logger.Info().Err(err).
				Str(log.Storage, storage).
				Dur("route_duration", routeDuration).
				Int("repl_tasks", replicate(ctx, replSvc, taskList, logger))
			// The client is still there and has to be told.
			if r.Context().Err() != nil {
				timingFields(event, timing).Msg("proxy: client disconnected")
				return
			}
			timingFields(event, timing).Msg("proxy: request failed")
			util.WriteError(r.Context(), w, err)
			return
		}
		var enqueueDuration time.Duration
		ctx = log.WithStorage(ctx, storage)
		// TODO: is it reachable? This branch is active only if err == nil
		if isApiErr {
			zerolog.Ctx(ctx).Info().Err(err).Msg("s3 api error returned")
			// create replication tasks according to replication rules
		} else {
			enqueueStart := time.Now()
			replicate(ctx, replSvc, taskList, logger)
			enqueueDuration = time.Since(enqueueStart)
		}
		// Nobody to hand the answer to; the replication above was what
		// mattered.
		if r.Context().Err() != nil {
			timingFields(logger.Info(), timing).
				Str(log.Storage, storage).
				Dur("route_duration", routeDuration).
				Int("status", resp.StatusCode).
				Dur("repl_enqueue_duration", enqueueDuration).
				Msg("proxy: client disconnected")
			return
		}
		// Forward response to original client
		for k, v := range resp.Header {
			w.Header().Set(k, v[0])
		}
		internalDuration := body.internalDuration()
		metrics.ProxyRequestInternalDuration(xctx.GetMethod(ctx).String(), storage, internalDuration)
		w.WriteHeader(resp.StatusCode)
		written, err := io.Copy(w, resp.Body)
		if err != nil {
			// The status is out, so there is no telling the client.
			logger.Err(err).Int64("bytes", written).Msg("unable to copy response body")
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

// On a context of its own: the change is on the storage, so recording it must
// not depend on the client that asked still being there.
func replicate(ctx context.Context, replSvc replication.Service, taskList []tasks.SyncTask, logger *zerolog.Logger) int {
	if len(taskList) == 0 {
		return 0
	}
	replCtx := context.WithoutCancel(ctx)
	var enqueued int
	for _, task := range taskList {
		if err := replSvc.Replicate(replCtx, task); err != nil {
			logger.Err(err).Msg("unable to handle replication")
			continue
		}
		enqueued++
	}
	return enqueued
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
