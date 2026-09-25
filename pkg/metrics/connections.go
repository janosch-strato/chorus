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

package metrics

import (
	"context"
	"net"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Connection metrics. The "layer" label distinguishes where the
// socket lives so the http transport we own and other pools (e.g.
// rclone) can be told apart on one set of series.
const (
	ConnLayerHTTP = "http"
	ConnLayerPool = "pool"
)

var connOpened = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_connections_opened_total",
		Help: "Number of connections opened to s3 storage.",
	},
	[]string{"storage", "layer"},
)

var connClosed = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_connections_closed_total",
		Help: "Number of connections to s3 storage that were closed.",
	},
	[]string{"storage", "layer"},
)

var connOpen = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "storage_connections_open",
		Help: "Number of currently open connections to s3 storage.",
	},
	[]string{"storage", "layer"},
)

// DialFunc is the shape of net/http.Transport.DialContext.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// InstrumentedDial wraps a base dialer so each successfully opened
// connection bumps the opened counter and the open gauge, and the
// matching Close bumps the closed counter and decrements the gauge.
// Counting happens once per connection even if Close is called more
// than once.
func InstrumentedDial(storage, layer string, base DialFunc) DialFunc {
	opened := connOpened.WithLabelValues(storage, layer)
	closed := connClosed.WithLabelValues(storage, layer)
	open := connOpen.WithLabelValues(storage, layer)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := base(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		opened.Inc()
		open.Inc()
		return &countedConn{
			Conn: conn,
			onClose: func() {
				closed.Inc()
				open.Dec()
			},
		}, nil
	}
}

// countedConn invokes onClose exactly once, on the first Close, before
// delegating to the wrapped connection.
type countedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *countedConn) Close() error {
	c.once.Do(c.onClose)
	return c.Conn.Close()
}
