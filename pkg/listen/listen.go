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

package listen

import (
	"context"
	"net"
	"net/http"
	"syscall"

	"golang.org/x/sys/unix"
)

// reusePort is set once from main, before any goroutine of this process runs,
// and is read only afterwards. That ordering is what makes a plain variable
// safe here, and it is also the only sensible use: a process is rolled out one
// way or the other, and a mix of shared and exclusive ports could be taken
// over on some of them only.
var reusePort bool

// SetReusePort lets the listeners of this process share their ports with
// another process, so a new instance can be started and be serving before the
// running one is stopped. Without it a restart refuses connections between the
// old process releasing the port and the new one binding it.
//
// It also removes the "address already in use" that otherwise stops a second
// instance of the same deployment from starting by accident.
//
// Call it from main before starting anything.
func SetReusePort(enabled bool) {
	reusePort = enabled
}

// Listen listens on a tcp address, sharing the port when SetReusePort is on.
func Listen(ctx context.Context, addr string) (net.Listener, error) {
	config := net.ListenConfig{}
	if reusePort {
		config.Control = func(_, _ string, conn syscall.RawConn) error {
			var sockErr error
			if err := conn.Control(func(fd uintptr) {
				sockErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return sockErr
		}
	}
	return config.Listen(ctx, "tcp", addr)
}

// ServeHTTP serves srv on a listener from Listen, replacing
// http.Server.ListenAndServe.
func ServeHTTP(ctx context.Context, srv *http.Server) error {
	listener, err := Listen(ctx, srv.Addr)
	if err != nil {
		return err
	}
	return srv.Serve(listener)
}
