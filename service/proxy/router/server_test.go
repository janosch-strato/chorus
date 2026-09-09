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
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/tasks"
)

// slowReader hands out one byte per call, taking its time over it, the way a
// client sending a body over a slow line does.
type slowReader struct {
	left  int
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if s.left == 0 {
		return 0, io.EOF
	}
	time.Sleep(s.delay)
	s.left--
	p[0] = 'x'
	return 1, nil
}

func Test_clientRequestBody(t *testing.T) {
	const delay = 20 * time.Millisecond

	t.Run("the time the client took to send is not ours", func(t *testing.T) {
		r := require.New(t)
		body := newClientRequestBody(io.NopCloser(&slowReader{left: 3, delay: delay}))

		read, err := io.ReadAll(body)
		r.NoError(err)
		r.Len(read, 3)
		r.Less(body.internalDuration(), delay, "counted from the last read, not from the first")
	})

	t.Run("a request without a body starts when it arrives", func(t *testing.T) {
		r := require.New(t)
		body := newClientRequestBody(io.NopCloser(strings.NewReader("")))

		time.Sleep(delay)
		r.GreaterOrEqual(body.internalDuration(), delay, "nothing read, so everything since is ours")
	})
}

// countingListener counts the connections the storage had to accept. Keep
// alive holds that at one for a whole test, unless a response was left
// unread and its connection dropped instead of going back to the pool.
type countingListener struct {
	net.Listener
	accepts atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return conn, err
}

// closedBody is how much of a storage response had been read by the time the
// proxy closed it.
type closedBody struct {
	read, length int64
}

// watchedBody reports that over closes, which is also what lets a test wait
// for the proxy to be done with a response.
type watchedBody struct {
	io.ReadCloser
	length int64
	read   int64
	closes chan<- closedBody
}

func (b *watchedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.read += int64(n)
	return n, err
}

func (b *watchedBody) Close() error {
	err := b.ReadCloser.Close()
	b.closes <- closedBody{read: b.read, length: b.length}
	return err
}

// storageRouter answers every request from one storage over a single client,
// the way the proxy keeps one s3client per storage.
type storageRouter struct {
	client  *http.Client
	address string
	closes  chan closedBody
}

func (s *storageRouter) Route(r *http.Request) (*http.Response, []tasks.SyncTask, string, bool, error) {
	// Detached from the customer's request, as pkg/s3client does on purpose:
	// a customer disconnect must not cancel the storage request.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.address, nil)
	if err != nil {
		return nil, nil, "", false, err
	}
	resp, err := s.client.Do(req)
	if resp != nil {
		resp.Body = &watchedBody{ReadCloser: resp.Body, length: resp.ContentLength, closes: s.closes}
	}
	return resp, nil, "storage", false, err
}

type noReplication struct{}

func (noReplication) Replicate(context.Context, tasks.SyncTask) error { return nil }

// proxyOverSlowStorage puts the proxy in front of a storage that sends its
// body in paced chunks. The pacing is what leaves part of the response still
// on the wire when a customer disconnects, as a storage answering over a real
// network does.
func proxyOverSlowStorage(t *testing.T, chunks int) (proxy *httptest.Server, accepted *countingListener, closes chan closedBody) {
	t.Helper()
	const (
		chunkSize = 4096
		chunkGap  = 2 * time.Millisecond
	)

	storage := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(chunks*chunkSize))
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < chunks; i++ {
			if _, err := w.Write(make([]byte, chunkSize)); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(chunkGap)
		}
	}))
	accepted = &countingListener{Listener: storage.Listener}
	storage.Listener = accepted
	storage.Start()
	t.Cleanup(storage.Close)

	closes = make(chan closedBody, 1)
	proxy = httptest.NewServer(Serve(&storageRouter{
		client:  &http.Client{Transport: &http.Transport{}},
		address: storage.URL,
		closes:  closes,
	}, noReplication{}))
	t.Cleanup(proxy.Close)
	return proxy, accepted, closes
}

// waitClosed returns the next storage response the proxy was done with, so
// that a test moves on to its next request only once the one before it has
// been given back to the pool, or dropped.
func waitClosed(t *testing.T, closes chan closedBody) closedBody {
	t.Helper()
	select {
	case closed := <-closes:
		return closed
	case <-time.After(10 * time.Second):
		t.Fatal("the proxy never closed the storage response")
		return closedBody{}
	}
}

// disconnectMidStream asks for an object and drops the connection right after
// the first bytes of the answer, the way a customer whose line breaks does.
// Lingering for no time at all makes that a reset, so the proxy's write fails
// then and there instead of disappearing into a socket buffer.
func disconnectMidStream(t *testing.T, proxyURL string) {
	t.Helper()
	r := require.New(t)
	address := strings.TrimPrefix(proxyURL, "http://")

	conn, err := net.Dial("tcp", address)
	r.NoError(err)
	defer conn.Close()
	r.NoError(conn.(*net.TCPConn).SetLinger(0))

	_, err = fmt.Fprintf(conn, "GET /object HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", address)
	r.NoError(err)
	_, err = conn.Read(make([]byte, 64))
	r.NoError(err)
}

// smallAnswer is an answer go1.27 is willing to finish reading by itself once
// it is closed unread, given the 50ms it allows itself for that. go1.26 does
// no such thing and drops the connection whatever the size.
const smallAnswer = 8

func Test_storageConnectionReuse(t *testing.T) {
	const requests = 5

	t.Run("a customer reading the whole answer leaves the connection reusable", func(t *testing.T) {
		r := require.New(t)
		proxy, accepted, closes := proxyOverSlowStorage(t, smallAnswer)

		for i := 0; i < requests; i++ {
			resp, err := http.Get(proxy.URL)
			r.NoError(err)
			_, err = io.Copy(io.Discard, resp.Body)
			r.NoError(err)
			r.NoError(resp.Body.Close())

			closed := waitClosed(t, closes)
			r.Equal(closed.length, closed.read, "forwarding read the answer to its end")
		}
		r.EqualValues(1, accepted.accepts.Load(), "one storage connection for all of them")
	})

	t.Run("a customer disconnecting mid stream leaves the connection reusable", func(t *testing.T) {
		r := require.New(t)
		proxy, accepted, closes := proxyOverSlowStorage(t, smallAnswer)

		for i := 0; i < requests; i++ {
			disconnectMidStream(t, proxy.URL)

			closed := waitClosed(t, closes)
			r.Equal(closed.length, closed.read, "the rest of the answer was drained before closing")
		}
		r.EqualValues(1, accepted.accepts.Load(), "the disconnects cost no storage connection")
	})
}

// An answer no version of the standard library finishes reading for us once
// it is closed unread: go1.26 never does, and go1.27 only up to
// stdlibPostCloseDrain, so what this measures is our own drain and nothing
// else. Object reads through the proxy are routinely this size and larger.
const (
	largeAnswer          = 80
	stdlibPostCloseDrain = 256 << 10
	// Long enough for go1.27 to have finished a drain of its own, had it
	// attempted one, before the next request asks for a connection. Without
	// it a pass would only say that our drain got there first.
	stdlibDrainGrace = 200 * time.Millisecond
)

func Test_storageConnectionReuse_beyondStdlibDrain(t *testing.T) {
	const requests = 3
	r := require.New(t)
	r.Greater(largeAnswer*4096, stdlibPostCloseDrain,
		"the answer has to be past what the standard library drains, or this tests that instead")

	proxy, accepted, closes := proxyOverSlowStorage(t, largeAnswer)
	for i := 0; i < requests; i++ {
		disconnectMidStream(t, proxy.URL)
		waitClosed(t, closes)
		time.Sleep(stdlibDrainGrace)
	}

	// Only the count: what the drain read is the other test's business, so a
	// failure here says the connection was lost and nothing else.
	r.EqualValues(1, accepted.accepts.Load(), "the disconnects cost no storage connection")
}
