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

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/trace"
)

// The size is representative of an object read through the proxy, and well
// inside the drain's own cap.
//
// It is not a threshold: the standard library never finishes an answer for us
// here, whatever its size. A response body closed before EOF takes
// bodyEOFSignal.Close's early path, which tells the transport the body was not
// read out and the connection is not pooled. The 256KB drain is the server
// side reading out a request body after its handler returned
// (maxPostHandlerReadBytes), and the 2KB one is a redirect slurping the
// response it is leaving behind (maxBodySlurpSize). Neither runs on this path,
// so what this measures is our own drain at any size.
const abandonedAnswer = 512 << 10

// newPacedStorage answers object reads in chunks, with a gap between them. The
// pacing is what leaves part of the answer still on the wire when a customer
// disconnects, as a storage answering over a real network does.
//
// The returned counter is of connections the storage had to accept. Keep alive
// holds that still, unless an answer was left unread and its connection
// dropped instead of going back to the pool.
func newPacedStorage(t *testing.T, size int) (*httptest.Server, *atomic.Int64, chan int) {
	t.Helper()
	const chunkSize = 8 << 10

	accepts := &atomic.Int64{}
	// How much of an answer the storage got to write. The drain is below the
	// body the handler closes, so it cannot be seen from up there; from here
	// it is the difference between an answer that was taken and one cut off.
	wrote := make(chan int, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A client is only built once the storage answers a probe, which asks
		// for a bucket location. Anything but a network failure counts as
		// reachable, so a bare 404 does.
		if r.URL.Query().Has("location") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Without it Go chunks the answer, and the length is what the drain
		// weighs against its cap.
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.WriteHeader(http.StatusOK)
		sent := 0
		defer func() { wrote <- sent }()
		for sent < size {
			n, err := w.Write(make([]byte, min(chunkSize, size-sent)))
			sent += n
			if err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(2 * time.Millisecond)
		}
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			accepts.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, accepts, wrote
}

// newStorageClient builds the real thing, so that the answer the router hands
// back is wrapped as s3client wraps it.
func newStorageClient(t *testing.T, storage *httptest.Server) s3client.Client {
	t.Helper()
	r := require.New(t)

	ctx, shutdown := context.WithCancel(context.Background())
	t.Cleanup(shutdown) // stops the health check goroutine
	const user = "user"
	ctx = xctx.SetUser(ctx, user)

	_, tp, err := trace.NewTracerProvider(&trace.Config{}, dom.AppInfo{})
	r.NoError(err)
	clients, err := s3client.New(ctx, &s3.StorageConfig{
		Storages: map[string]s3.Storage{testSource: {
			Address:     s3.NewConfAddr(strings.TrimPrefix(storage.URL, "http://")),
			Credentials: map[string]s3.CredentialsV4{user: {AccessKeyID: "key", SecretAccessKey: "secret"}},
			IsMain:      true,
			// Long enough not to probe again during a test.
			HealthCheckInterval: time.Hour,
		}},
	}, metrics.NewS3Service(false), tp)
	r.NoError(err)

	c, err := clients.GetByName(ctx, testSource)
	r.NoError(err)
	return c
}

// watchedBody says when the handler is done with an answer, which is what
// lets a test wait. It wraps the answer s3client gave, so closing it closes
// that one, drain and all, before the test is told.
type watchedBody struct {
	io.ReadCloser
	closed chan<- struct{}
}

func (b *watchedBody) Close() error {
	err := b.ReadCloser.Close()
	b.closed <- struct{}{}
	return err
}

// What can be given up on is the answer underneath, so the wrapper hands it
// on. A wrapper in the proxy proper would have to do the same.
func (b *watchedBody) Abort() {
	if answer, ok := b.ReadCloser.(s3client.Abortable); ok {
		answer.Abort()
	}
}

// storageRouter asks the storage for an object over the real client, the way
// the proxy keeps one s3client per storage.
type storageRouter struct {
	client s3client.Client
	url    string
	closed chan struct{}
}

func (s *storageRouter) Route(r *http.Request) (*http.Response, []tasks.SyncTask, string, bool, error) {
	ctx := xctx.SetMethod(r.Context(), s3.GetObject)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url+"/"+testBucket+"/obj", nil)
	if err != nil {
		return nil, nil, testSource, false, err
	}
	resp, isApiErr, err := s.client.Do(req)
	if resp != nil {
		resp.Body = &watchedBody{ReadCloser: resp.Body, closed: s.closed}
	}
	return resp, nil, testSource, isApiErr, err
}

// newProxyOver puts the real Serve in front of the storage.
func newProxyOver(t *testing.T, storage *httptest.Server) (*httptest.Server, chan struct{}) {
	t.Helper()
	closed := make(chan struct{}, 1)
	proxy := httptest.NewServer(Serve(&storageRouter{
		client: newStorageClient(t, storage),
		url:    storage.URL,
		closed: closed,
	}, noReplication{}))
	t.Cleanup(proxy.Close)
	return proxy, closed
}

// waitClosed returns the answer the handler was just done with, so that a test
// asks for the next one only once this one has gone back to the pool, or been
// dropped. Measuring reuse rather than how the two race.
func waitClosed(t *testing.T, closed chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler never closed the storage answer")
	}
}

// readItAll asks through the proxy and reads the answer to its end.
func readItAll(t *testing.T, proxyURL string) {
	t.Helper()
	r := require.New(t)
	resp, err := http.Get(proxyURL + "/" + testBucket + "/obj")
	r.NoError(err)
	_, err = io.Copy(io.Discard, resp.Body)
	r.NoError(err)
	r.NoError(resp.Body.Close())
}

// disconnectMidStream asks for an object and drops the connection right after
// the first bytes of the answer, the way a customer whose line breaks does.
// Lingering for no time at all makes that a reset, so the handler's write
// fails then and there instead of disappearing into a socket buffer.
func disconnectMidStream(t *testing.T, proxyURL string) {
	t.Helper()
	r := require.New(t)
	address := strings.TrimPrefix(proxyURL, "http://")

	conn, err := net.Dial("tcp", address)
	r.NoError(err)
	defer conn.Close()
	r.NoError(conn.(*net.TCPConn).SetLinger(0))

	_, err = fmt.Fprintf(conn, "GET /%s/obj HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
		testBucket, address)
	r.NoError(err)
	_, err = conn.Read(make([]byte, 64))
	r.NoError(err)
}

// The whole path: a customer that resets its connection mid answer must leave
// the storage connection reusable, which takes both the read outliving the
// customer and the rest of the answer being read out.
func TestACustomerDisconnectCostsNoStorageConnection(t *testing.T) {
	const requests = 5
	r := require.New(t)

	storage, accepts, wrote := newPacedStorage(t, abandonedAnswer)
	proxy, closed := newProxyOver(t, storage)

	// One request seen through, so that what the disconnects are measured
	// against is a connection that exists and is pooled.
	readItAll(t, proxy.URL)
	waitClosed(t, closed)
	r.Equal(abandonedAnswer, <-wrote)
	pooled := accepts.Load()

	for i := 0; i < requests; i++ {
		disconnectMidStream(t, proxy.URL)

		waitClosed(t, closed)
		r.Equal(abandonedAnswer, <-wrote, "the rest of the answer was left on the wire")
	}
	r.EqualValues(pooled, accepts.Load(), "the disconnects cost a storage connection each")
}

// The counter is only still because the answers were finished, not because it
// cannot move.
func TestACustomerReadingItAllKeepsOneStorageConnection(t *testing.T) {
	const requests = 5
	r := require.New(t)

	const size = 64 << 10
	storage, accepts, wrote := newPacedStorage(t, size)
	proxy, closed := newProxyOver(t, storage)

	readItAll(t, proxy.URL)
	waitClosed(t, closed)
	r.Equal(size, <-wrote)
	pooled := accepts.Load()

	for i := 0; i < requests; i++ {
		readItAll(t, proxy.URL)
		waitClosed(t, closed)
		r.Equal(size, <-wrote, "forwarding stopped short of the answer's end")
	}
	r.EqualValues(pooled, accepts.Load(), "one storage connection for all of them")
}

// newStallingStorage answers with the head of an object and then stops, the
// way a storage that hangs mid answer does.
//
// The channel says when it has stopped, so that a test disconnects at the one
// moment that matters: with the proxy waiting on a read and nothing left to
// write to the customer, which is where the customer going is unnoticed.
//
// Letting the storage go is the caller's to defer rather than a cleanup of
// its own, because closing either server waits on the handlers still in them.
// A deferred call runs before every cleanup, a failed assertion included, so
// a test that finds the read still held ends saying so instead of hanging on
// it.
