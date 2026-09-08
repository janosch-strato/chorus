package s3client

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

type countingStorage struct {
	*httptest.Server
	accepts *atomic.Int64
}

func newCountingStorage(t *testing.T, bodySize int) countingStorage {
	t.Helper()
	accepts := &atomic.Int64{}
	body := strings.Repeat("x", bodySize)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Without it Go chunks anything past its write buffer, and the length
		// is what decides here.
		w.Header().Set("Content-Length", strconv.Itoa(bodySize))
		_, _ = io.WriteString(w, body)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			accepts.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return countingStorage{Server: srv, accepts: accepts}
}

// Reports whether the next request could reuse the connection.
func (s countingStorage) abandon(t *testing.T, r *require.Assertions) (reused, cancelled bool) {
	t.Helper()
	client := s.Client()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled = false
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	r.NoError(err)
	resp, err := client.Do(req)
	r.NoError(err)

	body := newDrainingBody(resp.Body, resp.ContentLength, func() { cancelled = true; cancel() })
	_, err = io.ReadFull(body, make([]byte, 8)) // as far as the customer got
	r.NoError(err)
	r.NoError(body.Close())

	traced := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused },
	})
	next, err := http.NewRequestWithContext(traced, http.MethodGet, s.URL, nil)
	r.NoError(err)
	resp2, err := client.Do(next)
	r.NoError(err)
	_, _ = io.Copy(io.Discard, resp2.Body)
	r.NoError(resp2.Body.Close())
	return reused, cancelled
}

// A connection is only reusable once its answer has been read to the end.
func TestDrainingBodyKeepsTheConnection(t *testing.T) {
	r := require.New(t)

	storage := newCountingStorage(t, maxDrainBytes/2)
	reused, cancelled := storage.abandon(t, r)

	r.True(reused, "the connection was dropped although finishing the answer was cheap")
	r.False(cancelled, "an answer worth finishing was given up on instead")
	r.EqualValues(1, storage.accepts.Load(), "the storage had to accept a second connection")
}

func TestDrainingBodyGivesUpOnALargeAnswer(t *testing.T) {
	r := require.New(t)

	storage := newCountingStorage(t, maxDrainBytes+1<<12)
	reused, cancelled := storage.abandon(t, r)

	r.False(reused, "a whole object was read out just to keep a connection")
	r.True(cancelled, "the request was left running after its answer was abandoned")
	r.EqualValues(2, storage.accepts.Load())
}

func TestDrainingBodyReadsOutAnAnswerOfUnknownLength(t *testing.T) {
	r := require.New(t)

	cancelled := false
	short := strings.NewReader(strings.Repeat("x", 1<<10))
	body := newDrainingBody(io.NopCloser(short), -1, func() { cancelled = true })

	r.NoError(body.Close())
	r.False(cancelled, "an answer that was short enough to finish was given up on")
	r.Zero(short.Len(), "the answer was not read out")
}

func TestDrainingBodyReadsOutALongAnswerOfUnknownLength(t *testing.T) {
	r := require.New(t)

	cancelled := false
	long := strings.NewReader(strings.Repeat("x", maxDrainBytes+1<<10))
	body := newDrainingBody(io.NopCloser(long), -1, func() { cancelled = true })

	r.NoError(body.Close())
	r.False(cancelled, "an answer of unknown length was given up on for its size")
	r.Zero(long.Len(), "the answer was not read out")
}

// Nobody is waiting on this read, so it is not left to the storage's request
// timeout, which is measured in minutes.
func TestDrainingBodyIsBoundedByItsBudget(t *testing.T) {
	r := require.New(t)

	const budget = 200 * time.Millisecond
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "start")
		w.(http.Flusher).Flush()
		<-release // a storage that goes quiet mid answer
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	// No client timeout, so only the budget can end this.
	client := srv.Client()
	r.Zero(client.Timeout)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	r.NoError(err)
	resp, err := client.Do(req)
	r.NoError(err)
	r.EqualValues(-1, resp.ContentLength, "the storage was supposed to leave the length unsaid")

	body := newDrainingBody(resp.Body, resp.ContentLength, cancel)
	body.budget = budget
	_, err = io.ReadFull(body, make([]byte, 5))
	r.NoError(err)

	start := time.Now()
	r.NoError(body.Close())
	waited := time.Since(start)

	r.Less(waited, 5*time.Second, "an answer that never ends was not bounded at all")
	r.GreaterOrEqual(waited, budget, "the answer was not read out for as long as it was allowed")
	r.Less(waited, budget*5, "the reading outlasted its budget")
}

func TestDrainingBodyCountsWhatWasRead(t *testing.T) {
	r := require.New(t)

	cancelled := false
	size := maxDrainBytes + 16
	body := newDrainingBody(io.NopCloser(strings.NewReader(strings.Repeat("x", size))),
		int64(size), func() { cancelled = true })

	_, err := io.ReadFull(body, make([]byte, 32))
	r.NoError(err)
	r.NoError(body.Close())
	r.False(cancelled, "what had already been read was not counted against the budget")
}

// The request host matches the storage address, so Do forwards it as-is
// rather than signing it.
func testClient(t *testing.T, srv *httptest.Server) *client {
	t.Helper()
	return &client{
		c:          srv.Client(),
		conf:       s3.Storage{Address: s3.NewConfAddr(srv.URL)},
		name:       "storage",
		user:       "user",
		online:     &atomic.Bool{},
		metricsSvc: metrics.NewS3Service(false),
	}
}

// The two halves together: a read whose customer walks away keeps its
// connection, which it cannot do if the customer's leaving cancels it.
func TestDoKeepsTheConnectionOfAnAbandonedRead(t *testing.T) {
	r := require.New(t)

	storage := newCountingStorage(t, maxDrainBytes/2)
	c := testClient(t, storage.Server)

	customerCtx, customerLeft := context.WithCancel(context.Background())
	ctx := xctx.SetMethod(customerCtx, s3.GetObject)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, storage.URL+"/buck/obj", nil)
	r.NoError(err)

	resp, isApiErr, err := c.Do(req)
	r.NoError(err)
	r.False(isApiErr)

	_, err = io.ReadFull(resp.Body, make([]byte, 8)) // as far as the customer got
	r.NoError(err)
	customerLeft()
	r.NoError(resp.Body.Close())

	var reused bool
	traced := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused },
	})
	next, err := http.NewRequestWithContext(traced, http.MethodGet, storage.URL, nil)
	r.NoError(err)
	resp2, err := storage.Client().Do(next)
	r.NoError(err)
	_, _ = io.Copy(io.Discard, resp2.Body)
	r.NoError(resp2.Body.Close())

	r.True(reused, "the customer leaving took the storage connection with it")
	r.EqualValues(1, storage.accepts.Load(), "the storage had to accept a second connection")
}

func TestDrainingBodyDoesNotReadALengthItWasTold(t *testing.T) {
	r := require.New(t)

	cancelled := false
	long := strings.NewReader(strings.Repeat("x", maxDrainBytes+1<<10))
	before := long.Len()
	body := newDrainingBody(io.NopCloser(long), int64(before), func() { cancelled = true })

	r.NoError(body.Close())
	r.True(cancelled)
	r.Equal(before, long.Len(), "an answer the storage said was too long was read out anyway")
}

// A read that finished does not take its connection away afterwards. net/http
// would forgive a timer left running, so this pins the property, not that.
func TestDrainingBodyDoesNotCancelAfterItFinished(t *testing.T) {
	r := require.New(t)

	const budget = 50 * time.Millisecond
	storage := newCountingStorage(t, 1<<10)
	client := storage.Client()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, storage.URL, nil)
	r.NoError(err)
	resp, err := client.Do(req)
	r.NoError(err)

	body := newDrainingBody(resp.Body, resp.ContentLength, cancel)
	body.budget = budget
	_, err = io.ReadFull(body, make([]byte, 8))
	r.NoError(err)
	r.NoError(body.Close()) // finishes well inside the budget

	time.Sleep(budget * 4) // long enough for a timer that was left running

	var reused bool
	traced := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused },
	})
	next, err := http.NewRequestWithContext(traced, http.MethodGet, storage.URL, nil)
	r.NoError(err)
	resp2, err := client.Do(next)
	r.NoError(err)
	_, _ = io.Copy(io.Discard, resp2.Body)
	r.NoError(resp2.Body.Close())

	r.True(reused, "the connection was taken away after the read that borrowed it had finished")
	r.EqualValues(1, storage.accepts.Load())
}

// What AbortResponse reaches for is the answer's own way of giving up, which
// is the only thing that ends a read the storage has left waiting.
func TestAbortResponseEndsTheRequestAnAnswerCameOn(t *testing.T) {
	r := require.New(t)

	ended := false
	body := newDrainingBody(io.NopCloser(strings.NewReader("answer")), 6, func() { ended = true })

	AbortResponse(&http.Response{Body: body})
	r.True(ended, "the request the answer came on was left running")
}

// An answer already in memory, an api error's among them, has no request left
// to end, and neither has one that never arrived.
func TestAbortResponseLeavesAnAnswerWithNoRequestBehindIt(t *testing.T) {
	r := require.New(t)

	r.NotPanics(func() { AbortResponse(nil) })
	r.NotPanics(func() { AbortResponse(&http.Response{}) })
	r.NotPanics(func() {
		AbortResponse(&http.Response{Body: io.NopCloser(strings.NewReader("an api error"))})
	})
	r.NotPanics(func() {
		AbortResponse(&http.Response{Body: newDrainingBody(io.NopCloser(strings.NewReader("x")), 1, nil)})
	})
}
