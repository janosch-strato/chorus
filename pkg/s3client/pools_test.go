package s3client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoragePools(t *testing.T) {
	const answerStart = 5 * time.Minute

	r := require.New(t)
	pools := newStoragePools(answerStart)

	t.Run("The two pools are separate", func(t *testing.T) {
		// A storage hanging on the answer pool must not take the connections
		// of the ordinary traffic.
		r.NotSame(pools.shared, pools.answer, "both clients would share one pool")
	})

	t.Run("The answer pool is capped, because nothing else bounds it", func(t *testing.T) {
		// They have no timeout, so connections are the only bound.
		r.EqualValues(s3AnswerMaxConns, pools.answer.MaxConnsPerHost)
		r.EqualValues(s3AnswerMaxIdleConnsPerHost, pools.answer.MaxIdleConnsPerHost)
		r.False(pools.answer.ForceAttemptHTTP2,
			"one http/2 connection carries any number of requests, which would leave the cap bounding nothing")
	})

	t.Run("The shared pool is unchanged", func(t *testing.T) {
		r.Zero(pools.shared.MaxConnsPerHost, "the ordinary traffic is bounded by the http timeout, not by a cap")
		r.EqualValues(s3MaxIdleConnsPerHost, pools.shared.MaxIdleConnsPerHost)
		r.True(pools.shared.ForceAttemptHTTP2)
	})

	t.Run("Only the shared pool bounds the silence before an answer", func(t *testing.T) {
		r.EqualValues(answerStart, pools.shared.ResponseHeaderTimeout,
			"the configured bound did not reach the pool it is for")
		r.Zero(pools.answer.ResponseHeaderTimeout,
			"a request whose answer is the only record of its outcome was given up on")
	})

	// http.Transport would give up on every request at once.
	t.Run("A negative bound is none", func(t *testing.T) {
		r.Zero(newStoragePools(-1).shared.ResponseHeaderTimeout,
			"a storage asked for no bound and got one that fires immediately")
	})

	t.Run("Both inherit the settings they have in common", func(t *testing.T) {
		r.Equal(pools.shared.IdleConnTimeout, pools.answer.IdleConnTimeout)
		r.Equal(pools.shared.TLSHandshakeTimeout, pools.answer.TLSHandshakeTimeout)
		r.Equal(pools.shared.ExpectContinueTimeout, pools.answer.ExpectContinueTimeout)
	})
}

// The bound is on the silence, not on the request: an object has no size limit.
func TestResponseHeaderTimeoutSpansOnlyTheSilence(t *testing.T) {
	const bound = 150 * time.Millisecond

	client := &http.Client{Transport: newS3Transport(bound)} // no request timeout, as ours have none

	t.Run("An upload slower than it still gets through", func(t *testing.T) {
		r := require.New(t)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.Copy(io.Discard, req.Body)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		body := &drip{chunks: 6, every: bound / 2}
		req, err := http.NewRequest(http.MethodPut, srv.URL, body)
		r.NoError(err)
		req.ContentLength = int64(body.chunks)

		resp, err := client.Do(req)
		r.NoError(err, "a slow upload was cut off by a bound meant for silence")
		_ = resp.Body.Close()
	})

	t.Run("A storage that says nothing is given up on", func(t *testing.T) {
		r := require.New(t)

		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.Copy(io.Discard, req.Body)
			<-release // took the request and answers nothing
		}))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(release) })

		start := time.Now()
		resp, err := client.Get(srv.URL)
		if resp != nil {
			_ = resp.Body.Close()
		}

		r.Error(err, "a storage that never answered was waited on for ever")
		r.Less(time.Since(start), 5*time.Second)
	})
}

type drip struct {
	chunks int
	every  time.Duration
	sent   int
}

func (d *drip) Read(p []byte) (int, error) {
	if d.sent == d.chunks {
		return 0, io.EOF
	}
	time.Sleep(d.every)
	d.sent++
	p[0] = 'x'
	return 1, nil
}
