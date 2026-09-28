package s3client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResponseHeaderTimeoutSpansOnlyTheSilence(t *testing.T) {
	const bound = 150 * time.Millisecond

	client := &http.Client{Transport: newS3Transport("test", bound)} // no request timeout, as ours have none

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
