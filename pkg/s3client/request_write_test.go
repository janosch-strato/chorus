package s3client

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

func requireCancelled(t *testing.T, r *require.Assertions, reqCtx context.Context) {
	t.Helper()
	select {
	case <-reqCtx.Done():
		r.ErrorIs(reqCtx.Err(), context.Canceled)
	case <-time.After(5 * time.Second):
		r.Fail("the request of a disconnected client was not given up")
	}
}

func requireAlive(t *testing.T, r *require.Assertions, reqCtx context.Context) {
	t.Helper()
	select {
	case <-reqCtx.Done():
		r.Fail("a request that may have reached the storage was cancelled")
	case <-time.After(100 * time.Millisecond):
	}
	r.NoError(reqCtx.Err())
}

type stallingBody struct {
	reading chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *stallingBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	<-b.release
	return 0, io.ErrUnexpectedEOF
}

func listenAndServe(t *testing.T, r *require.Assertions, h http.HandlerFunc) string {
	t.Helper()
	srv := http.Server{Handler: h}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	r.NoError(err)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(func() { _ = srv.Close() })
	return listener.Addr().String()
}

func TestCancelUntilWritten(t *testing.T) {
	t.Run("A client that leaves before the request was written cancels it", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, _ := cancelUntilWritten(clientCtx, true)
		r.NoError(reqCtx.Err())

		disconnect()
		requireCancelled(t, r, reqCtx)
	})

	t.Run("An upload is given up on until its last byte is written", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, _ := cancelUntilWritten(clientCtx, true)
		r.Nil(httptrace.ContextClientTrace(reqCtx).WroteHeaders)

		disconnect()
		requireCancelled(t, r, reqCtx)
	})

	t.Run("An upload that is written in full is waited out", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, write := cancelUntilWritten(clientCtx, true)
		httptrace.ContextClientTrace(reqCtx).WroteRequest(httptrace.WroteRequestInfo{})

		disconnect()
		requireAlive(t, r, reqCtx)
		r.ErrorIs(write.classify(io.ErrUnexpectedEOF), dom.ErrStorageAnswerLost)
	})

	t.Run("A delete is waited out once its headers are written", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, _ := cancelUntilWritten(clientCtx, false)
		httptrace.ContextClientTrace(reqCtx).WroteHeaders()

		disconnect()
		requireAlive(t, r, reqCtx)
	})

	// Only the cancellation is meant to be dropped.
	t.Run("The request keeps what its client context carried", func(t *testing.T) {
		r := require.New(t)

		type key struct{}
		clientCtx, disconnect := context.WithCancel(context.WithValue(context.Background(), key{}, "carried"))
		defer disconnect()
		reqCtx, _, _ := cancelUntilWritten(clientCtx, false)

		r.Equal("carried", reqCtx.Value(key{}))
	})

	t.Run("A disconnect while connecting ends the request", func(t *testing.T) {
		r := require.New(t)

		dialing := make(chan struct{})
		client := &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(dialing)
				<-ctx.Done() // a storage that cannot be connected to
				return nil, ctx.Err()
			},
		}}

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, write := cancelUntilWritten(clientCtx, false)
		req, err := http.NewRequestWithContext(reqCtx,
			http.MethodDelete, "http://storage.invalid/bucket/object", http.NoBody)
		r.NoError(err)

		done := make(chan error, 1)
		go func() {
			resp, err := client.Do(req)
			if resp != nil {
				_ = resp.Body.Close()
			}
			done <- err
		}()

		<-dialing
		disconnect()
		select {
		case doErr := <-done:
			r.ErrorIs(doErr, context.Canceled)
			r.ErrorIs(write.classify(doErr), dom.ErrClientGone)
		case <-time.After(5 * time.Second):
			r.Fail("the request kept connecting for a client that left")
		}
	})

	t.Run("A storage slow to answer is waited out after its client left", func(t *testing.T) {
		r := require.New(t)

		uploaded := make(chan struct{})
		answer := make(chan struct{})
		addr := listenAndServe(t, r, func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.Copy(io.Discard, req.Body)
			close(uploaded)
			<-answer // a storage that has the request and takes its time
			_, _ = w.Write([]byte("<DeleteResult/>"))
		})

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, write := cancelUntilWritten(clientCtx, true)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
			"http://"+addr+"/bucket?delete", strings.NewReader("payload"))
		r.NoError(err)

		type result struct {
			body []byte
			err  error
		}
		done := make(chan result, 1)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				done <- result{err: err}
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			done <- result{body: body, err: err}
		}()

		<-uploaded
		disconnect()
		time.Sleep(200 * time.Millisecond)
		close(answer)

		select {
		case got := <-done:
			r.NoError(got.err, "the answer was not waited for")
			r.EqualValues("<DeleteResult/>", string(got.body))
			r.NoError(write.classify(nil))
		case <-time.After(5 * time.Second):
			r.Fail("the answer never arrived")
		}
	})

	t.Run("A storage that dies without answering may still have carried it out", func(t *testing.T) {
		r := require.New(t)

		uploaded := make(chan struct{})
		addr := listenAndServe(t, r, func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.Copy(io.Discard, req.Body)
			close(uploaded)
			conn, _, hijackErr := w.(http.Hijacker).Hijack()
			if hijackErr == nil {
				_ = conn.Close()
			}
		})

		clientCtx, disconnect := context.WithCancel(context.Background())
		defer disconnect()
		reqCtx, _, write := cancelUntilWritten(clientCtx, true)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
			"http://"+addr+"/bucket?delete", strings.NewReader("payload"))
		r.NoError(err)

		done := make(chan error, 1)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if resp != nil {
				_ = resp.Body.Close()
			}
			done <- err
		}()

		<-uploaded
		select {
		case doErr := <-done:
			r.Error(doErr)
			// The client is still there, so this is the storage's failure.
			r.ErrorIs(write.classify(doErr), dom.ErrStorageAnswerLost)
			r.NotErrorIs(write.classify(doErr), dom.ErrClientGone)
		case <-time.After(5 * time.Second):
			r.Fail("the request did not end with the storage")
		}
	})
}

func TestCancelUntilWrittenRecord(t *testing.T) {
	// Past the write the storage is already carrying the change out, so
	// cancelling only turns a settled outcome into one nobody knows.
	t.Run("An upload that is out is carried out although its client left", func(t *testing.T) {
		r := require.New(t)

		committed := make(chan struct{})
		addr := listenAndServe(t, r, func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.Copy(io.Discard, req.Body)
			time.Sleep(200 * time.Millisecond)
			close(committed)
			w.WriteHeader(http.StatusOK)
		})

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, write := cancelUntilWritten(clientCtx, true)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPut,
			"http://"+addr+"/bucket/object", strings.NewReader("payload"))
		r.NoError(err)

		done := make(chan error, 1)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if resp != nil {
				_ = resp.Body.Close()
			}
			done <- err
		}()

		time.Sleep(50 * time.Millisecond)
		disconnect()

		select {
		case doErr := <-done:
			r.NoError(doErr, "the upload was aborted after it had been sent")
			r.NoError(write.classify(doErr), "a completed upload was reported as unfinished")
			select {
			case <-committed:
			default:
				r.Fail("the storage never got to commit the upload")
			}
		case <-time.After(5 * time.Second):
			r.Fail("the upload never finished")
		}
	})

	t.Run("A disconnect during an upload aborts it at the storage", func(t *testing.T) {
		r := require.New(t)

		read := make(chan error, 1)
		addr := listenAndServe(t, r, func(_ http.ResponseWriter, req *http.Request) {
			_, err := io.Copy(io.Discard, req.Body)
			read <- err
		})

		stalled := &stallingBody{reading: make(chan struct{}), release: make(chan struct{})}
		t.Cleanup(func() { close(stalled.release) })

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, _, _ := cancelUntilWritten(clientCtx, true)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPut,
			"http://"+addr+"/bucket/object",
			io.MultiReader(strings.NewReader("head"), stalled))
		r.NoError(err)
		req.ContentLength = -1
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}()

		<-stalled.reading
		disconnect()
		requireCancelled(t, r, reqCtx)
		select {
		case err := <-read:
			r.Error(err, "the storage was given a complete upload to answer")
		case <-time.After(5 * time.Second):
			r.Fail("the upload did not end at the storage")
		}
	})

	t.Run("An upload counts as written when its last byte is out", func(t *testing.T) {
		r := require.New(t)

		reqCtx, _, write := cancelUntilWritten(context.Background(), true)
		httptrace.ContextClientTrace(reqCtx).WroteRequest(httptrace.WroteRequestInfo{})
		r.True(write.settled)
	})

	t.Run("An upload that failed to be written does not count", func(t *testing.T) {
		r := require.New(t)

		reqCtx, _, write := cancelUntilWritten(context.Background(), true)
		httptrace.ContextClientTrace(reqCtx).WroteRequest(httptrace.WroteRequestInfo{Err: io.ErrUnexpectedEOF})
		r.False(write.settled)
	})

	t.Run("A delete counts as written with its headers", func(t *testing.T) {
		r := require.New(t)

		reqCtx, _, write := cancelUntilWritten(context.Background(), false)
		httptrace.ContextClientTrace(reqCtx).WroteHeaders()
		r.True(write.settled)
	})
}

func TestClassify(t *testing.T) {
	gone, disconnect := context.WithCancel(context.Background())
	disconnect()

	t.Run("A request that answered is left alone", func(t *testing.T) {
		_, _, write := cancelUntilWritten(gone, false)
		require.New(t).NoError(write.classify(nil))
	})

	t.Run("A failure of its own is left alone", func(t *testing.T) {
		r := require.New(t)

		_, _, write := cancelUntilWritten(gone, false)
		err := write.classify(io.ErrUnexpectedEOF)
		r.NotErrorIs(err, dom.ErrClientGone)
		r.NotErrorIs(err, dom.ErrStorageAnswerLost)
	})

	t.Run("A cancelled request of a client that is still there is left alone", func(t *testing.T) {
		r := require.New(t)

		_, _, write := cancelUntilWritten(context.Background(), false)
		err := write.classify(context.Canceled)
		r.NotErrorIs(err, dom.ErrClientGone)
		r.NotErrorIs(err, dom.ErrStorageAnswerLost)
	})

	t.Run("A request that was not written changed nothing", func(t *testing.T) {
		r := require.New(t)

		_, _, write := cancelUntilWritten(gone, false)
		err := write.classify(context.Canceled)
		r.ErrorIs(err, dom.ErrClientGone)
		r.ErrorIs(err, context.Canceled)
	})

	t.Run("A request that was written may have been carried out", func(t *testing.T) {
		r := require.New(t)

		reqCtx, _, write := cancelUntilWritten(gone, false)
		httptrace.ContextClientTrace(reqCtx).WroteHeaders()
		err := write.classify(context.Canceled)
		r.ErrorIs(err, dom.ErrStorageAnswerLost)
		r.NotErrorIs(err, dom.ErrClientGone)
	})

	t.Run("A read passes no record and is left alone", func(t *testing.T) {
		r := require.New(t)

		var none *requestWrite
		r.ErrorIs(none.classify(context.Canceled), context.Canceled)
		r.NotErrorIs(none.classify(context.Canceled), dom.ErrClientGone)
	})

	// Either answer is right; what must not happen is a torn one.
	t.Run("A write racing a disconnect settles on one of the two", func(t *testing.T) {
		r := require.New(t)

		for range 200 {
			clientCtx, disconnect := context.WithCancel(context.Background())
			reqCtx, _, write := cancelUntilWritten(clientCtx, false)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); httptrace.ContextClientTrace(reqCtx).WroteHeaders() }()
			go func() { defer wg.Done(); disconnect() }()
			wg.Wait()

			err := write.classify(context.Canceled)
			r.True(errors.Is(err, dom.ErrStorageAnswerLost) || errors.Is(err, dom.ErrClientGone),
				"neither outcome was reached: %v", err)
		}
	})
}

func TestCancelUntilAnswered(t *testing.T) {
	t.Run("A client that leaves before anything came back ends the read", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, cancel := cancelUntilAnswered(clientCtx)
		defer cancel()
		r.NoError(reqCtx.Err())

		disconnect()
		requireCancelled(t, r, reqCtx)
	})

	// Cancelling would take the connection down with the read.
	t.Run("A read whose answer began is not ended by its client", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		reqCtx, cancel := cancelUntilAnswered(clientCtx)
		defer cancel()
		httptrace.ContextClientTrace(reqCtx).GotFirstResponseByte()

		disconnect()
		requireAlive(t, r, reqCtx)
	})

	t.Run("What ends it then is its own cancel", func(t *testing.T) {
		r := require.New(t)

		reqCtx, cancel := cancelUntilAnswered(context.Background())
		httptrace.ContextClientTrace(reqCtx).GotFirstResponseByte()

		cancel()
		requireCancelled(t, r, reqCtx)
	})

	// A destination read runs on the answer timeout's context, a child of the
	// client's. Watching that chain is what keeps the timeout able to end it.
	t.Run("Anything else that can end the chain still can", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		defer disconnect()
		answerCtx, ranOut := context.WithCancel(clientCtx) // the answer timeout
		defer ranOut()

		reqCtx, cancel := cancelUntilAnswered(answerCtx)
		defer cancel()

		ranOut()
		requireCancelled(t, r, reqCtx)
	})

	t.Run("And the client behind it still can too", func(t *testing.T) {
		r := require.New(t)

		clientCtx, disconnect := context.WithCancel(context.Background())
		answerCtx, ranOut := context.WithCancel(clientCtx)
		defer ranOut()

		reqCtx, cancel := cancelUntilAnswered(answerCtx)
		defer cancel()

		disconnect()
		requireCancelled(t, r, reqCtx)
	})

	t.Run("An answer racing a disconnect settles on one of the two", func(t *testing.T) {
		r := require.New(t)

		for range 200 {
			clientCtx, disconnect := context.WithCancel(context.Background())
			reqCtx, cancel := cancelUntilAnswered(clientCtx)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); httptrace.ContextClientTrace(reqCtx).GotFirstResponseByte() }()
			go func() { defer wg.Done(); disconnect() }()
			wg.Wait()

			if reqCtx.Err() == nil {
				r.NoError(reqCtx.Err())
			} else {
				r.ErrorIs(reqCtx.Err(), context.Canceled)
			}
			cancel()
		}
	})
}
