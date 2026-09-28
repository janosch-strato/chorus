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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
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

type recordingReplication struct{ got context.Context }

func (r *recordingReplication) Replicate(ctx context.Context, _ tasks.SyncTask) error {
	r.got = ctx
	return nil
}

// The change is on the storage, so recording it must not depend on the client
// still being there - while still counting in what the request reports, which
// a hand copied context used to lose.
func TestReplicateOutlivesTheClient(t *testing.T) {
	r := require.New(t)

	base, disconnect := context.WithCancel(context.Background())
	ctx, timing := xctx.WithTiming(base)
	disconnect()
	r.Error(ctx.Err(), "the client context should be cancelled for this test to mean anything")

	recorder := &recordingReplication{}
	logger := zerolog.Nop()
	enqueued := replicate(ctx, recorder, []tasks.SyncTask{&tasks.ObjectSyncPayload{}}, &logger)

	r.Equal(1, enqueued)
	r.NotNil(recorder.got)
	r.NoError(recorder.got.Err(),
		"the change was recorded on the very context the client had just cancelled")
	r.Same(timing, xctx.GetTiming(recorder.got),
		"the detached context lost the request's timing, so its redis calls go unmeasured")
}

type fakeRouter struct {
	route func(*http.Request) (*http.Response, []tasks.SyncTask, string, bool, error)
}

func (f fakeRouter) Route(r *http.Request) (*http.Response, []tasks.SyncTask, string, bool, error) {
	return f.route(r)
}

type noReplication struct{}

func (noReplication) Replicate(context.Context, tasks.SyncTask) error { return nil }

type closingBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *closingBody) Close() error {
	b.closed.Store(true)
	return nil
}

// A connection only returns to its pool once the answer is closed, so every
// way out of the handler has to close one that arrived.
func TestServeClosesTheStorageAnswer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		isApiEr bool
	}{
		{name: "an answer that is forwarded"},
		{name: "an answer whose route then failed", err: dom.ErrInternal},
		{name: "an answer of a client that left", err: dom.ErrClientGone},
		{name: "an api error", isApiEr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)

			body := &closingBody{Reader: strings.NewReader("answer")}
			handler := Serve(fakeRouter{route: func(*http.Request) (*http.Response, []tasks.SyncTask, string, bool, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body,
					Header:     http.Header{},
				}, nil, testSource, tc.isApiEr, tc.err
			}}, noReplication{})

			handler.ServeHTTP(httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/"+testBucket+"/obj", nil))

			r.True(body.closed.Load(), "the storage answer was never closed, so its connection is held")
		})
	}
}
