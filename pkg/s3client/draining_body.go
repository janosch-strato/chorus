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

package s3client

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// maxDrainBytes is how much of an answer nobody wants is worth reading to keep
// the connection it came on. The alternative is a dial, which costs a
// handshake and, under a burst, the name resolver besides.
const maxDrainBytes = 10 << 20

// drainBudget bounds an answer whose length the storage never gave, and stops
// one that goes quiet mid answer from holding the request. The storage's
// request timeout would do it eventually, but it is meant for a request
// somebody is still waiting on.
const drainBudget = 2 * time.Second

// drainingBody finishes an answer that its reader stopped short of, so that the
// connection goes back to the pool. A connection is reusable only once its
// answer has been read to the end, so the choice at close is between reading
// out what is left and giving the connection up.
//
// Close and Abort may be called while a Read is waiting, as with any response
// body. Concurrent Reads are not meaningful, and the count is then only
// approximate.
type drainingBody struct {
	io.ReadCloser
	// Held this long because a read is no longer cancelled by its client, see
	// cancelUntilAnswered.
	cancel context.CancelFunc
	// -1 when the storage did not say.
	remaining atomic.Int64
	budget    time.Duration
}

func newDrainingBody(body io.ReadCloser, contentLength int64, cancel context.CancelFunc) *drainingBody {
	b := &drainingBody{ReadCloser: body, cancel: cancel, budget: drainBudget}
	b.remaining.Store(contentLength)
	return b
}

func (b *drainingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.remaining.Load() >= 0 {
		b.remaining.Add(-int64(n))
	}
	return n, err
}

func (b *drainingBody) Close() error {
	if b.remaining.Load() > maxDrainBytes {
		b.Abort()
		return b.ReadCloser.Close()
	}
	// A read already waiting on the connection is only handed back by ending
	// the request.
	if b.cancel != nil {
		defer time.AfterFunc(b.budget, b.cancel).Stop()
	}
	// An unfinished answer drops the connection, as it always did.
	_, _ = io.Copy(io.Discard, b.ReadCloser)
	return b.ReadCloser.Close()
}

// Abort ends the request the answer came on, leaving what is still on the
// wire unread. The connection goes with it, which is the cost of not waiting.
func (b *drainingBody) Abort() {
	if b.cancel != nil {
		b.cancel()
	}
}

// Abortable is an answer that can be given up on before it has been read out.
// A wrapper around one passes this on, or the answer under it cannot be
// reached.
type Abortable interface {
	Abort()
}

// AbortResponse gives up on an answer nobody is waiting for any more, so that
// a reader the storage has left hanging is let go: from the first byte a read
// no longer follows the context it was made on, see cancelUntilAnswered, and
// ending the request is what is left. Closing the body reaches the same place
// but only through the drain, which is itself a read of the answer that
// stopped.
//
// An answer with nothing to give up on - one already read into memory, an api
// error's among them - is left alone.
func AbortResponse(resp *http.Response) {
	if resp == nil {
		return
	}
	if body, ok := resp.Body.(Abortable); ok {
		body.Abort()
	}
}
