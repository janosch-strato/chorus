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
	"errors"
	"fmt"
	"net/http/httptrace"
	"sync"

	"github.com/clyso/chorus/pkg/dom"
)

// The moment from which the storage may act on the request.
//
// Both hooks fire before the transport's write buffer reaches the socket, so
// written is an upper bound. That is the safe direction: a change nobody
// confirmed is looked at, an unnoticed one is not. For the same reason the
// body-less case must not use WroteRequest, which is no closer to the flush
// and fires later.
func onRequestWritten(hasBody bool, written func()) *httptrace.ClientTrace {
	if hasBody {
		return &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				written()
			}
		}}
	}
	return &httptrace.ClientTrace{WroteHeaders: written}
}

// How far a request got: a write is out of its client's hands once written, a
// read once its answer began to arrive.
type requestWrite struct {
	clientCtx context.Context

	mu      sync.Mutex
	settled bool
	gaveUp  bool
	stop    func() bool
}

// The client context is watched rather than made the parent, so that what it
// carries stays and only its cancellation goes. Watching keeps everything else
// on that chain, the destination read timeout above all, in force until the
// request settles.
func (w *requestWrite) followClientUntilSettled(clientCtx context.Context) (context.Context, context.CancelFunc) {
	w.clientCtx = clientCtx
	ctx, cancel := context.WithCancel(context.WithoutCancel(clientCtx))
	w.stop = context.AfterFunc(clientCtx, func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.settled {
			return
		}
		w.gaveUp = true
		cancel()
	})
	return ctx, cancel
}

// For a request that changes the storage.
//
// Once written, cancelling spares the storage nothing - it is already carrying
// the change out - and only turns an outcome about to be settled into one
// nobody knows, which the proxy would then have to replicate on a guess. The
// client's own timeout bounds it from there.
func cancelUntilWritten(clientCtx context.Context, hasBody bool) (context.Context, context.CancelFunc, *requestWrite) {
	w := &requestWrite{}
	ctx, cancel := w.followClientUntilSettled(clientCtx)
	return httptrace.WithClientTrace(ctx, onRequestWritten(hasBody, w.markSettled)), cancel, w
}

// For a read, which can always be asked for again: seeing it through is never
// about the answer, only about the connection. The transport drops a cancelled
// request's connection instead of pooling it, so from the first byte such a
// read is ended by closing its body, see drainingBody.
func cancelUntilAnswered(clientCtx context.Context) (context.Context, context.CancelFunc) {
	w := &requestWrite{}
	ctx, cancel := w.followClientUntilSettled(clientCtx)
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotFirstResponseByte: w.markSettled,
	}), cancel
}

func (w *requestWrite) markSettled() {
	w.mu.Lock()
	w.settled = true
	stop := w.stop
	w.mu.Unlock()
	// Outside the lock: stop does not wait for a callback already running.
	// Whichever reached the lock first decided, and either answer is right.
	if stop != nil {
		stop()
	}
}

// What a failure means for what is on the storage, which decides whether a
// replication task is created. Not airtight: Do can return from a cancellation
// just before the write hook runs. Waiting for a hook that may never come would
// delay every aborted request.
func (w *requestWrite) classify(err error) error {
	if w == nil || err == nil {
		return err
	}
	w.mu.Lock()
	written, gaveUp := w.settled, w.gaveUp
	w.mu.Unlock()
	// Without a record, only a cancellation says the client was the reason.
	gaveUp = gaveUp || (errors.Is(err, context.Canceled) && w.clientCtx.Err() != nil)
	switch {
	case written:
		return fmt.Errorf("%w: %w", dom.ErrStorageAnswerLost, err)
	case gaveUp:
		return fmt.Errorf("%w: %w", dom.ErrClientGone, err)
	default:
		return err
	}
}
