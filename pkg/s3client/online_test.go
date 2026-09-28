package s3client

import (
	"context"
	"errors"
	"net"
	"net/http/httptrace"
	"net/url"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

// Minio decides this, on errors that are ours and wrapped. Pinned so that an
// upgrade of it cannot quietly change who goes offline.
func TestWhatMarksAStorageUnreachable(t *testing.T) {
	hostDown := &url.Error{
		Op:  "Get",
		URL: "http://storage.invalid/probe",
		Err: &net.OpError{Op: "dial", Err: errors.New("connection refused")},
	}
	gone, disconnect := context.WithCancel(context.Background())
	disconnect()

	t.Run("A storage that cannot be reached is", func(t *testing.T) {
		require.New(t).True(mclient.IsNetworkOrHostDown(hostDown, false))
	})

	// A client leaving ends many requests at once, saying nothing about the
	// storage.
	t.Run("A client that left is not", func(t *testing.T) {
		r := require.New(t)

		_, _, write := cancelUntilWritten(gone, false)
		err := write.classify(context.Canceled)

		r.ErrorIs(err, dom.ErrClientGone)
		r.False(mclient.IsNetworkOrHostDown(err, false),
			"a client that left was taken for a storage that cannot be reached")
	})

	t.Run("Nor is one that left after its request was sent", func(t *testing.T) {
		r := require.New(t)

		ctx, _, write := cancelUntilWritten(gone, false)
		httptrace.ContextClientTrace(ctx).WroteHeaders()
		err := write.classify(context.Canceled)

		r.ErrorIs(err, dom.ErrStorageAnswerLost)
		r.False(mclient.IsNetworkOrHostDown(err, false))
	})

	t.Run("A storage that dropped the request is", func(t *testing.T) {
		r := require.New(t)

		ctx, _, write := cancelUntilWritten(gone, false)
		httptrace.ContextClientTrace(ctx).WroteHeaders()
		err := write.classify(hostDown)

		r.ErrorIs(err, dom.ErrStorageAnswerLost)
		r.True(mclient.IsNetworkOrHostDown(err, false),
			"the wrapping hid a storage that had gone away")
	})

	// Recognised only through errors.Is, so the wrapping has to keep the cause.
	t.Run("A storage that ran out of time is", func(t *testing.T) {
		r := require.New(t)

		ctx, _, write := cancelUntilWritten(gone, false)
		httptrace.ContextClientTrace(ctx).WroteHeaders()
		err := write.classify(context.DeadlineExceeded)

		r.True(mclient.IsNetworkOrHostDown(err, false),
			"the wrapping hid a storage that had stopped answering")
	})
}
