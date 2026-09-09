package handler

import (
	"context"
	"errors"
	"net/http"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func statError(code string, status int) error {
	return mclient.ErrorResponse{Code: code, StatusCode: status, Message: code}
}

func Test_sourceHolds(t *testing.T) {
	t.Run("A storage that answers with the object still holds it", func(t *testing.T) {
		r := require.New(t)

		holds, answered := sourceHolds(nil)
		r.True(answered)
		r.True(holds)
	})

	t.Run("A storage that names the object or its bucket as gone has answered", func(t *testing.T) {
		r := require.New(t)

		for _, code := range []string{mclient.NoSuchKey, mclient.NoSuchBucket} {
			holds, answered := sourceHolds(statError(code, http.StatusNotFound))
			r.True(answered, code)
			r.False(holds, code)
		}
	})

	// The destination delete rests on this answer, so anything short of the
	// storage naming the object gone has to be retried.
	t.Run("A storage that could not answer says nothing about the object", func(t *testing.T) {
		r := require.New(t)

		unanswered := []error{
			statError("InternalError", http.StatusInternalServerError),
			statError("SlowDown", http.StatusServiceUnavailable),
			statError("AccessDenied", http.StatusForbidden),
			statError("RequestTimeout", http.StatusBadRequest),
			statError("ExpiredToken", http.StatusBadRequest),
			context.Canceled,
			context.DeadlineExceeded,
			errors.New("dial tcp: connection refused"),
		}
		for _, err := range unanswered {
			holds, answered := sourceHolds(err)
			r.False(answered, "%v was taken for an answer", err)
			r.False(holds, "%v", err)
		}
	})
}
