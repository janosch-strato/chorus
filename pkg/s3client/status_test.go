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
	"net"
	"net/http"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
)

// apiError is what a non-success status becomes on the way out of client.Do.
func apiError(code int) error {
	return mclient.ErrorResponse{StatusCode: code, Code: "Test"}
}

func Test_s3RequestStatus(t *testing.T) {
	tests := []struct {
		name string
		resp *http.Response
		err  error
		want metrics.ReqStatus
	}{
		{name: "answered 200", resp: &http.Response{StatusCode: 200}, want: metrics.StatusOK},
		{
			// not in the set of statuses chorus treats as success, so it
			// arrives here as an error, but the storage did answer
			name: "answered 304", resp: &http.Response{StatusCode: 304},
			err: apiError(304), want: metrics.StatusRedirect,
		},
		{
			// the bucket lives in another region, which is a misconfigured
			// storage rather than a request that worked
			name: "answered 301", resp: &http.Response{StatusCode: 301},
			err: apiError(301), want: metrics.StatusRedirect,
		},
		{name: "answered 299", resp: &http.Response{StatusCode: 299}, err: apiError(299), want: metrics.StatusOK},
		{name: "answered 201", resp: &http.Response{StatusCode: 201}, err: apiError(201), want: metrics.StatusOK},
		{name: "answered 404", resp: &http.Response{StatusCode: 404}, err: apiError(404), want: metrics.StatusClientErr},
		{name: "answered 503", resp: &http.Response{StatusCode: 503}, err: apiError(503), want: metrics.StatusServerErr},
		{name: "error only, 500", err: apiError(500), want: metrics.StatusServerErr},
		{
			name: "storage unreachable",
			err:  fmt.Errorf("dial: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")}),
			want: metrics.StatusNetworkErr,
		},
		{
			// given up on before the request left this process: counting it
			// as network_error would say the storage is down
			name: "never sent", err: fmt.Errorf("%w: unknown signature", dom.ErrInternal),
			want: metrics.StatusInternalErr,
		},
		{name: "cancelled", err: context.Canceled, want: metrics.StatusInternalErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, s3RequestStatus(tt.resp, tt.err))
		})
	}
}

func Test_ResponseStatus(t *testing.T) {
	r := require.New(t)
	r.Equal(200, ResponseStatus(&http.Response{StatusCode: 200}, nil))
	// the response is gone by the time the router sees the error
	r.Equal(503, ResponseStatus(nil, apiError(503)))
	r.Equal(0, ResponseStatus(nil, context.Canceled))
	r.Equal(0, ResponseStatus(nil, nil))
}
