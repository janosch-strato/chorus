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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
