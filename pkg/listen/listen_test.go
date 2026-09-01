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

package listen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ListenExclusiveByDefault(t *testing.T) {
	r := require.New(t)

	first, err := Listen(t.Context(), "127.0.0.1:0")
	r.NoError(err)
	defer first.Close()

	_, err = Listen(t.Context(), first.Addr().String())
	r.Error(err, "the port is not shared unless asked for")
}

func Test_ListenSharesPort(t *testing.T) {
	r := require.New(t)
	// main sets this before anything runs, so no other test may listen after
	SetReusePort(true)
	t.Cleanup(func() { SetReusePort(false) })

	first, err := Listen(t.Context(), "127.0.0.1:0")
	r.NoError(err)
	defer first.Close()

	second, err := Listen(t.Context(), first.Addr().String())
	r.NoError(err, "a second listener shares the port")
	defer second.Close()
}
