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

package settings

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/switches"
)

// Every switch has to behave the same way, because the maint api serves them
// all through the one handler.
func Test_All(t *testing.T) {
	r := require.New(t)
	r.NotEmpty(All())

	names := map[string]struct{}{}
	for _, sw := range All() {
		t.Run(sw.Name(), func(t *testing.T) {
			r := require.New(t)
			r.NotContains(names, sw.Name(), "the name is the path the api serves it under")
			names[sw.Name()] = struct{}{}
			r.NotEmpty(sw.Syntax(), "the api has to be able to say what it accepts")

			was := sw.GetString()
			t.Cleanup(func() { require.NoError(t, sw.SetString(was)) })
			r.NotEmpty(was)
			r.NoError(sw.SetString(was), "what it holds is a value it accepts")

			r.NoError(sw.SetString(""), "the empty value is its default")
			def := sw.GetString()
			r.NotEmpty(def)

			r.ErrorIs(sw.SetString("no-such-value"), switches.ErrInvalidValue)
			r.Equal(def, sw.GetString(), "a refused value leaves it alone")
		})
	}
}
