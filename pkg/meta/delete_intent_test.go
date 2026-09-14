/*
 * Copyright © 2024 Clyso GmbH
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

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_DeleteIntents(t *testing.T) {
	intent := func(request, owner, name string) DeleteIntent {
		return DeleteIntent{
			Request: request, Owner: owner, User: "u", Storage: "src",
			Object:   dom.Object{Bucket: "buck", Name: name},
			Versions: map[Destination]int64{ToDest("src", ""): 3},
		}
	}
	one := intent("req-1", "proxy-a", "o")

	t.Run("A proxy that was interrupted in nothing has nothing to finish", func(t *testing.T) {
		r := require.New(t)
		d := NewDeleteIntents(testutil.SetupRedis(t))

		mine, err := d.OwnedBy(t.Context(), "proxy-a")
		r.NoError(err)
		r.Empty(mine)
	})

	t.Run("What is recorded comes back whole", func(t *testing.T) {
		r := require.New(t)
		d := NewDeleteIntents(testutil.SetupRedis(t))

		odd := intent("req-1", "proxy-a", `a|b"c/d\e`)
		r.NoError(d.Record(t.Context(), []DeleteIntent{one, odd}))

		mine, err := d.OwnedBy(t.Context(), "proxy-a")
		r.NoError(err)
		r.ElementsMatch([]DeleteIntent{one, odd}, mine,
			"the proxy cannot finish what it cannot read back")
	})

	t.Run("A delete that was written down leaves nothing behind", func(t *testing.T) {
		r := require.New(t)
		d := NewDeleteIntents(testutil.SetupRedis(t))

		r.NoError(d.Record(t.Context(), []DeleteIntent{one}))
		r.NoError(d.Done(t.Context(), []DeleteIntent{one}))

		mine, err := d.OwnedBy(t.Context(), "proxy-a")
		r.NoError(err)
		r.Empty(mine)
	})

	t.Run("Only its own are its to finish", func(t *testing.T) {
		r := require.New(t)
		d := NewDeleteIntents(testutil.SetupRedis(t))

		theirs := intent("req-2", "proxy-b", "o")
		r.NoError(d.Record(t.Context(), []DeleteIntent{one, theirs}))

		mine, err := d.OwnedBy(t.Context(), "proxy-a")
		r.NoError(err)
		r.Equal([]DeleteIntent{one}, mine)
	})

	// So the one that finishes first does not take the other's away.
	t.Run("One request does not speak for another", func(t *testing.T) {
		r := require.New(t)
		d := NewDeleteIntents(testutil.SetupRedis(t))

		second := intent("req-2", "proxy-a", "o")
		r.NoError(d.Record(t.Context(), []DeleteIntent{one, second}))
		r.NoError(d.Done(t.Context(), []DeleteIntent{one}))

		mine, err := d.OwnedBy(t.Context(), "proxy-a")
		r.NoError(err)
		r.Equal([]DeleteIntent{second}, mine,
			"a delete still under way was written off by another one finishing")
	})

	t.Run("Recording nothing and finishing nothing are not errors", func(t *testing.T) {
		r := require.New(t)
		d := NewDeleteIntents(testutil.SetupRedis(t))
		r.NoError(d.Record(t.Context(), nil))
		r.NoError(d.Done(t.Context(), nil))
	})
}
