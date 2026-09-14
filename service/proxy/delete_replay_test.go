/*
 * Copyright © 2024 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
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

package proxy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/testutil"
)

type fakeFinisher struct {
	present  map[string]bool
	nowVers  map[string]map[meta.Destination]int64
	removed  []string
	queued   []string
	statErr  error
	writeErr error
}

func (f *fakeFinisher) gone(_ context.Context, i meta.DeleteIntent) (bool, error) {
	if f.statErr != nil {
		return false, f.statErr
	}
	return !f.present[i.Object.Name], nil
}

func (f *fakeFinisher) remove(_ context.Context, i meta.DeleteIntent) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	f.removed = append(f.removed, i.Object.Name)
	f.present[i.Object.Name] = false
	return nil
}

func (f *fakeFinisher) versions(_ context.Context, i meta.DeleteIntent) (map[meta.Destination]int64, error) {
	return f.nowVers[i.Object.Name], nil
}

func (f *fakeFinisher) queue(_ context.Context, i meta.DeleteIntent) error {
	f.queued = append(f.queued, i.Object.Name)
	return nil
}

func Test_finishInterruptedDeletes(t *testing.T) {
	const ours = "proxy-a"
	src := meta.ToDest("src", "")
	was := map[meta.Destination]int64{src: 3}

	intent := func(name string) meta.DeleteIntent {
		return meta.DeleteIntent{
			Request: "req-1", Owner: ours, User: "u", Storage: "src",
			Object: dom.Object{Bucket: "buck", Name: name}, Versions: was,
		}
	}
	record := func(t *testing.T, is ...meta.DeleteIntent) (*meta.DeleteIntents, *fakeFinisher) {
		t.Helper()
		d := meta.NewDeleteIntents(testutil.SetupRedis(t))
		require.NoError(t, d.Record(t.Context(), is))
		f := &fakeFinisher{present: map[string]bool{}, nowVers: map[string]map[meta.Destination]int64{}}
		for _, i := range is {
			f.nowVers[i.Object.Name] = was
		}
		return d, f
	}

	t.Run("Nothing was interrupted", func(t *testing.T) {
		r := require.New(t)
		d := meta.NewDeleteIntents(testutil.SetupRedis(t))
		f := &fakeFinisher{present: map[string]bool{}}
		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Empty(f.removed)
		r.Empty(f.queued)
	})

	t.Run("An object that is gone is replicated and not deleted again", func(t *testing.T) {
		r := require.New(t)
		d, f := record(t, intent("gone"))

		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Empty(f.removed, "an object that was already gone was deleted again")
		r.Equal([]string{"gone"}, f.queued)

		left, err := d.OwnedBy(t.Context(), ours)
		r.NoError(err)
		r.Empty(left)
	})

	// Whether the storage got there cannot be found out, so it is made again.
	t.Run("An object that is still there is deleted again and replicated", func(t *testing.T) {
		r := require.New(t)
		d, f := record(t, intent("here"))
		f.present["here"] = true

		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Equal([]string{"here"}, f.removed, "the delete was left unfinished")
		r.Equal([]string{"here"}, f.queued)

		left, err := d.OwnedBy(t.Context(), ours)
		r.NoError(err)
		r.Empty(left)
	})

	// This is what lets another proxy serve while this one is starting up.
	t.Run("An object written again since is left alone", func(t *testing.T) {
		r := require.New(t)
		d, f := record(t, intent("rewritten"))
		f.present["rewritten"] = true
		f.nowVers["rewritten"] = map[meta.Destination]int64{src: 4}

		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Empty(f.removed, "an object somebody wrote after the delete was deleted anyway")
		r.Empty(f.queued, "a delete that was superseded was replicated")

		left, err := d.OwnedBy(t.Context(), ours)
		r.NoError(err)
		r.Empty(left, "it was left to be tried again at the next startup")
	})

	// It dropped them itself and stopped before queueing the replication. A
	// write would have left its own behind, so there is nothing to give way to.
	t.Run("An object whose versions the delete itself dropped is carried through", func(t *testing.T) {
		r := require.New(t)
		d, f := record(t, intent("dropped"))
		f.present["dropped"] = true
		f.nowVers["dropped"] = nil

		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Equal([]string{"dropped"}, f.removed, "a delete of its own making was read as a newer write")
		r.Equal([]string{"dropped"}, f.queued)

		left, err := d.OwnedBy(t.Context(), ours)
		r.NoError(err)
		r.Empty(left)
	})

	// The storage had carried it out; only the replication was left to queue.
	t.Run("A delete that dropped the versions and reached the storage is replicated", func(t *testing.T) {
		r := require.New(t)
		d, f := record(t, intent("settled"))
		f.nowVers["settled"] = nil

		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Empty(f.removed, "an object that was already gone was deleted again")
		r.Equal([]string{"settled"}, f.queued, "the destination was left holding the object")

		left, err := d.OwnedBy(t.Context(), ours)
		r.NoError(err)
		r.Empty(left)
	})

	t.Run("An object that had no versions and has some now is left alone", func(t *testing.T) {
		r := require.New(t)
		fresh := intent("fresh")
		fresh.Versions = nil
		d, f := record(t, fresh)
		f.present["fresh"] = true
		f.nowVers["fresh"] = map[meta.Destination]int64{src: 1}

		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Empty(f.removed)
		r.Empty(f.queued)
	})

	t.Run("Another proxy's deletes are left alone", func(t *testing.T) {
		r := require.New(t)
		theirs := intent("theirs")
		theirs.Owner = "proxy-b"
		d, f := record(t, theirs)
		f.present["theirs"] = true

		r.NoError(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Empty(f.removed, "a delete another proxy is making was made again from under it")

		left, err := d.OwnedBy(t.Context(), "proxy-b")
		r.NoError(err)
		r.Len(left, 1)
	})

	// Not to be read as the object being gone; the intent stays for next time.
	t.Run("A storage that says nothing stops the replay", func(t *testing.T) {
		r := require.New(t)
		d, f := record(t, intent("o"))
		f.statErr = errors.New("storage is down")

		r.Error(finishInterruptedDeletes(t.Context(), d, ours, f))
		r.Empty(f.queued, "a storage that said nothing was read as the object being gone")

		left, err := d.OwnedBy(t.Context(), ours)
		r.NoError(err)
		r.Len(left, 1, "the delete was written off although nothing settled it")
	})
}
