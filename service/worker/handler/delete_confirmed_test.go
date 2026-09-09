package handler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/tasks"
)

func Test_deleteIsConfirmed(t *testing.T) {
	obj := dom.Object{Bucket: "buck", Name: "o"}
	versions := map[meta.Destination]int64{meta.ToDest("src", ""): 6, meta.ToDest("dst", ""): 5}
	none := map[meta.Destination]int64{}

	t.Run("A confirmed delete nothing overtook needs no second opinion", func(t *testing.T) {
		require.New(t).True(deleteIsConfirmed(
			tasks.ObjectSyncPayload{Object: obj, Deleted: true}, none))
	})

	// The versions are back, so a write landed after the delete: the delete is
	// stale and the source has to say so.
	t.Run("A confirmed delete of an object written again is asked about", func(t *testing.T) {
		require.New(t).False(deleteIsConfirmed(
			tasks.ObjectSyncPayload{Object: obj, Deleted: true}, versions))
	})

	// The metadata was kept on purpose here, so its presence says nothing.
	t.Run("A delete nobody confirmed is always asked about", func(t *testing.T) {
		r := require.New(t)
		for _, objMeta := range []map[meta.Destination]int64{none, versions} {
			r.False(deleteIsConfirmed(
				tasks.ObjectSyncPayload{Object: obj, Deleted: true, Unconfirmed: true}, objMeta))
		}
	})

	// A sync task only routed here because a concurrent delete took the
	// metadata: it never confirmed anything itself.
	t.Run("A sync task that only lost its metadata is asked about", func(t *testing.T) {
		require.New(t).False(deleteIsConfirmed(
			tasks.ObjectSyncPayload{Object: obj}, none))
	})
}
