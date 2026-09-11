package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_custom_bucket_compatibility(t *testing.T) {
	r := require.New(t)
	//setup:
	type OldSync struct {
		FromStorage string
		ToStorage   string
		CreatedAt   time.Time
	}
	type OldBucketCreatePayload struct {
		OldSync
		Bucket   string
		Location string
	}

	oldTask := OldBucketCreatePayload{
		OldSync: OldSync{
			FromStorage: "stor1",
			ToStorage:   "stor2",
			CreatedAt:   time.Now(),
		},
		Bucket:   "buck1",
		Location: "loc",
	}
	oldJson, err := json.Marshal(&oldTask)
	r.NoError(err)
	t.Run("new handler works with old", func(t *testing.T) {
		r := require.New(t)
		var p BucketCreatePayload
		r.NoError(json.Unmarshal(oldJson, &p))
		r.EqualValues(oldTask.FromStorage, p.FromStorage)
		r.EqualValues(oldTask.ToStorage, p.ToStorage)
		r.EqualValues(oldTask.CreatedAt.Unix(), p.CreatedAt.Unix())
		r.EqualValues(oldTask.Bucket, p.Bucket)
		r.EqualValues(oldTask.Location, p.Location)
		r.Empty(p.ToBucket)
	})

	t.Run("custom bucket name preserved", func(t *testing.T) {
		r := require.New(t)
		bucket := "bucket"
		src := BucketCreatePayload{
			Sync: Sync{
				ToBucket: bucket,
			},
		}
		srcJson, err := json.Marshal(&src)
		r.NoError(err)
		var dst BucketCreatePayload
		r.NoError(json.Unmarshal(srcJson, &dst))
		r.NotNil(dst.ToBucket)
		r.EqualValues(src.ToBucket, dst.ToBucket, "custom bucket preserved")

		src = BucketCreatePayload{}
		srcJson, err = json.Marshal(&src)
		r.NoError(err)
		r.NoError(json.Unmarshal(srcJson, &dst))
		r.Empty(dst.ToBucket, "no custom bucket unmarshal")
	})
}

func Test_migrate_obj_copy_task_is_retained(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	client := asynq.NewClientFromRedisClient(c)
	t.Cleanup(func() { client.Close() })

	replicationID := entity.NewReplicationStatusID("user", "src", "buck", "dst", "dst-buck")
	payload := MigrateObjCopyPayload{
		Sync:   Sync{FromStorage: "src", ToStorage: "dst", ToBucket: "dst-buck"},
		Bucket: "buck",
		Obj:    ObjPayload{Name: "dir/obj"},
	}
	task, err := NewReplicationTask(ctx, replicationID, payload)
	r.NoError(err)

	info, err := client.EnqueueContext(ctx, task)
	r.NoError(err)
	// a finished copy task is not kept: the copies are recorded per object
	// by the worker, see storage.SetMigratedObj
	r.Zero(info.Retention)
	r.EqualValues(MigrateObjCopyQueue(replicationID), info.Queue)
	r.EqualValues(MigrateObjCopyTaskID("dir/obj", ""), info.ID)
}

func Test_MigrateObjCopyTaskID(t *testing.T) {
	r := require.New(t)
	r.EqualValues("o:obj", MigrateObjCopyTaskID("obj", ""))
	r.EqualValues("o:obj:v1", MigrateObjCopyTaskID("obj", "v1"))
	// an object may be named like anything, the prefix still tells the two
	// kinds of task sharing the copy queue apart
	r.NotEqual(copyVersionedIDPrefix+"pre", MigrateObjCopyTaskID(copyVersionedIDPrefix+"pre", ""))
}
