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

package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/buger/jsonparser"
	"github.com/hibiken/asynq"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

// A list of task types.
const (
	TypeBucketCreate   = "bucket:create"
	TypeBucketDelete   = "bucket:delete"
	TypeBucketSyncTags = "bucket:sync:tags"
	TypeBucketSyncACL  = "bucket:sync:acl"

	TypeObjectSync     = "object:sync"
	TypeObjectSyncTags = "object:sync:tags"
	TypeObjectSyncACL  = "object:sync:acl"

	TypeMigrateBucketListObjects  = "migrate:bucket:list_objects"
	TypeMigrateObjCopy            = "migrate:object:copy"
	TypeMigrateObjectListVersions = "migrate:object:list_versions"
	TypeMigrateVersionedObject    = "migrate:object:copy_versioned"

	TypeConsistencyCheck          = "consistency"
	TypeConsistencyCheckList      = "consistency:list"
	TypeConsistencyCheckReadiness = "consistency:readiness"
	TypeConsistencyCheckResult    = "consistency:result"

	TypeApiZeroDowntimeSwitch = "api:switch_zero_downtime"
	TypeApiSwitchWithDowntime = "api:switch_w_downtime"
)

type Queue string

const (
	QueueAPI                      Queue = "api"
	QueueMigrateListObjectsPrefix Queue = "migr_list_obj"
	QueueConsistencyCheck         Queue = "consistency_check"
	QueueMigrateCopyObjectPrefix  Queue = "migr_copy_obj"
	QueueEventsPrefix             Queue = "event"
)

// Priority defines the priority of the queues from highest to lowest.
var Priority = map[string]int{
	string(QueueAPI): 200, // highest priority
	string(QueueMigrateListObjectsPrefix) + ":*": 100,
	string(QueueConsistencyCheck):                50,
	string(QueueMigrateCopyObjectPrefix) + ":*":  10,
	string(QueueEventsPrefix) + ":*":             5, // lowest priority
	"*":                                          1, // fallback for legacy queues
}

func replicationQueueName(queuePrefix Queue, id entity.ReplicationStatusID) string {
	switch queuePrefix {
	case QueueMigrateCopyObjectPrefix,
		QueueMigrateListObjectsPrefix,
		QueueEventsPrefix:
		return fmt.Sprintf("%s:%s:%s:%s:%s", queuePrefix, id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket)
	default:
		panic(fmt.Sprintf("%s is not a replication queue prefix", queuePrefix))
	}
}

// MigrateBucketListObjectsTaskID returns the task id of the listing task for a
// bucket and prefix.
func MigrateBucketListObjectsTaskID(fromStorage, toStorage, bucket, toBucket, prefix string) string {
	id := fmt.Sprintf("mgr:lo:%s:%s:%s:%s", fromStorage, toStorage, bucket, toBucket)
	if prefix != "" {
		id += ":" + prefix
	}
	return id
}

// MigrateObjCopyQueue returns the name of the queue holding the object copy
// tasks of the given replication.
func MigrateObjCopyQueue(id entity.ReplicationStatusID) string {
	return replicationQueueName(QueueMigrateCopyObjectPrefix, id)
}

// MigrateObjCopyTaskID returns the task id of the object copy task for the
// given object. The id is deterministic so that the task can be looked up
// without knowing anything but the object itself.
func MigrateObjCopyTaskID(fromStorage, toStorage, bucket, toBucket, object, versionID string) string {
	id := fmt.Sprintf("mgr:co:%s:%s:%s:%s:%s", fromStorage, toStorage, bucket, toBucket, object)
	if versionID != "" {
		id += ":" + versionID
	}
	return id
}

func InitMigrationQueues(id entity.ReplicationStatusID) []string {
	return []string{
		replicationQueueName(QueueMigrateListObjectsPrefix, id),
		replicationQueueName(QueueMigrateCopyObjectPrefix, id),
	}
}

func EventMigrationQueues(id entity.ReplicationStatusID) []string {
	return []string{
		replicationQueueName(QueueEventsPrefix, id),
	}
}

func AllReplicationQueues(id entity.ReplicationStatusID) []string {
	return append(InitMigrationQueues(id), EventMigrationQueues(id)...)
}

type SyncTask interface {
	GetFrom() string
	GetToStorage() string
	GetToBucket() string
	SetFrom(from string)
	SetTo(storage string, bucket string)
	InitDate()
	GetDate() time.Time
}

type Sync struct {
	FromStorage string
	ToStorage   string
	ToBucket    string
	CreatedAt   time.Time
}

func (t *Sync) GetFrom() string {
	return t.FromStorage
}
func (t *Sync) GetToStorage() string {
	return t.ToStorage
}
func (t *Sync) GetToBucket() string {
	return t.ToBucket
}
func (t *Sync) SetFrom(from string) {
	t.FromStorage = from
}
func (t *Sync) SetTo(storage string, bucket string) {
	t.ToStorage = storage
	t.ToBucket = bucket
}
func (t *Sync) InitDate() {
	t.CreatedAt = time.Now().UTC()
}
func (t *Sync) GetDate() time.Time {
	return t.CreatedAt
}

type ZeroDowntimeReplicationSwitchPayload struct {
	Sync
	Bucket string
	User   string
}

type BucketSyncTagsPayload struct {
	Bucket string
	Sync
}

type ObjSyncTagsPayload struct {
	Object dom.Object
	Sync
}

type BucketSyncACLPayload struct {
	Bucket string
	Sync
}

type ObjSyncACLPayload struct {
	Object dom.Object
	Sync
}

type BucketCreatePayload struct {
	Sync
	Bucket   string
	Location string
	//Storage  string
}

type ObjectSyncPayload struct {
	Object dom.Object
	Sync

	//FromVersion int64
	ObjSize int64
	Deleted bool
}

type BucketDeletePayload struct {
	Sync
	Bucket string
	//Storage string
}

type ObjInfo struct {
	Name      string
	VersionID string
}

type ListObjectVersionsPayload struct {
	Sync
	Bucket string
	Prefix string
}

type MigrateVersionedObjectPayload struct {
	Sync
	Bucket string
	Prefix string
}

type MigrateBucketListObjectsPayload struct {
	Sync
	Bucket    string
	Prefix    string
	Versioned bool
}

type MigrateObjCopyPayload struct {
	Sync
	Bucket string
	Obj    ObjPayload
}

type ObjPayload struct {
	Name        string
	VersionID   string
	ETag        string
	Size        int64
	ContentType string
}

type MigrateLocation struct {
	Storage string
	Bucket  string
	User    string
}

type ConsistencyCheckPayload struct {
	ID        string
	Locations []MigrateLocation
}

type ConsistencyCheckListPayload struct {
	MigrateLocation
	Prefix       string
	ID           string
	StorageCount uint8
}

type ConsistencyCheckReadinessPayload struct {
	ID string
}

type ConsistencyCheckDeletePayload struct {
	ID string
}

type SwitchWithDowntimePayload struct {
	FromStorage string
	ToStorage   string
	User        string
	Bucket      string
	CreatedAt   time.Time
}

type ReplicationTask interface {
	BucketCreatePayload |
		BucketDeletePayload |
		BucketSyncTagsPayload |
		BucketSyncACLPayload |
		ObjectSyncPayload |
		ObjSyncTagsPayload |
		ObjSyncACLPayload |
		MigrateBucketListObjectsPayload |
		MigrateObjCopyPayload |
		ListObjectVersionsPayload |
		MigrateVersionedObjectPayload
}

// NewReplicationTask builds the task for a replication payload. Extra options
// are applied last, so a caller can override the defaults, for example to
// reschedule a listing under a different task id.
func NewReplicationTask[T ReplicationTask](ctx context.Context, replicationID entity.ReplicationStatusID, payload T, extra ...asynq.Option) (*asynq.Task, error) {
	bytes, err := json.Marshal(&payload)
	if err != nil {
		return nil, err
	}
	if xctx.GetUser(ctx) != "" {
		bytes, err = jsonparser.Set(bytes, []byte(`"`+xctx.GetUser(ctx)+`"`), "User")
		if err != nil {
			return nil, fmt.Errorf("%w: unable to add User to payload", err)
		}
	}
	taskType := ""
	var optionList []asynq.Option
	switch p := any(payload).(type) {
	case BucketCreatePayload:
		id := fmt.Sprintf("cb:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket)
		queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeBucketCreate
	case BucketDeletePayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeBucketDelete
	case ObjectSyncPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeObjectSync
	case BucketSyncTagsPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeBucketSyncTags
	case BucketSyncACLPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeBucketSyncACL
	case ObjSyncTagsPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeObjectSyncTags
	case ObjSyncACLPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeObjectSyncACL
	case MigrateBucketListObjectsPayload:
		id := MigrateBucketListObjectsTaskID(p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, p.Prefix)
		queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateBucketListObjects
	case MigrateObjCopyPayload:
		id := MigrateObjCopyTaskID(p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, p.Obj.Name, p.Obj.VersionID)
		queue := MigrateObjCopyQueue(replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateObjCopy
	case MigrateVersionedObjectPayload:
		id := fmt.Sprintf("mgr:cov:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.Prefix)
		queue := replicationQueueName(QueueMigrateCopyObjectPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateVersionedObject
	case ListObjectVersionsPayload:
		id := fmt.Sprintf("mgr:lov:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.Prefix)
		queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateObjectListVersions
	default:
		return nil, fmt.Errorf("%w: unknown task type %T", dom.ErrInvalidArg, p)
	}

	// Even though the internal asynq task presentation allows infinite tasks by setting the timeout
	// to 0 (see asynqs TaskMessage struct in internal/base/base.go), asynq.NewTask() prevents this
	// by setting a default timeout of 30 minutes if no deadline and  a timeout of 0 is configured.
	// Since golangs time.Duration has no value for infinity, we just set  a timeout of 100 years here,
	// which is most likely long enough for most tasks.
	optionList = append(optionList, asynq.Timeout(100*24*365*time.Hour), asynq.MaxRetry(math.MaxInt32))
	optionList = append(optionList, extra...)
	return asynq.NewTask(taskType, bytes, optionList...), nil
}

// TaskObjectInfo holds decoded object/bucket identifiers from a task payload.
type TaskObjectInfo struct {
	Object      string
	Bucket      string
	ToBucket    string
	FromStorage string
	ToStorage   string
}

// ParseTaskObjectInfo decodes a task payload into object/bucket identifiers.
// Returns nil for unrecognised task types.
func ParseTaskObjectInfo(taskType string, payload []byte) (*TaskObjectInfo, error) {
	var p struct {
		Sync
		Bucket string     `json:"Bucket"`
		Object dom.Object `json:"Object"`
		Obj    ObjPayload `json:"Obj"`
		Prefix string     `json:"Prefix"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("unmarshal %s payload: %w", taskType, err)
	}

	info := &TaskObjectInfo{
		ToBucket:    p.ToBucket,
		FromStorage: p.FromStorage,
		ToStorage:   p.ToStorage,
	}

	switch taskType {
	case TypeMigrateObjCopy:
		info.Object = p.Obj.Name
		info.Bucket = p.Bucket
	case TypeMigrateVersionedObject:
		info.Object = p.Prefix
		info.Bucket = p.Bucket
	case TypeObjectSync, TypeObjectSyncTags, TypeObjectSyncACL:
		info.Object = p.Object.Name
		info.Bucket = p.Object.Bucket
	case TypeBucketCreate, TypeBucketDelete, TypeBucketSyncTags, TypeBucketSyncACL:
		info.Bucket = p.Bucket
	default:
		return nil, nil
	}

	return info, nil
}

type ApiTask interface {
	ZeroDowntimeReplicationSwitchPayload |
		SwitchWithDowntimePayload |
		ConsistencyCheckPayload |
		ConsistencyCheckListPayload |
		ConsistencyCheckReadinessPayload |
		ConsistencyCheckDeletePayload
}

func NewTask[T ApiTask](ctx context.Context, payload T) (*asynq.Task, error) {
	bytes, err := json.Marshal(&payload)
	if err != nil {
		return nil, err
	}
	if xctx.GetUser(ctx) != "" {
		bytes, err = jsonparser.Set(bytes, []byte(`"`+xctx.GetUser(ctx)+`"`), "User")
		if err != nil {
			return nil, fmt.Errorf("%w: unable to add User to payload", err)
		}
	}
	taskType := ""
	var optionList []asynq.Option
	switch p := any(payload).(type) {
	case ZeroDowntimeReplicationSwitchPayload:
		optionList = []asynq.Option{asynq.Queue(string(QueueAPI)), asynq.TaskID(fmt.Sprintf("api:zdrs:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.User, p.Bucket))}
		taskType = TypeApiZeroDowntimeSwitch
	case SwitchWithDowntimePayload:
		id := fmt.Sprintf("api:sd:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.User, p.Bucket)
		optionList = []asynq.Option{asynq.Queue(string(QueueAPI)), asynq.TaskID(id)}
		taskType = TypeApiSwitchWithDowntime
	case ConsistencyCheckPayload:
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck))}
		taskType = TypeConsistencyCheck
	case ConsistencyCheckListPayload:
		id := fmt.Sprintf("cc:l:%s:%s:%s:%s", p.ID, p.Storage, p.Bucket, p.Prefix)
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck)), asynq.TaskID(id)}
		taskType = TypeConsistencyCheckList
	case ConsistencyCheckReadinessPayload:
		id := fmt.Sprintf("cc:r:%s", p.ID)
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck)), asynq.TaskID(id)}
		taskType = TypeConsistencyCheckReadiness
	case ConsistencyCheckDeletePayload:
		id := fmt.Sprintf("cc:d:%s", p.ID)
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck)), asynq.TaskID(id)}
		taskType = TypeConsistencyCheckResult
	default:
		return nil, fmt.Errorf("%w: unknown task type %T", dom.ErrInvalidArg, p)
	}

	optionList = append(optionList, asynq.MaxRetry(math.MaxInt32))
	return asynq.NewTask(taskType, bytes, optionList...), nil
}
