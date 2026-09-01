/*
 * Copyright © 2025 Strato GmbH
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

package status

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/switches"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

type MaintApiConfig struct {
	Enabled bool   `yaml:"enabled,omitempty"`
	Prefix  string `yaml:"prefix,omitempty"`
}

type Config struct {
	Enabled       bool           `yaml:"enabled,omitempty"`
	Prefix        string         `yaml:"prefix,omitempty"`
	StatusPath    string         `yaml:"path,omitempty"`
	Port          int            `yaml:"port"`
	CheckInterval string         `yaml:"checkinterval,omitempty"`
	MaintApi      MaintApiConfig `yaml:"maintApi,omitempty"`
}

const defaultStatusPath = "bucket"
const defaultCheckInterval = "5s"

type migrationStatus int

const (
	statusUnknown migrationStatus = iota
	statusRunning
	statusLiveSync
	statusDone
	statusInconsistent
	statusFailed
)

func (ms migrationStatus) String() string {
	switch ms {
	case statusUnknown:
		return "unknown"
	case statusRunning:
		return "running"
	case statusLiveSync:
		return "livesync"
	case statusDone:
		return "done"
	case statusInconsistent:
		return "inconsistent"
	case statusFailed:
		return "failed"
	default:
		return "invalid status"
	}
}

func (ms migrationStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(ms.String())
}

type S3FloatMigrationStatusResponse struct {
	Bucket          string             `json:"bucket"`
	Status          migrationStatus    `json:"status"`
	Error           string             `json:"error,omitempty"`
	InitialProgress *migrationProgress `json:"initial_migration_progress,omitempty"`
	LiveProgress    *migrationProgress `json:"live_migration_progress,omitempty"`
}

type migrationProgress struct {
	Done        int `json:"done"`
	Pending     int `json:"pending"`
	Rescheduled int `json:"rescheduled"`
	Failed      int `json:"failed"`
}

func Handler(conf Config, logger zerolog.Logger, cSrv pb.ChorusServer, pSvc policy.Service, qSvc tasks.QueueService) (http.Handler, error) {
	var err error
	var prefix = ""
	if conf.Prefix != "" {
		prefix = strings.Trim(conf.Prefix, "/")
	}
	// The underscore prevents collisions with S3 bucket names (which cannot contain underscores).
	if !strings.Contains(prefix, "_") {
		return nil, errors.New("prefix is required and has to contain a '_'")
	}

	statusPath := defaultStatusPath
	if conf.StatusPath != "" {
		statusPath = strings.Trim(conf.StatusPath, "/")
	}
	if prefix == "" {
		statusPath = fmt.Sprintf("/%s/", statusPath)
	} else {
		statusPath = fmt.Sprintf("/%s/%s/", prefix, statusPath)
	}
	logger.Info().Str("prefix", prefix).Str("status_path", statusPath).Msg("setting up status api")

	var checkInterval time.Duration
	if conf.CheckInterval != "" {
		checkInterval, err = time.ParseDuration(conf.CheckInterval)
	} else {
		checkInterval, err = time.ParseDuration(defaultCheckInterval)
	}
	if err != nil {
		return nil, err
	}

	rcloneMutex := sync.Mutex{}
	rcloneMap := map[string]bool{}

	lockRcloneBucket := func(bucket string) bool {
		rcloneMutex.Lock()
		defer rcloneMutex.Unlock()
		_, exists := rcloneMap[bucket]
		if exists {
			return false
		}
		rcloneMap[bucket] = true
		return true
	}
	unlockRcloneBucket := func(bucket string) {
		rcloneMutex.Lock()
		defer rcloneMutex.Unlock()
		delete(rcloneMap, bucket)
	}

	chorusMutex := sync.Mutex{}
	chorusMap := map[string]bool{}

	lockChorusBucket := func(bucket string) bool {
		chorusMutex.Lock()
		defer chorusMutex.Unlock()
		_, exists := chorusMap[bucket]
		if exists {
			return false
		}
		chorusMap[bucket] = true
		return true
	}
	unlockChorusBucket := func(bucket string) {
		chorusMutex.Lock()
		defer chorusMutex.Unlock()
		delete(chorusMap, bucket)
	}

	srv := http.NewServeMux()
	srv.HandleFunc(statusPath, func(w http.ResponseWriter, r *http.Request) {
		handleStatusRequest(logger, cSrv, pSvc, checkInterval, statusPath, lockRcloneBucket, unlockRcloneBucket, lockChorusBucket, unlockChorusBucket, w, r)
	})

	if conf.MaintApi.Enabled {
		maintPrefix := strings.Trim(conf.MaintApi.Prefix, "/")
		if maintPrefix == "" {
			maintPrefix = "maint_api"
		}
		// The underscore prevents collisions with S3 bucket names (which cannot contain underscores).
		if !strings.Contains(maintPrefix, "_") {
			return nil, errors.New("maint_api prefix must contain a '_'")
		}
		failedTasksPattern := fmt.Sprintf("/%s/bucket/{bucket}/failed-tasks", maintPrefix)
		migStatusPattern := fmt.Sprintf("/%s/bucket/{bucket}/mig-status", maintPrefix)
		logger.Info().Str("failedTasksPath", failedTasksPattern).Str("migStatusPath", migStatusPattern).Msg("setting up maint api")
		srv.HandleFunc(failedTasksPattern, func(w http.ResponseWriter, r *http.Request) {
			handleMaintFailedTasks(logger, qSvc, pSvc, r.PathValue("bucket"), w, r)
		})
		listingSpeedPattern := fmt.Sprintf("/%s/listing-speed", maintPrefix)
		logger.Info().Str("listingSpeedPath", listingSpeedPattern).Msg("setting up listing speed api")
		srv.HandleFunc(listingSpeedPattern, func(w http.ResponseWriter, r *http.Request) {
			handleListingSpeed(logger, w, r)
		})
		srv.HandleFunc(migStatusPattern, func(w http.ResponseWriter, r *http.Request) {
			handleMaintMigStatus(logger, cSrv, pSvc, checkInterval, r.PathValue("bucket"),
				lockRcloneBucket, unlockRcloneBucket, lockChorusBucket, unlockChorusBucket, w, r)
		})
	} else {
		logger.Info().Msg("maint api is disabled")
	}

	return srv, nil
}

type failedTask struct {
	TaskID       string     `json:"taskId,omitempty"`
	Object       string     `json:"object,omitempty"`
	Bucket       string     `json:"bucket,omitempty"`
	ToBucket     string     `json:"toBucket,omitempty"`
	ErrorMessage string     `json:"errorMessage,omitempty"`
	RetryCount   int        `json:"retryCount"`
	MaxRetry     int        `json:"maxRetry"`
	LastFailedAt *time.Time `json:"lastFailedAt,omitempty"`
	TaskType     string     `json:"taskType,omitempty"`
}

func taskInfoToFailedTask(t *asynq.TaskInfo) (*failedTask, error) {
	ft := &failedTask{
		TaskID:       t.ID,
		ErrorMessage: t.LastErr,
		RetryCount:   t.Retried,
		MaxRetry:     t.MaxRetry,
		TaskType:     t.Type,
	}
	if !t.LastFailedAt.IsZero() {
		ts := t.LastFailedAt.UTC()
		ft.LastFailedAt = &ts
	}
	info, err := tasks.ParseTaskObjectInfo(t.Type, t.Payload)
	if err != nil {
		return nil, err
	}
	if info != nil {
		ft.Object = info.Object
		ft.Bucket = info.Bucket
		ft.ToBucket = info.ToBucket
	}
	return ft, nil
}

// lookupReplication finds the single replication for the given bucket.
// Returns dom.ErrNotFound if no replication exists.
func lookupReplication(ctx context.Context, logger zerolog.Logger, pSvc policy.Service, bucket string) (entity.ReplicationStatusID, entity.ReplicationStatusExtended, error) {
	replications, err := pSvc.ListReplicationPolicyInfo(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("ListReplicationPolicyInfo failed")
		return entity.ReplicationStatusID{}, entity.ReplicationStatusExtended{}, fmt.Errorf("list replication policies: %w", err)
	}

	var (
		found    bool
		foundID  entity.ReplicationStatusID
		foundExt entity.ReplicationStatusExtended
		extra    int
		mismatch bool
	)
	for id, status := range replications {
		if id.FromBucket != bucket {
			logger.Error().Str("fromBucket", id.FromBucket).Msg("non-matching replication detected")
			mismatch = true
			continue
		}
		if found {
			logger.Error().Str("fromBucket", id.FromBucket).Msg("superfluous replication detected")
			extra++
			continue
		}
		foundID = id
		foundExt = status
		found = true
	}

	if extra > 0 {
		return entity.ReplicationStatusID{}, entity.ReplicationStatusExtended{}, fmt.Errorf("%w: multiple replications detected", dom.ErrInternal)
	}
	if mismatch {
		return entity.ReplicationStatusID{}, entity.ReplicationStatusExtended{}, fmt.Errorf("%w: replication bucket mismatch", dom.ErrInternal)
	}
	if !found {
		return entity.ReplicationStatusID{}, entity.ReplicationStatusExtended{}, dom.ErrNotFound
	}
	return foundID, foundExt, nil
}

func handleMaintFailedTasks(logger zerolog.Logger, qSvc tasks.QueueService, pSvc policy.Service, bucket string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		if err := json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"}); err != nil {
			logger.Error().Err(err).Msg("failed to encode response")
		}
		return
	}

	ctx := r.Context()

	var rspCode int
	var rspBody any

	defer func() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rspCode)
		if err := json.NewEncoder(w).Encode(rspBody); err != nil {
			logger.Error().Err(err).Msg("failed to encode response")
		}
	}()

	params := r.URL.Query()

	replID, _, err := lookupReplication(ctx, logger, pSvc, bucket)
	if errors.Is(err, dom.ErrNotFound) {
		rspCode = http.StatusNotFound
		rspBody = map[string]string{"error": "no replication found for bucket"}
		return
	}
	if err != nil {
		rspCode = http.StatusInternalServerError
		rspBody = map[string]string{"error": "internal error"}
		return
	}

	errorFilter := params.Get("error_filter")

	queues := tasks.AllReplicationQueues(replID)
	var results []*failedTask

	for _, queue := range queues {
		infos, err := qSvc.ListFailedTasks(ctx, queue)
		if err != nil {
			logger.Error().Err(err).Str("queue", queue).Msg("list failed tasks failed")
			rspCode = http.StatusInternalServerError
			rspBody = map[string]string{"error": "internal error"}
			return
		}
		for _, info := range infos {
			ft, err := taskInfoToFailedTask(info)
			if err != nil {
				logger.Error().Err(err).Str("taskId", info.ID).Msg("failed to parse task payload, skipping")
				continue
			}
			results = append(results, ft)
		}
	}

	if errorFilter != "" {
		filtered := results[:0]
		for _, t := range results {
			if !strings.Contains(t.ErrorMessage, errorFilter) {
				continue
			}
			filtered = append(filtered, t)
		}
		results = filtered
	}

	if results == nil {
		results = []*failedTask{}
	}

	rspCode = http.StatusOK
	rspBody = results
}

func handleStatusRequest(logger zerolog.Logger, cSrv pb.ChorusServer, pSvc policy.Service, checkPollInterval time.Duration, prefix string, lockRcloneFunc func(string) bool, unlockRcloneFunc func(string), lockChorusFunc func(string) bool, unlockChorusFunc func(string), w http.ResponseWriter, r *http.Request) {
	bucket := strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/")
	writeErr := func(code int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if err := json.NewEncoder(w).Encode(S3FloatMigrationStatusResponse{Bucket: bucket, Error: msg}); err != nil {
			logger.Error().Err(err).Msg("failed to encode error response")
		}
	}
	if strings.ContainsRune(bucket, '/') {
		writeErr(http.StatusBadRequest, "unexpected path")
		return
	}
	if bucket == "" {
		writeErr(http.StatusBadRequest, "bucket missing")
		return
	}
	handleMaintMigStatus(logger, cSrv, pSvc, checkPollInterval, bucket, lockRcloneFunc, unlockRcloneFunc, lockChorusFunc, unlockChorusFunc, w, r)
}

func handleMaintMigStatus(logger zerolog.Logger, cSrv pb.ChorusServer, pSvc policy.Service, checkPollInterval time.Duration, bucket string, lockRcloneFunc func(string) bool, unlockRcloneFunc func(string), lockChorusFunc func(string) bool, unlockChorusFunc func(string), w http.ResponseWriter, r *http.Request) {
	var err error
	initProgress := migrationProgress{}
	liveProgress := migrationProgress{}
	progress := false
	rspCode := http.StatusInternalServerError
	rsp := S3FloatMigrationStatusResponse{
		Status: statusUnknown,
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	defer func() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rspCode)
		if rspCode != http.StatusOK && rsp.Error == "" {
			rsp.Error = http.StatusText(rspCode)
		}
		rsp.Bucket = bucket
		if progress {
			rsp.InitialProgress = &initProgress
			rsp.LiveProgress = &liveProgress
		}
		err := json.NewEncoder(w).Encode(rsp)
		if err != nil {
			logger.Error().Err(err).Msg("writing json response failed")
		}
	}()

	logger = logger.With().Str("bucket", bucket).Logger()
	params := r.URL.Query()
	check := false
	checkChorus := false
	if params.Get("check") != "" {
		check, err = strconv.ParseBool(params.Get("check"))
		if err != nil {
			rspCode = http.StatusBadRequest
			rsp.Error = "check parameter has to be a boolean"
			return
		}
	}
	if check && params.Get("check.chorus") != "" {
		checkChorus, err = strconv.ParseBool(params.Get("check.chorus"))
		if err != nil {
			rspCode = http.StatusBadRequest
			rsp.Error = "check.chorus parameter has to be a boolean"
			return
		}
	}
	if params.Get("status.progress") != "" {
		progress, err = strconv.ParseBool(params.Get("status.progress"))
		if err != nil {
			rspCode = http.StatusBadRequest
			rsp.Error = "status.progress parameter has to be a boolean"
			return
		}
	}
	logger.Debug().Bool("check", check).Bool("progress", progress).Bool("checkChorus", checkChorus).Msg("processing status request")

	replId, replExt, err := lookupReplication(ctx, logger, pSvc, bucket)
	if errors.Is(err, dom.ErrNotFound) {
		logger.Info().Msg("no matching replication configured. returning success with status unknown")
		rspCode = http.StatusOK
		rsp.Status = statusUnknown
		return
	}
	if err != nil {
		rspCode = http.StatusInternalServerError
		rsp.Error = "internal error"
		return
	}
	repl := &replExt

	if progress {
		initProgress = migrationProgress{
			Done:        repl.InitMigration.Done,
			Pending:     repl.InitMigration.Pending,
			Failed:      repl.InitMigration.Failed,
			Rescheduled: repl.InitMigration.Rescheduled,
		}
		liveProgress = migrationProgress{
			Done:        repl.EventMigration.Done,
			Pending:     repl.EventMigration.Pending,
			Failed:      repl.EventMigration.Failed,
			Rescheduled: repl.EventMigration.Rescheduled,
		}
	}

	if repl.InitMigration.Failed+repl.EventMigration.Failed > 0 {
		rspCode = http.StatusOK
		rsp.Status = statusFailed
		return
	}

	if !repl.InitDone() {
		rspCode = http.StatusOK
		rsp.Status = statusRunning
		return
	}

	if repl.EventMigration.Pending > 0 {
		rspCode = http.StatusOK
		rsp.Status = statusLiveSync
		return
	}

	if !check {
		rspCode = http.StatusOK
		rsp.Status = statusDone
		return
	}

	// rclone based consistency check
	if !checkChorus {
		if !lockRcloneFunc(bucket) {
			rspCode = http.StatusServiceUnavailable
			rsp.Error = "rclone consistency check already running for another client"
			return
		}
		defer unlockRcloneFunc(bucket)
		rcloneReq := pb.CompareBucketRequest{
			User:     replId.User,
			Bucket:   replId.FromBucket,
			From:     replId.FromStorage,
			To:       replId.ToStorage,
			ToBucket: replId.ToBucket,
		}
		rcloneRsp, err := cSrv.CompareBucket(ctx, &rcloneReq)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				rsp.Error = err.Error()
				logger.Error().Err(err).Msg("CompareBucket failed")
				return
			}
			logger.Info().Msg("CompareBucket cancelled")
			return
		}
		rspCode = http.StatusOK
		if rcloneRsp.IsMatch {
			rsp.Status = statusDone
		} else {
			rsp.Status = statusInconsistent
		}
		logger.Info().Str("status", rsp.Status.String()).Msg("CompareBucket finished")
		return
	}
	if !lockChorusFunc(bucket) {
		rspCode = http.StatusServiceUnavailable
		rsp.Error = "chorus consistency check already running for another client"
		return
	}
	defer unlockChorusFunc(bucket)

	// chorus based consistency check
	consistencyReq := pb.ConsistencyCheckRequest{
		Locations: []*pb.MigrateLocation{
			&pb.MigrateLocation{Storage: replId.FromStorage, Bucket: replId.FromBucket, User: replId.User},
			&pb.MigrateLocation{Storage: replId.ToStorage, Bucket: replId.ToBucket, User: replId.User},
		},
	}
	_, err = cSrv.StartConsistencyCheck(ctx, &consistencyReq)
	if err != nil {
		if err.Error() == "consistency check for this set of storage locations already exists" {
			rspCode = http.StatusServiceUnavailable
			rsp.Error = "chorus consistency check already running for another client"
			return
		}
		if !errors.Is(err, context.Canceled) {
			rsp.Error = err.Error()
			logger.Error().Err(err).Msg("StartConsistencyCheck failed")
			return
		}
		logger.Info().Err(err).Msg("StartConsistencyCheck canceled")
		return
	}
	defer func() {
		_, err := cSrv.DeleteConsistencyCheckReport(context.Background(), &consistencyReq)
		if err != nil {
			logger.Error().Err(err).Msg("DeleteConsistencyCheckReport failed")
		}
	}()
	ticker := time.NewTicker(checkPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := cSrv.GetConsistencyCheckReport(ctx, &consistencyReq)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					rsp.Error = err.Error()
					logger.Error().Err(err).Msg("GetConsistencyCheckReport failed")
				}
				logger.Info().Err(err).Msg("GetConsistencyCheckReport canceled")
				return
			}
			if res.Check.Ready {
				rspCode = http.StatusOK
				if res.Check.Consistent {
					rsp.Status = statusDone
				} else {
					rsp.Status = statusInconsistent
				}
				return
			}
		}
	}
}

type listingSpeedResponse struct {
	Speed string `json:"listing_speed"`
	Error string `json:"error,omitempty"`
}

// handleListingSpeed reports the bucket listing speed of this instance, and
// changes it on PUT. The change lasts until the next restart, which applies
// the configured speed again.
func handleListingSpeed(logger zerolog.Logger, w http.ResponseWriter, r *http.Request) {
	write := func(code int, rsp listingSpeedResponse) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if err := json.NewEncoder(w).Encode(rsp); err != nil {
			logger.Error().Err(err).Msg("failed to encode listing speed response")
		}
	}
	switch r.Method {
	case http.MethodGet:
		write(http.StatusOK, listingSpeedResponse{Speed: switches.ListingSpeed()})
	case http.MethodPut, http.MethodPost:
		speed := strings.TrimSpace(r.URL.Query().Get("speed"))
		if err := switches.SetListingSpeed(speed); err != nil || speed == "" {
			write(http.StatusBadRequest, listingSpeedResponse{
				Speed: switches.ListingSpeed(),
				Error: fmt.Sprintf("speed query parameter must be %s or %s", switches.ListingFull, switches.ListingAuto),
			})
			return
		}
		logger.Info().Str("listing_speed", speed).Msg("bucket listing speed changed")
		write(http.StatusOK, listingSpeedResponse{Speed: switches.ListingSpeed()})
	default:
		write(http.StatusMethodNotAllowed, listingSpeedResponse{
			Speed: switches.ListingSpeed(),
			Error: "use GET to read and PUT to change the listing speed",
		})
	}
}
