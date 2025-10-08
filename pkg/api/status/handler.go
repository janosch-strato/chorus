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

	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/policy"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

type Config struct {
	Enabled       bool   `yaml:"enabled,omitempty"`
	Prefix        string `yaml:"prefix,omitempty"`
	StatusPath    string `yaml:"path,omitempty"`
	Port          int    `yaml:"port"`
	CheckInterval string `yaml:"checkinterval,omitempty"`
}

const defaultStatusPath = "bucket"
const defaultCheckInterval = "5s"

type migrationStatus int

const (
	statusUnknown migrationStatus = iota
	statusRunning
	statusLiveSync
	statusPaused
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
	case statusPaused:
		return "paused"
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
	Done     int    `json:"done"`
	Pending  int    `json:"pending"`
	Progress string `json:"progress"`
}

func Handler(conf Config, logger zerolog.Logger, cSrv pb.ChorusServer, pSvc policy.Service) (http.Handler, error) {
	var err error
	var prefix = ""
	if conf.Prefix != "" {
		prefix = strings.Trim(conf.Prefix, "/")
	}
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

	return srv, nil
}

func handleStatusRequest(logger zerolog.Logger, cSrv pb.ChorusServer, pSvc policy.Service, checkPollInterval time.Duration, prefix string, lockRcloneFunc func(string) bool, unlockRcloneFunc func(string), lockChorusFunc func(string) bool, unlockChorusFunc func(string), w http.ResponseWriter, r *http.Request) {
	var err error
	var bucket string
	initProgress := migrationProgress{Progress: "0%"}
	liveProgress := migrationProgress{Progress: "0%"}
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

	bucket = strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/")
	if strings.ContainsRune(bucket, '/') {
		rspCode = http.StatusBadRequest
		rsp.Error = "unexpected path"
		return
	}
	if bucket == "" {
		rspCode = http.StatusBadRequest
		rsp.Error = "bucket missing"
		return
	}
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

	replications, err := pSvc.ListReplicationPolicyInfo(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("ListReplicationPolicyInfo failed")
		rspCode = http.StatusInternalServerError
		return
	}

	var repl *entity.ReplicationStatusExtended
	var replId *entity.ReplicationStatusID
	for id, status := range replications {
		if repl != nil {
			logger.Error().Str("User", id.User).Str("FromStorage", id.FromStorage).Str("FromBucket", id.FromBucket).Str("ToStorage", id.ToStorage).Str("ToBucket", id.ToBucket).Msg("superfluous migration detected")
			rspCode = http.StatusInternalServerError
			return
		}
		if id.FromBucket != bucket {
			logger.Error().Str("User", id.User).Str("FromStorage", id.FromStorage).Str("FromBucket", id.FromBucket).Str("ToStorage", id.ToStorage).Str("ToBucket", id.ToBucket).Msg("non-matching migration detected")
			rspCode = http.StatusInternalServerError
			return
		}
		repl = &status
		replId = &id
	}

	if repl == nil {
		logger.Info().Msg("no matching replication configured. returning success with status unknown")
		rspCode = http.StatusOK
		rsp.Status = statusUnknown
		return
	}

	if progress {
		ip := 0.0
		if repl.InitMigration.Done != 0 {
			ip = (float64(repl.InitMigration.Done) / float64(repl.InitMigration.Done+repl.InitMigration.Unprocessed)) * 100.0
		}
		initProgress = migrationProgress{
			Done:     repl.InitMigration.Done,
			Pending:  repl.InitMigration.Unprocessed,
			Progress: fmt.Sprintf("%.0f%%", ip),
		}
		lp := 0.0
		if repl.EventMigration.Done != 0 {
			lp = (float64(repl.EventMigration.Done) / float64(repl.EventMigration.Done+repl.EventMigration.Unprocessed)) * 100.0
		}
		liveProgress = migrationProgress{
			Done:     repl.EventMigration.Done,
			Pending:  repl.EventMigration.Unprocessed,
			Progress: fmt.Sprintf("%.0f%%", lp),
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

	if repl.EventMigration.Unprocessed > 0 {
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
