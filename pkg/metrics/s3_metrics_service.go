/*
 * Copyright © 2023 Clyso GmbH
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

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/s3"
)

var requestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "storage_request_duration_seconds",
		Help:    "Duration of api calls to s3 storage, by the outcome they ended in.",
		Buckets: storageLatencyBuckets,
	},
	[]string{"flow", "storage", "method", "status"},
)

// ReqStatus is the bounded outcome vocabulary for the "status" label
// on storage_requests_total. Keeping it to a handful of classes (vs.
// raw HTTP codes) bounds cardinality and lets the data path and the
// control-plane share one label vocabulary.
type ReqStatus string

const (
	StatusOK         ReqStatus = "ok"
	StatusClientErr  ReqStatus = "client_error"
	StatusServerErr  ReqStatus = "server_error"
	StatusNetworkErr ReqStatus = "network_error"
	// StatusInternalErr is a call the storage never answered and never
	// refused: it was given up on before it left, or cancelled. Kept apart
	// from network_error so that the one class meaning "the storage is
	// unreachable" says only that.
	StatusInternalErr ReqStatus = "internal_error"
)

var countRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_requests_total",
		Help: "Number of api calls to s3 storage.",
	},
	[]string{"flow", "storage", "method", "status"},
)

var bytesUpload = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_bucket_bytes_upload",
		Help: "Number of bytes uploaded to s3 storage.",
	},
	[]string{"flow", "storage", "bucket"},
)

var bytesDownload = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_bucket_bytes_download",
		Help: "Number of bytes downloaded from s3 storage.",
	},
	[]string{"flow", "storage", "bucket"},
)

var rcloneCalcUsage = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "rclone_calc_mem_usage",
		Help: "Calculated rclone_memory_usage.",
	},
)

var rcloneFilesSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "rclone_file_size_processing",
		Help: "Size of files currently processed with rclone.",
	},
)

var rcloneFilesNum = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "rclone_file_num_processing",
		Help: "Amount of files currently processed with rclone.",
	},
)

// rcloneFsCache counts cached rclone Fs instances. Each distinct
// (storage, bucket, user) Fs owns one HTTP transport / connection
// pool shared across copy tasks, so this is the pool-layer signal for
// rclone — rclone builds its own dialer internally, so true per-socket
// open/close counts are not available the way they are for the http
// layer (see metrics.ConnLayerHTTP).
var rcloneFsCache = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "rclone_fs_cache_entries",
		Help: "Number of cached rclone Fs instances (shared connection pools).",
	},
)

type S3Service interface {
	// Count records a successful api call (status="ok"). It is the
	// convenience form for the many call sites that only fire on
	// success; for paths that observe the real outcome use CountStatus.
	Count(flow xctx.Flow, storage string, method s3.Method)
	// CountStatus records an api call with an explicit outcome class,
	// so the same storage_requests_total series also carries error
	// counts under its "status" label.
	CountStatus(flow xctx.Flow, storage string, method s3.Method, status ReqStatus)
	// Duration records how long a storage took to answer an api call, with
	// the outcome it ended in: an average that mixes the calls that failed
	// with the ones that worked explains neither.
	Duration(flow xctx.Flow, storage string, method s3.Method, status ReqStatus, d time.Duration)
	Upload(flow xctx.Flow, storage, bucket string, bytes int)
	Download(flow xctx.Flow, storage, bucket string, bytes int)

	RcloneCalcMemUsageInc(bytes int64)
	RcloneCalcMemUsageDec(bytes int64)
	RcloneCalcFileSizeInc(bytes int64)
	RcloneCalcFileSizeDec(bytes int64)
	RcloneCalcFileNumInc()
	RcloneCalcFileNumDec()
	// RcloneFsCacheInc records that a new rclone Fs (and its connection
	// pool) was added to the cache.
	RcloneFsCacheInc()
}

func NewS3Service(enabled bool) S3Service {
	return &svcS3{enabled: enabled}
}

type svcS3 struct {
	enabled bool
}

func (s svcS3) RcloneCalcMemUsageInc(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneCalcUsage.Add(float64(bytes))
}

func (s svcS3) RcloneCalcMemUsageDec(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneCalcUsage.Sub(float64(bytes))
}

func (s svcS3) RcloneCalcFileSizeInc(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneFilesSize.Add(float64(bytes))
}

func (s svcS3) RcloneCalcFileSizeDec(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneFilesSize.Sub(float64(bytes))
}

func (s svcS3) RcloneCalcFileNumInc() {
	if !s.enabled {
		return
	}
	rcloneFilesNum.Add(float64(1))
}

func (s svcS3) RcloneCalcFileNumDec() {
	if !s.enabled {
		return
	}
	rcloneFilesNum.Sub(float64(1))
}

func (s svcS3) RcloneFsCacheInc() {
	if !s.enabled {
		return
	}
	rcloneFsCache.Inc()
}

func (s svcS3) Count(flow xctx.Flow, storage string, method s3.Method) {
	s.CountStatus(flow, storage, method, StatusOK)
}

func (s svcS3) CountStatus(flow xctx.Flow, storage string, method s3.Method, status ReqStatus) {
	if !s.enabled {
		return
	}
	countRequests.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"method":  method.String(),
		"status":  string(status)}).Inc()
}

func (s svcS3) Duration(flow xctx.Flow, storage string, method s3.Method, status ReqStatus, d time.Duration) {
	if !s.enabled {
		return
	}
	requestDuration.WithLabelValues(string(flow), storage, method.String(), string(status)).Observe(d.Seconds())
}

func (s svcS3) Upload(flow xctx.Flow, storage, bucket string, bytes int) {
	if !s.enabled {
		return
	}
	bytesUpload.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"bucket":  bucket}).Add(float64(bytes))
}

func (s svcS3) Download(flow xctx.Flow, storage, bucket string, bytes int) {
	if !s.enabled {
		return
	}
	bytesDownload.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"bucket":  bucket}).Add(float64(bytes))
}
