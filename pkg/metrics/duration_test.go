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

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/s3"
)

// histogram returns the series of a histogram family that carries all of the
// given label values.
func histogram(t *testing.T, name string, labels ...string) *dto.Histogram {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			values := map[string]bool{}
			for _, label := range metric.GetLabel() {
				values[label.GetValue()] = true
			}
			found := true
			for _, want := range labels {
				if !values[want] {
					found = false
					break
				}
			}
			if found {
				return metric.GetHistogram()
			}
		}
	}
	return nil
}

func Test_ProxyRequestDuration(t *testing.T) {
	r := require.New(t)
	ProxyRequestDuration("GetObject", "destination", 100*time.Millisecond)

	h := histogram(t, "proxy_request_duration_seconds", "GetObject", "destination")
	r.NotNil(h, "series not exported")
	r.EqualValues(1, h.GetSampleCount())
	r.InDelta(0.1, h.GetSampleSum(), 0.001)

	// a request that never reached a storage still has to carry a label value
	ProxyRequestDuration("GetObject", "", time.Second)
	r.NotNil(histogram(t, "proxy_request_duration_seconds", "GetObject", "none"))
}

func Test_MigrationCopyPhase(t *testing.T) {
	r := require.New(t)
	for _, phase := range []string{"copy", "acl", "tags"} {
		MigrationCopyPhase(phase, "source", "destination", 2*time.Second)
		h := histogram(t, "migration_copy_phase_duration_seconds", phase, "source", "destination")
		r.NotNil(h, "phase %q not exported", phase)
		r.EqualValues(1, h.GetSampleCount())
		r.InDelta(2, h.GetSampleSum(), 0.001)
	}
}

func Test_S3Service_Duration(t *testing.T) {
	r := require.New(t)
	NewS3Service(true).Duration(xctx.Migration, "source", s3.GetObject, 11*time.Second)

	h := histogram(t, "storage_request_duration_seconds", string(xctx.Migration), "source", s3.GetObject.String())
	r.NotNil(h, "series not exported")
	r.EqualValues(1, h.GetSampleCount())
	r.InDelta(11, h.GetSampleSum(), 0.001)

	// disabled service records nothing
	NewS3Service(false).Duration(xctx.Migration, "other-storage", s3.GetObject, time.Second)
	r.Nil(histogram(t, "storage_request_duration_seconds", "other-storage"))
}
