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

package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/tasks"
)

// routeResult is what a Router hands the http layer back.
type routeResult struct {
	resp     *http.Response
	storage  string
	isApiErr bool
	err      error
}

type fakeRouter struct{ result routeResult }

func (f fakeRouter) Route(*http.Request) (*http.Response, []tasks.SyncTask, string, bool, error) {
	return f.result.resp, nil, f.result.storage, f.result.isApiErr, f.result.err
}

// statusCount returns what proxy_storage_status_total holds for one storage
// and status right now.
func statusCount(t *testing.T, storage, status string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "proxy_storage_status_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range metric.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["storage"] == storage && labels["status"] == status {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// Test_proxyStorageStatusCountsErrors pins that a storage answering with an
// error is counted. Every status outside the handful chorus treats as success
// leaves client.Do as an error, so the handler returns before the counting it
// does on the way out; counting only there would leave the metric holding
// successes alone, and a dashboard reading "no storage errors" while a
// storage answers 503 to everything.
func Test_proxyStorageStatusCountsErrors(t *testing.T) {
	before503 := statusCount(t, "source", "503")
	handler := Serve(fakeRouter{routeResult{
		storage:  "source",
		isApiErr: true,
		err:      mclient.ErrorResponse{StatusCode: 503, Code: "SlowDown"},
	}}, nil)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/bucket/object", strings.NewReader("")))

	require.InDelta(t, before503+1, statusCount(t, "source", "503"), 0.001,
		"a 503 from the storage has to reach proxy_storage_status_total")
}

// Test_proxyStorageStatusIgnoresNonAnswers pins the other side: a request that
// never got a storage answer has no status to count, and must not invent one.
func Test_proxyStorageStatusIgnoresNonAnswers(t *testing.T) {
	before := statusCount(t, "source", "0")
	handler := Serve(fakeRouter{routeResult{
		storage: "source",
		err:     context.Canceled,
	}}, nil)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/bucket/object", strings.NewReader("")))

	require.InDelta(t, before, statusCount(t, "source", "0"), 0.001,
		"a request the storage never answered must not be counted under any status")
}
