// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package web

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
)

// Test for non-availability API endpoints in Prometheus Agent mode.
func TestAPIEndPoints(t *testing.T) {
	t.Parallel()

	opts := &Options{
		ListenAddress:  ":9090",
		ReadTimeout:    30 * time.Second,
		MaxConnections: 512,
		Context:        nil,
		Storage:        nil,
		QueryEngine:    nil,
		ScrapeManager:  &scrape.Manager{},
		RuleManager:    &rules.Manager{},
		Notifier:       nil,
		RoutePrefix:    "/",
		EnableAdminAPI: true,
		ExternalURL: &url.URL{
			Scheme: "http",
			Host:   "localhost:9090",
			Path:   "/",
		},
		Version:  &PrometheusVersion{},
		Gatherer: prometheus.DefaultGatherer,
		IsAgent:  true,
	}

	opts.Flags = map[string]string{}

	webHandler := New(nil, opts)
	webHandler.Ready()

	for _, u := range []string{
		"http://localhost:9090/-/labels",
		"http://localhost:9090/label",
		"http://localhost:9090/series",
		"http://localhost:9090/alertmanagers",
		"http://localhost:9090/query",
		"http://localhost:9090/query_range",
		"http://localhost:9090/query_exemplars",
	} {
		w := httptest.NewRecorder()

		req, err := http.NewRequest("GET", u, nil)

		require.NoError(t, err)

		webHandler.router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	}
}
