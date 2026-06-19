// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/util/compression"
)

// TestUpdateMetadataMemory measures the retained heap cost of enabling
// per-series metadata, the path that becomes hot when prometheus.scrape runs
// with honor_metadata=true (Alloy) or with --enable-feature=metadata-wal-records
// (vanilla Prometheus agent mode).
//
// For each scrape shape it builds a head, pre-creates many series sharing a
// small number of metric names, optionally attaches metadata, then reports
// HeapInuse bytes per series with metadata off and on. The gap between the two
// is the per-series overhead of enabling metadata, which is what e.g. the
// unique.Handle interning is meant to shrink.
//
// This is a measurement, not a throughput benchmark, so it is a plain test:
// no b.N loop, no timing. Run it and read the logged numbers:
//
//	go test ./tsdb -run='TestUpdateMetadataMemory' -v
//
// To check whether a change shrank the overhead, run it on this branch and on
// a baseline and compare the logged "overhead" B/series by hand.
func TestUpdateMetadataMemory(t *testing.T) {
	cases := []struct {
		numMetricNames  int
		seriesPerMetric int
	}{
		{numMetricNames: 1000, seriesPerMetric: 1000}, // 100k series
	}
	for _, tc := range cases {
		total := tc.numMetricNames * tc.seriesPerMetric
		off := metadataHeapInuse(t, tc.numMetricNames, tc.seriesPerMetric, false)
		on := metadataHeapInuse(t, tc.numMetricNames, tc.seriesPerMetric, true)
		t.Logf("series=%d: metadata off=%d B/series, on=%d B/series, overhead=%d B/series",
			total, off/uint64(total), on/uint64(total), (on-off)/uint64(total))
	}
}

// metadataHeapInuse pre-creates numMetricNames*seriesPerMetric series in a
// fresh head, optionally attaches metadata to each, and returns the
// process HeapInuse after the data is committed and live. Comparing the value
// with metadata off vs on cancels out the baseline and leaves the metadata
// overhead.
func metadataHeapInuse(t *testing.T, numMetricNames, seriesPerMetric int, metadataEnabled bool) uint64 {
	// A realistic HELP string — verbose Prometheus help text is the dominant
	// per-series memory cost without interning.
	helpFiller := strings.Repeat("x", 160)

	h, _ := newTestHead(t, 10000, compression.None, false)
	defer func() { require.NoError(t, h.Close()) }()

	app := h.Appender(context.Background())
	for i := 0; i < numMetricNames; i++ {
		md := metadata.Metadata{
			Type: "counter",
			Unit: "requests",
			Help: fmt.Sprintf("metric_%d: %s", i, helpFiller),
		}
		for j := 0; j < seriesPerMetric; j++ {
			lset := labels.FromStrings(
				labels.MetricName, fmt.Sprintf("metric_%d", i),
				"instance", fmt.Sprintf("instance_%d", j),
				"job", "bench",
			)
			_, err := app.Append(0, lset, 0, 0)
			require.NoError(t, err)
			if metadataEnabled {
				_, err := app.UpdateMetadata(0, lset, md)
				require.NoError(t, err)
			}
		}
	}
	require.NoError(t, app.Commit())

	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}
