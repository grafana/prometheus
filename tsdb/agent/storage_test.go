// Copyright 2021 The Prometheus Authors
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

package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
)

var promAgentData = filepath.Join(os.TempDir(), "data-agent")

func TestUnsupported(t *testing.T) {
	opts := DefaultOptions()
	cfg := promlog.Config{}
	logger := promlog.New(&cfg)

	s, err := NewStorage(logger, prometheus.DefaultRegisterer, nil, promAgentData, opts)
	if err != nil {
		t.Fatalf("unable to create storage for the agent: %v", err)
	}
	defer s.Close()

	t.Run("Querier", func(t *testing.T) {
		_, err := s.Querier(context.TODO(), 0, 0)
		require.Equal(t, err, ErrUnsupported)
	})

	t.Run("ChunkQuerier", func(t *testing.T) {
		_, err := s.ChunkQuerier(context.TODO(), 0, 0)
		require.Equal(t, err, ErrUnsupported)
	})

	t.Run("ExemplarQuerier", func(t *testing.T) {
		_, err := s.ExemplarQuerier(context.TODO())
		require.Equal(t, err, ErrUnsupported)
	})
}

func TestPreCommit(t *testing.T) {
	const numDatapoints = 1000

	// Create 8 series with 1000 data-points of different ranges and run queries.
	lbls := labelsForTest()
	opts := DefaultOptions()
	cfg := promlog.Config{}
	logger := promlog.New(&cfg)
	reg := prometheus.NewRegistry()

	s, err := NewStorage(logger, reg, nil, promAgentData, opts)
	if err != nil {
		t.Fatalf("unable to create storage for the agent: %v", err)
	}

	a := s.Appender(context.TODO())
	for _, l := range lbls {
		lset := labels.New(l...)
		for i := 0; i < numDatapoints; i++ {
			sample := tsdbutil.GenerateSamples(0, 1)
			_, err := a.Append(0, lset, sample[0].T(), sample[0].V())
			require.NoError(t, err)
		}
	}

	var samplesAdded float64
	var actvieTimeSeries float64

	m := gatherFamily(t, reg, "prometheus_agent_wal_active_series")
	actvieTimeSeries = m.Metric[0].Gauge.GetValue()

	m = gatherFamily(t, reg, "prometheus_agent_wal_samples_appended_total")
	samplesAdded = m.Metric[0].Counter.GetValue()

	require.Equal(t, samplesAdded, float64(numDatapoints*8))
	require.Equal(t, actvieTimeSeries, float64(8))
}

func TestCommit(t *testing.T) {
	const numDatapoints = 1000

	// Create 8 series with 1000 data-points of different ranges and run queries.
	lbls := labelsForTest()
	opts := DefaultOptions()
	cfg := promlog.Config{}
	logger := promlog.New(&cfg)
	reg := prometheus.NewRegistry()
	remoteStorage := remote.NewStorage(log.With(logger, "component", "remote"), reg, startTime, promAgentData, time.Second*30, nil)

	s, err := NewStorage(logger, reg, remoteStorage, promAgentData, opts)
	if err != nil {
		t.Fatalf("unable to create storage for the agent: %v", err)
	}

	a := s.Appender(context.TODO())
	for _, l := range lbls {
		lset := labels.New(l...)
		for i := 0; i < numDatapoints; i++ {
			sample := tsdbutil.GenerateSamples(0, 1)
			_, err := a.Append(0, lset, sample[0].T(), sample[0].V())
			require.NoError(t, err)
		}
	}

	require.NoError(t, a.Commit())
	time.Sleep(time.Second * 10)

	var walPages float64

	m := gatherFamily(t, reg, "prometheus_tsdb_wal_completed_pages_total")
	walPages = m.Metric[0].Counter.GetValue()

	require.GreaterOrEqual(t, walPages, float64(1))
}

func TestTruncateWAL(t *testing.T) {
	const numDatapoints = 1000

	// Create 8 series with 1000 data-points of different ranges and run queries.
	lbls := labelsForTest()
	opts := DefaultOptions()
	opts.TruncateFrequency = time.Second * 15
	cfg := promlog.Config{}
	logger := promlog.New(&cfg)
	reg := prometheus.NewRegistry()
	remoteStorage := remote.NewStorage(log.With(logger, "component", "remote"), reg, startTime, promAgentData, time.Second*30, nil)

	s, err := NewStorage(logger, reg, remoteStorage, promAgentData, opts)
	if err != nil {
		t.Fatalf("unable to create storage for the agent: %v", err)
	}

	a := s.Appender(context.TODO())

	for _, l := range lbls {
		lset := labels.New(l...)
		for i := 0; i < numDatapoints; i++ {
			sample := tsdbutil.GenerateSamples(0, 1)
			_, err := a.Append(0, lset, sample[0].T(), sample[0].V())
			require.NoError(t, err)

		}
	}

	require.NoError(t, a.Commit())
	time.Sleep(time.Second * 20)

	var deleteSeries float64

	m := gatherFamily(t, reg, "prometheus_agent_wal_deleted_series")
	deleteSeries = m.Metric[0].Gauge.GetValue()

	require.Equal(t, deleteSeries, float64(8))
}

func TestWALReplay(t *testing.T) {
	const numDatapoints = 1000

	// Create 8 series with 1000 data-points of different ranges and run queries.
	lbls := labelsForTest()
	opts := DefaultOptions()

	cfg := promlog.Config{}
	logger := promlog.New(&cfg)
	reg := prometheus.NewRegistry()
	remoteStorage := remote.NewStorage(log.With(logger, "component", "remote"), reg, startTime, promAgentData, time.Second*30, nil)

	s, err := NewStorage(logger, reg, remoteStorage, promAgentData, opts)
	if err != nil {
		t.Fatalf("unable to create storage for the agent: %v", err)
	}

	a := s.Appender(context.TODO())

	for _, l := range lbls {
		lset := labels.New(l...)
		for i := 0; i < numDatapoints; i++ {
			sample := tsdbutil.GenerateSamples(0, 1)
			_, err := a.Append(0, lset, sample[0].T(), sample[0].V())
			require.NoError(t, err)

		}
	}

	require.NoError(t, a.Commit())
	restartOpts := DefaultOptions()
	restartCfg := promlog.Config{}
	restartLogger := promlog.New(&restartCfg)
	restartReg := prometheus.NewRegistry()

	_, err = NewStorage(restartLogger, restartReg, nil, promAgentData, restartOpts)
	if err != nil {
		t.Fatalf("unable to create storage for the agent: %v", err)
	}

	var actvieTimeSeries float64

	m := gatherFamily(t, reg, "prometheus_agent_wal_active_series")
	actvieTimeSeries = m.Metric[0].Gauge.GetValue()

	require.Equal(t, actvieTimeSeries, float64(8))
}

func startTime() (int64, error) {
	return time.Now().Unix() * 1000, nil
}

func labelsForTest() []labels.Labels {
	// Create 8 series with 1000 data-points of different ranges and run queries.
	return []labels.Labels{
		{
			{Name: "a", Value: "b"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "prometheus"},
		},
		{
			{Name: "a", Value: "b"},
			{Name: "instance", Value: "127.0.0.1:9090"},
			{Name: "job", Value: "prometheus"},
		},
		{
			{Name: "a", Value: "b"},
			{Name: "instance", Value: "127.0.0.1:9090"},
			{Name: "job", Value: "prom-k8s"},
		},
		{
			{Name: "a", Value: "b"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "prom-k8s"},
		},
		{
			{Name: "a", Value: "c"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "prometheus"},
		},
		{
			{Name: "a", Value: "c"},
			{Name: "instance", Value: "127.0.0.1:9090"},
			{Name: "job", Value: "prometheus"},
		},
		{
			{Name: "a", Value: "c"},
			{Name: "instance", Value: "127.0.0.1:9090"},
			{Name: "job", Value: "prom-k8s"},
		},
		{
			{Name: "a", Value: "c"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "prom-k8s"},
		},
	}
}

func gatherFamily(t *testing.T, reg prometheus.Gatherer, familyName string) *dto.MetricFamily {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err, "failed to gather metrics")

	for _, f := range families {
		if f.GetName() == familyName {
			return f
		}
	}

	t.Fatalf("could not find family %s", familyName)
	return nil
}
