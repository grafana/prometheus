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

package main

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var promPath = os.Args[0]
var promConfig = filepath.Join("..", "..", "documentation", "examples", "prometheus-agent.yml")
var promData = filepath.Join(os.TempDir(), "data")

func TestMain(m *testing.M) {
	for i, arg := range os.Args {
		if arg == "-test.main" {
			os.Args = append(os.Args[:i], os.Args[i+1:]...)
			main()
			return
		}
	}

	// On linux with a global proxy the tests will fail as the go client(http,grpc) tries to connect through the proxy.
	os.Setenv("no_proxy", "localhost,127.0.0.1,0.0.0.0,:")

	exitCode := m.Run()
	os.RemoveAll(promData)
	os.Exit(exitCode)
}

func TestComputeExternalURL(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{
			input: "",
			valid: true,
		},
		{
			input: "http://proxy.com/prometheus",
			valid: true,
		},
		{
			input: "'https://url/prometheus'",
			valid: false,
		},
		{
			input: "'relative/path/with/quotes'",
			valid: false,
		},
		{
			input: "http://alertmanager.company.com",
			valid: true,
		},
		{
			input: "https://double--dash.de",
			valid: true,
		},
		{
			input: "'http://starts/with/quote",
			valid: false,
		},
		{
			input: "ends/with/quote\"",
			valid: false,
		},
	}

	for _, test := range tests {
		_, err := computeExternalURL(test.input, "0.0.0.0:9090")
		if test.valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err, "input=%q", test.input)
		}
	}
}

// Let's provide an invalid configuration file and verify the exit status indicates the error.
func TestFailedStartupExitCode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	fakeInputFile := "fake-input-file"
	expectedExitStatus := 2

	prom := exec.Command(promPath, "-test.main", "--config.file="+fakeInputFile)
	err := prom.Run()
	require.Error(t, err)

	if exitError, ok := err.(*exec.ExitError); ok {
		status := exitError.Sys().(syscall.WaitStatus)
		require.Equal(t, expectedExitStatus, status.ExitStatus())
	} else {
		t.Errorf("unable to retrieve the exit status for prometheus: %v", err)
	}
}

func TestWALSegmentSizeBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	for size, expectedExitStatus := range map[string]int{"9MB": 1, "257MB": 1, "10": 2, "1GB": 1, "12MB": 0} {
		prom := exec.Command(promPath, "-test.main", "--storage.wal.segment-size="+size, "--config.file="+promConfig)

		// Log stderr in case of failure.
		stderr, err := prom.StderrPipe()
		require.NoError(t, err)
		go func() {
			slurp, _ := ioutil.ReadAll(stderr)
			t.Log(string(slurp))
		}()

		err = prom.Start()
		require.NoError(t, err)

		if expectedExitStatus == 0 {
			done := make(chan error, 1)
			go func() { done <- prom.Wait() }()
			select {
			case err := <-done:
				t.Errorf("prometheus-agent should be still running: %v", err)
			case <-time.After(5 * time.Second):
				prom.Process.Kill()
			}
			continue
		}

		err = prom.Wait()
		require.Error(t, err)
		if exitError, ok := err.(*exec.ExitError); ok {
			status := exitError.Sys().(syscall.WaitStatus)
			require.Equal(t, expectedExitStatus, status.ExitStatus())
		} else {
			t.Errorf("unable to retrieve the exit status for prometheus: %v", err)
		}
	}
}
