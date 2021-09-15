// Copyright 2017 The Prometheus Authors
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
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var promAgentPath = os.Args[0]
var promAgentConfig = filepath.Join("..", "..", "documentation", "examples", "prometheus.yml")

// Test for invalid configuration file and verify the exit status indicates the error.
func TestInvalidConfig(t *testing.T) {
	fakeInputFile := "fake-input-file"
	expectedExitStatus := 2

	prom := exec.Command(promAgentPath, "-test.main", "--agent", "--config.file="+fakeInputFile)
	err := prom.Run()
	require.Error(t, err)

	if exitError, ok := err.(*exec.ExitError); ok {
		status := exitError.Sys().(syscall.WaitStatus)
		require.Equal(t, expectedExitStatus, status.ExitStatus())
	} else {
		t.Errorf("unable to retrieve the exit status for prometheus: %v", err)
	}
}

// Test for invalid cmdLine config and verify the exit status indicates the error.
func TestInvalidCmdLine(t *testing.T) {
	prom := exec.Command(promPath, "-test.main", "--aagent", "--config.file="+promAgentConfig)
	err := prom.Start()
	require.NoError(t, err)

	expectedExitStatus := 1
	actualExitStatus := 0

	done := make(chan error, 1)
	go func() { done <- prom.Wait() }()
	select {
	case err := <-done:
		t.Logf("prometheus should be still running: %v", err)
		actualExitStatus = prom.ProcessState.ExitCode()
	case <-time.After(5 * time.Second):
		prom.Process.Kill()
	}

	require.Equal(t, expectedExitStatus, actualExitStatus)
}

// Test for successful startup.
func TestSuccesfulStartup(t *testing.T) {
	prom := exec.Command(promPath, "-test.main", "--agent", "--config.file="+promAgentConfig)
	err := prom.Start()
	require.NoError(t, err)

	expectedExitStatus := 0
	actualExitStatus := 0

	done := make(chan error, 1)
	go func() { done <- prom.Wait() }()
	select {
	case err := <-done:
		t.Logf("prometheus should be still running: %v", err)
		actualExitStatus = prom.ProcessState.ExitCode()
	case <-time.After(5 * time.Second):
		prom.Process.Kill()
	}
	require.Equal(t, expectedExitStatus, actualExitStatus)
}
