// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"volcano.sh/volcano/pkg/agent/qos"
)

func TestReadOnlyCLI(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{"etc/os-release": "ID=hce\nVERSION_ID=2.0\n", "proc/sys/kernel/osrelease": "test\n", "sys/fs/cgroup/cgroup.controllers": "cpu memory io", "sys/fs/cgroup/pod/cpu.qos_level": "2", "sys/fs/cgroup/pod/io.max": "8:0 rbps=max"}
	for name, value := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(value), 0444); err != nil {
			t.Fatal(err)
		}
	}
	policy := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(policy, []byte(`{"name":"cpu","required":[{"dimension":"cpu","mechanism":"cpu.qos.hce"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if err := run([]string{"--host-root", root, "--cpu-cgroup", "sys/fs/cgroup/pod", "--policy", policy}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Report     qos.Report
		Assessment qos.Assessment
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Assessment.InterfacesDetected || result.Assessment.EnforcementReady || result.Report.EnforcementVerified {
		t.Fatal("CLI claims enforcement")
	}
	for name, value := range files {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(data) != value {
			t.Fatal("probe modified host fixture", name, err)
		}
	}
	if err := run([]string{"--apply"}, &out, &stderr); err == nil {
		t.Fatal("apply mode accepted")
	}
}

func TestCLIValidation(t *testing.T) {
	for _, content := range []string{`{"name":"p","required":[],"enforce":true}`, `{} {}`} {
		file := filepath.Join(t.TempDir(), "policy.json")
		if err := os.WriteFile(file, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		var out, stderr bytes.Buffer
		if err := run([]string{"--host-root", t.TempDir(), "--policy", file}, &out, &stderr); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}
