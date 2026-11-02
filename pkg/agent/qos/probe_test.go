// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0
package qos

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"
)

func fixture() fstest.MapFS {
	files := fstest.MapFS{
		"etc/os-release":                   {Data: []byte("ID=hce\nVERSION_ID=\"2.0\"\n")},
		"proc/sys/kernel/osrelease":        {Data: []byte("test-baseline\n")},
		"sys/fs/cgroup/cgroup.controllers": {Data: []byte("cpu memory io")},
	}
	for _, name := range []string{"cpu.qos_level", "cpu.max", "cpu.weight", "memory.low", "memory.min", "memory.high", "net_cls.classid", "io.max", "io.weight"} {
		files["sys/fs/cgroup/"+name] = &fstest.MapFile{Data: []byte("unchanged"), Mode: 0444}
	}
	return files
}

func TestProbeAndAssessmentNeverCertifyEnforcement(t *testing.T) {
	host := fixture()
	report, err := Probe(context.Background(), host, DefaultPaths(), time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if report.OSID != "hce" || report.OSVersion != "2.0" || report.KernelRelease != "test-baseline" || report.CgroupMode != "v2-marker-detected" {
		t.Fatalf("identity=%+v", report)
	}
	policy := Policy{Name: "four-dimensions", Required: []Requirement{{CPU, "cpu.qos.hce"}, {Memory, "memory.protection.v2"}, {Network, "network.netcls.v1"}, {IO, "io.max.v2"}}}
	result, err := Assess(policy, report)
	if err != nil {
		t.Fatal(err)
	}
	if !result.InterfacesDetected || result.EnforcementReady || report.EnforcementVerified || len(result.Gaps) != 4 {
		t.Fatalf("false readiness: %+v", result)
	}
	for name, file := range host {
		if name != "etc/os-release" && name != "proc/sys/kernel/osrelease" && name != "sys/fs/cgroup/cgroup.controllers" && string(file.Data) != "unchanged" {
			t.Fatal("probe modified host", name)
		}
	}
}

func TestMissingAndInvalidPaths(t *testing.T) {
	report, err := Probe(context.Background(), fstest.MapFS{}, DefaultPaths(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if report.CgroupMode != "unknown" || len(report.Warnings) < 2 {
		t.Fatal("missing OS baseline not surfaced")
	}
	for _, m := range report.Mechanisms {
		if m.State != Missing {
			t.Fatalf("missing interface state %+v", m)
		}
	}
	p := DefaultPaths()
	p.CPU = "../outside"
	if _, err := Probe(context.Background(), fixture(), p, time.Now()); err == nil {
		t.Fatal("parent traversal accepted")
	}
	p.CPU = "/sys/fs/cgroup"
	if _, err := Probe(context.Background(), fixture(), p, time.Now()); err == nil {
		t.Fatal("absolute relative-path accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Probe(ctx, fixture(), DefaultPaths(), time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
}

type deniedFS struct{ fs.FS }

func (d deniedFS) Open(name string) (fs.File, error) {
	if name == "sys/fs/cgroup/cpu.qos_level" {
		return nil, fs.ErrPermission
	}
	return d.FS.Open(name)
}
func TestPermissionErrorsAreUnknown(t *testing.T) {
	report, err := Probe(context.Background(), deniedFS{fixture()}, DefaultPaths(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if report.Mechanisms[0].State != Unknown {
		t.Fatal("permission error became missing or supported")
	}
}

func TestCgroupV1AndPartialControllers(t *testing.T) {
	host := fstest.MapFS{}
	p := DefaultPaths()
	p.CPU = "sys/fs/cgroup/cpu/pod"
	p.Memory = "sys/fs/cgroup/memory/pod"
	p.IO = "sys/fs/cgroup/blkio/pod"
	p.Network = "sys/fs/cgroup/net_cls/pod"
	for _, name := range []string{p.CPU + "/cpu.cfs_period_us", p.CPU + "/cpu.cfs_quota_us", p.IO + "/blkio.throttle.read_bps_device"} {
		host[name] = &fstest.MapFile{}
	}
	report, err := Probe(context.Background(), host, p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if report.CgroupMode != "v1-cpu-marker-detected" {
		t.Fatal(report.CgroupMode)
	}
	for _, m := range report.Mechanisms {
		if m.ID == "cpu.quota.v1" && m.State != Detected {
			t.Fatal("v1 quota not detected")
		}
		if m.ID == "io.throttle.v1" && m.State != Missing {
			t.Fatal("partial IO controller incorrectly complete")
		}
	}
}

func TestInvalidPolicy(t *testing.T) {
	report, _ := Probe(context.Background(), fixture(), DefaultPaths(), time.Now())
	for _, p := range []Policy{
		{}, {Name: "empty"},
		{Name: "unknown", Required: []Requirement{{CPU, "invented"}}},
		{Name: "mismatch", Required: []Requirement{{CPU, "io.max.v2"}}},
		{Name: "duplicate", Required: []Requirement{{CPU, "cpu.qos.hce"}, {CPU, "cpu.qos.hce"}}},
	} {
		if _, err := Assess(p, report); err == nil {
			t.Fatalf("accepted invalid policy %+v", p)
		}
	}
	report.Mechanisms = append(report.Mechanisms, report.Mechanisms[0])
	if _, err := Assess(Policy{Name: "test", Required: []Requirement{{CPU, "cpu.qos.hce"}}}, report); err == nil {
		t.Fatal("duplicate report accepted")
	}
}
