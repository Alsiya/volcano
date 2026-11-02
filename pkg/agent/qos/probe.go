// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0
package qos

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"time"
)

// Paths are relative to the supplied filesystem root. For real assessment use an
// existing delegated Pod cgroup; controller files are often absent on root cgroups.
type Paths struct {
	Root          string
	CPU           string
	Memory        string
	Network       string
	IO            string
	OSRelease     string
	KernelRelease string
}

func DefaultPaths() Paths {
	return Paths{Root: "sys/fs/cgroup", CPU: "sys/fs/cgroup", Memory: "sys/fs/cgroup", Network: "sys/fs/cgroup", IO: "sys/fs/cgroup", OSRelease: "etc/os-release", KernelRelease: "proc/sys/kernel/osrelease"}
}

func validatePaths(p Paths) error {
	for name, v := range map[string]string{"root": p.Root, "cpu": p.CPU, "memory": p.Memory, "network": p.Network, "io": p.IO, "osRelease": p.OSRelease, "kernelRelease": p.KernelRelease} {
		if !fs.ValidPath(v) {
			return fmt.Errorf("%s path must be non-empty and relative without parent traversal: %q", name, v)
		}
	}
	return nil
}

type definition struct {
	id        string
	dimension Dimension
	directory string
	files     []string
	handler   string
}

// Probe only reads metadata and OS identity. It does not create test cgroups, write
// controller files, load modules, change sysctls, or run network/disk pressure tests.
func Probe(ctx context.Context, host fs.FS, p Paths, now time.Time) (Report, error) {
	report := Report{ObservedAt: now.UTC().Format(time.RFC3339), CgroupMode: "unknown", Mechanisms: []Mechanism{}, Warnings: []string{}}
	if host == nil {
		return report, fmt.Errorf("nil host filesystem")
	}
	if err := validatePaths(p); err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if contents, err := fs.ReadFile(host, p.OSRelease); err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(contents)))
		for scanner.Scan() {
			key, value, ok := strings.Cut(scanner.Text(), "=")
			if !ok {
				continue
			}
			value = strings.Trim(strings.TrimSpace(value), "\"'")
			switch key {
			case "ID":
				report.OSID = value
			case "VERSION_ID":
				report.OSVersion = value
			}
		}
		if err := scanner.Err(); err != nil {
			report.Warnings = append(report.Warnings, "os-release: "+err.Error())
		}
	} else {
		report.Warnings = append(report.Warnings, "os-release: "+err.Error())
	}
	if contents, err := fs.ReadFile(host, p.KernelRelease); err == nil {
		report.KernelRelease = strings.TrimSpace(string(contents))
	} else {
		report.Warnings = append(report.Warnings, "kernel release: "+err.Error())
	}
	if info, err := fs.Stat(host, path.Join(p.Root, "cgroup.controllers")); err == nil && !info.IsDir() {
		report.CgroupMode = "v2-marker-detected"
	} else if info, err := fs.Stat(host, path.Join(p.CPU, "cpu.cfs_period_us")); err == nil && !info.IsDir() {
		report.CgroupMode = "v1-cpu-marker-detected"
	}
	definitions := []definition{
		{"cpu.qos.hce", CPU, p.CPU, []string{"cpu.qos_level"}, "pkg/agent/events/handlers/cpuqos"},
		{"cpu.weight.v2", CPU, p.CPU, []string{"cpu.weight"}, ""},
		{"cpu.quota.v2", CPU, p.CPU, []string{"cpu.max"}, ""},
		{"cpu.shares.v1", CPU, p.CPU, []string{"cpu.shares"}, ""},
		{"cpu.quota.v1", CPU, p.CPU, []string{"cpu.cfs_quota_us", "cpu.cfs_period_us"}, ""},
		{"memory.qos.hce", Memory, p.Memory, []string{"memory.qos_level"}, "pkg/agent/events/handlers/memoryqos"},
		{"memory.protection.v2", Memory, p.Memory, []string{"memory.low", "memory.min"}, "pkg/agent/events/handlers/memoryqosv2"},
		{"memory.throttle.v2", Memory, p.Memory, []string{"memory.high"}, "pkg/agent/events/handlers/memoryqosv2"},
		{"network.netcls.v1", Network, p.Network, []string{"net_cls.classid"}, "pkg/agent/events/handlers/networkqos"},
		{"io.max.v2", IO, p.IO, []string{"io.max"}, ""},
		{"io.weight.v2", IO, p.IO, []string{"io.weight"}, ""},
		{"io.throttle.v1", IO, p.IO, []string{"blkio.throttle.read_bps_device", "blkio.throttle.write_bps_device"}, ""},
	}
	for _, d := range definitions {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		m := Mechanism{ID: d.id, Dimension: d.dimension, State: Detected, Files: []string{}, ExistingHandler: d.handler, Reason: "interface metadata detected at the supplied paths; semantics, write permission and effectiveness unverified"}
		var issues []string
		for _, file := range d.files {
			name := path.Join(d.directory, file)
			m.Files = append(m.Files, name)
			info, err := fs.Stat(host, name)
			if err != nil {
				if !fs.ValidPath(name) {
					return report, fmt.Errorf("invalid probe path %q", name)
				}
				if !errors.Is(err, fs.ErrNotExist) {
					m.State = Unknown
				} else if m.State != Unknown {
					m.State = Missing
				}
				issues = append(issues, name+": "+err.Error())
			} else if !info.Mode().IsRegular() {
				m.State = Unknown
				issues = append(issues, name+": not a regular controller file")
			}
		}
		if len(issues) > 0 {
			m.Reason = strings.Join(issues, "; ")
		}
		report.Mechanisms = append(report.Mechanisms, m)
	}
	report.Warnings = append(report.Warnings, "Read-only discovery is not HCE OS certification or runtime QoS verification. Missing files may mean an incorrect or non-delegated target path, not an unsupported kernel.")
	return report, nil
}
