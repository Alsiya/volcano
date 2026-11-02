// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0

// agent-qos-probe is a read-only node QoS assessment tool. There is no apply mode.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"volcano.sh/volcano/pkg/agent/qos"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("agent-qos-probe", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("host-root", "/", "filesystem root, e.g. /host for a read-only mounted host")
	paths := qos.DefaultPaths()
	flags.StringVar(&paths.Root, "cgroup-root", paths.Root, "cgroup hierarchy path relative to host-root")
	cpu := flags.String("cpu-cgroup", "", "existing CPU Pod cgroup path relative to host-root; default: cgroup-root")
	memory := flags.String("memory-cgroup", "", "existing memory Pod cgroup path relative to host-root; default: cgroup-root")
	network := flags.String("network-cgroup", "", "existing net_cls Pod cgroup path relative to host-root; default: cgroup-root")
	disk := flags.String("io-cgroup", "", "existing IO Pod cgroup path relative to host-root; default: cgroup-root")
	policyPath := flags.String("policy", "", "optional assessment-only JSON policy file")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if !filepath.IsAbs(*root) {
		return fmt.Errorf("host-root must be absolute")
	}
	choose := func(v string) string {
		if v != "" {
			return v
		}
		return paths.Root
	}
	paths.CPU, paths.Memory, paths.Network, paths.IO = choose(*cpu), choose(*memory), choose(*network), choose(*disk)
	report, err := qos.Probe(context.Background(), os.DirFS(*root), paths, time.Now())
	if err != nil {
		return err
	}
	result := struct {
		Report     qos.Report      `json:"report"`
		Assessment *qos.Assessment `json:"assessment,omitempty"`
	}{Report: report}
	if *policyPath != "" {
		file, err := os.Open(*policyPath)
		if err != nil {
			return err
		}
		defer file.Close()
		data, err := io.ReadAll(io.LimitReader(file, 1024*1024+1))
		if err != nil {
			return err
		}
		if len(data) > 1024*1024 {
			return fmt.Errorf("policy exceeds 1 MiB")
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		var policy qos.Policy
		if err := decoder.Decode(&policy); err != nil {
			return fmt.Errorf("invalid policy: %w", err)
		}
		var extra interface{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return fmt.Errorf("policy must contain exactly one JSON object")
		}
		assessment, err := qos.Assess(policy, report)
		if err != nil {
			return err
		}
		result.Assessment = &assessment
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
