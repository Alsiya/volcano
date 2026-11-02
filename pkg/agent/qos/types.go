// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0

// Package qos defines independent node QoS assessment and adapter contracts.
// Interface discovery is read-only and never certifies runtime isolation.
package qos

import (
	"context"
	"encoding/json"
	"fmt"
)

type Dimension string

const (
	CPU     Dimension = "cpu"
	Memory  Dimension = "memory"
	Network Dimension = "network"
	IO      Dimension = "io"
)

type InterfaceState string

const (
	Detected InterfaceState = "detected"
	Missing  InterfaceState = "missing"
	Unknown  InterfaceState = "unknown"
)

type Mechanism struct {
	ID        string         `json:"id"`
	Dimension Dimension      `json:"dimension"`
	State     InterfaceState `json:"interfaceState"`
	Files     []string       `json:"files"`
	// ExistingHandler is a source-code reference, not evidence of an enabled working adapter.
	ExistingHandler string `json:"existingHandler,omitempty"`
	Reason          string `json:"reason"`
}

type Report struct {
	ObservedAt    string      `json:"observedAt"`
	OSID          string      `json:"osID,omitempty"`
	OSVersion     string      `json:"osVersion,omitempty"`
	KernelRelease string      `json:"kernelRelease,omitempty"`
	CgroupMode    string      `json:"cgroupMode"`
	Mechanisms    []Mechanism `json:"mechanisms"`
	Warnings      []string    `json:"warnings"`
	// This probe does not apply writes, verify a baseline or run interference tests.
	EnforcementVerified bool `json:"enforcementVerified"`
}

// Requirement selects a specific mechanism, not just a dimension. Weight, quota,
// reclaim protection and bandwidth caps are different semantics and not interchangeable.
type Requirement struct {
	Dimension Dimension `json:"dimension"`
	Mechanism string    `json:"mechanism"`
}

type Policy struct {
	Name     string        `json:"name"`
	Required []Requirement `json:"required"`
}

type Gap struct {
	Dimension Dimension `json:"dimension"`
	Mechanism string    `json:"mechanism"`
	Reason    string    `json:"reason"`
}

type Assessment struct {
	Policy             string `json:"policy"`
	InterfacesDetected bool   `json:"interfacesDetected"`
	EnforcementReady   bool   `json:"enforcementReady"`
	Gaps               []Gap  `json:"gaps"`
}

// Assess never promotes read-only discovery into an enforcement readiness claim.
func Assess(policy Policy, report Report) (Assessment, error) {
	result := Assessment{Policy: policy.Name, InterfacesDetected: true, Gaps: []Gap{}}
	if policy.Name == "" || len(policy.Required) == 0 {
		return result, fmt.Errorf("policy name and at least one requirement are required")
	}
	known := make(map[string]Mechanism, len(report.Mechanisms))
	for _, m := range report.Mechanisms {
		if _, ok := known[m.ID]; ok {
			return result, fmt.Errorf("duplicate report mechanism %q", m.ID)
		}
		known[m.ID] = m
	}
	seen := map[string]bool{}
	for _, r := range policy.Required {
		if r.Dimension != CPU && r.Dimension != Memory && r.Dimension != Network && r.Dimension != IO {
			return result, fmt.Errorf("invalid dimension %q", r.Dimension)
		}
		if seen[r.Mechanism] {
			return result, fmt.Errorf("duplicate requirement %q", r.Mechanism)
		}
		seen[r.Mechanism] = true
		m, ok := known[r.Mechanism]
		if !ok || m.Dimension != r.Dimension {
			return result, fmt.Errorf("unknown or mismatched mechanism %q for %s", r.Mechanism, r.Dimension)
		}
		if m.State != Detected {
			result.InterfacesDetected = false
			result.Gaps = append(result.Gaps, Gap{r.Dimension, r.Mechanism, "interface not confirmed: " + m.Reason})
		}
		result.Gaps = append(result.Gaps, Gap{r.Dimension, r.Mechanism, "adapter integration, baseline qualification, target permissions and interference validation are not certified by this probe"})
	}
	return result, nil
}

// Target must be resolved from the live runtime by the hosting agent. Implementations
// must validate Pod UID, cgroup ownership and non-root scope before preparing writes.
type Target struct {
	NodeName string
	PodUID   string
	Cgroups  map[Dimension]string
}

type Intent struct {
	Requirement Requirement
	// Adapter-specific validated parameters; there are deliberately no production defaults.
	Parameters json.RawMessage
}

// Adapter is an integration seam. No built-in writer or automatic handler registration
// is provided until the HCE baseline and operational policy are qualified.
type Adapter interface {
	Name() string
	Prepare(context.Context, Target, Intent) (PreparedChange, error)
}

// PreparedChange must capture the previous owned state and reject conflicts with
// kubelet/CNI/other agents. Applied and verified are distinct lifecycle stages.
// A host must prepare every required dimension before applying any of them, restore
// applied changes in reverse order on error, and surface restore failures explicitly.
type PreparedChange interface {
	Apply(context.Context) error
	Verify(context.Context) error
	Restore(context.Context) error
}
