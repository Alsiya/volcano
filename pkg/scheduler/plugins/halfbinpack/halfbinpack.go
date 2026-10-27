// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0

// Package halfbinpack enforces a cluster-configured CPU-only allocation ratio on NPU nodes.
package halfbinpack

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/mitchellh/mapstructure"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	PluginName = "halfbinpack"
	MixedLabel = "volcano.sh/npu-cpu-mixed"
	// WorkloadLabel is retained for existing manifests, but never grants a quota exemption.
	WorkloadLabel = "volcano.sh/workload-type"
)

type config struct {
	// CPURatio is a percentage of physical Allocatable CPU, shared by every NPU node.
	CPURatio     float64 `json:"cpuRatio"`
	NPUResources string  `json:"npuResources"`
	Weight       float64 `json:"weight"`
}

type plugin struct {
	cfg       config
	configErr error
	npu       []v1.ResourceName
}

// New rejects legacy load-threshold arguments: a load percentage is not an allocation quota.
func New(args framework.Arguments) framework.Plugin {
	p := &plugin{cfg: config{CPURatio: 30, NPUResources: "huawei.com/Ascend910,huawei.com/Ascend310", Weight: 1}}
	for key := range args {
		if key != "config" {
			p.configErr = fmt.Errorf("halfbinpack: unknown argument %q", key)
			return p
		}
	}
	if value, ok := args["config"]; ok {
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{Result: &p.cfg, TagName: "json", ErrorUnused: true})
		if err == nil {
			err = decoder.Decode(value)
		}
		if err != nil {
			p.configErr = fmt.Errorf("halfbinpack: invalid config: %w", err)
			return p
		}
	}
	for _, name := range strings.Split(p.cfg.NPUResources, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if !strings.Contains(name, "/") || len(validation.IsQualifiedName(name)) != 0 {
			p.configErr = fmt.Errorf("halfbinpack: invalid NPU resource %q", name)
			return p
		}
		p.npu = append(p.npu, v1.ResourceName(name))
	}
	if len(p.npu) == 0 || math.IsNaN(p.cfg.CPURatio) || math.IsInf(p.cfg.CPURatio, 0) || p.cfg.CPURatio < 0 || p.cfg.CPURatio > 100 || math.IsNaN(p.cfg.Weight) || math.IsInf(p.cfg.Weight, 0) || p.cfg.Weight < 0 {
		p.configErr = fmt.Errorf("halfbinpack: cpuRatio must be in [0,100], weight must be nonnegative, and npuResources must be set")
	}
	return p
}

func (*plugin) Name() string { return PluginName }

// requests includes init containers, restartable sidecars and Pod overhead using Volcano's calculator.
func requests(task *api.TaskInfo) *api.Resource {
	if task.Pod != nil {
		return api.GetPodResourceRequest(task.Pod)
	}
	if task.InitResreq != nil {
		return task.InitResreq
	}
	if task.Resreq != nil {
		return task.Resreq
	}
	return api.EmptyResource()
}

func (p *plugin) isCPU(task *api.TaskInfo) bool {
	r := requests(task)
	for _, name := range p.npu {
		if r.Get(name) > 0 {
			return false
		}
	}
	// Missing or misleading labels must not bypass the quota.
	return true
}

func (p *plugin) managed(node *api.NodeInfo) bool {
	if node.Node == nil {
		return false
	}
	if node.Node.Labels[MixedLabel] == "true" {
		return true
	}
	// Detect NPU nodes even if a label is absent, and regardless of available NPU count.
	for _, name := range p.npu {
		quantity := node.Node.Status.Capacity[name]
		if quantity.Sign() > 0 {
			return true
		}
	}
	return false
}

func (p *plugin) quota(node *api.NodeInfo) float64 {
	// Use physical Allocatable, not Capacity or synthetic oversubscription resources.
	return math.Floor(float64(node.Node.Status.Allocatable.Cpu().MilliValue()) * p.cfg.CPURatio / 100)
}

func active(task *api.TaskInfo) bool {
	return task.Status != api.Succeeded && task.Status != api.Failed
}

// projected scans the supplied node snapshot. Preemption modifies a cloned NodeInfo,
// so simulations, reprieves and rollbacks cannot mutate a separate global quota cache.
// future accounts for pipelined replacements; current retains terminating pods until removal.
func (p *plugin) projected(task *api.TaskInfo, node *api.NodeInfo, current bool) float64 {
	total := requests(task).MilliCPU
	for _, existing := range node.Tasks {
		if existing.UID == task.UID || !active(existing) || !p.isCPU(existing) {
			continue
		}
		if current && existing.Status == api.Pipelined {
			continue
		}
		if !current && existing.Status == api.Releasing {
			continue
		}
		total += requests(existing).MilliCPU
	}
	return total
}

func (p *plugin) check(task *api.TaskInfo, node *api.NodeInfo, current bool) error {
	if p.configErr != nil {
		return p.configErr
	}
	if !p.managed(node) {
		return nil
	}
	if !p.isCPU(task) {
		return nil
	}
	if requests(task).MilliCPU <= 0 {
		return fmt.Errorf("halfbinpack: CPU-only pod must declare positive CPU requests")
	}
	quota := p.quota(node)
	used := p.projected(task, node, current)
	if used > quota {
		return fmt.Errorf("halfbinpack: CPU-only requests %.0fm exceed quota %.0fm (%.2f%% of allocatable CPU)", used, quota, p.cfg.CPURatio)
	}
	return nil
}

func podPriority(task *api.TaskInfo) int32 {
	if task.Pod != nil && task.Pod.Spec.Priority != nil {
		return *task.Pod.Spec.Priority
	}
	return 0
}

// victims uses only Pod priority; NPU resources and Job priority do not grant special treatment.
func victims(preemptor *api.TaskInfo, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
	if preemptor == nil || (preemptor.Pod != nil && preemptor.Pod.Spec.PreemptionPolicy != nil && *preemptor.Pod.Spec.PreemptionPolicy == v1.PreemptNever) {
		return nil, util.Permit
	}
	result := make([]*api.TaskInfo, 0, len(candidates))
	for _, candidate := range candidates {
		if podPriority(candidate) < podPriority(preemptor) {
			result = append(result, candidate)
		}
	}
	return result, util.Permit
}

func (p *plugin) OnSessionOpen(ssn *framework.Session) {
	ssn.StrictPodPreemption = true
	if p.configErr != nil {
		klog.Error(p.configErr)
	}
	predicate := func(task *api.TaskInfo, node *api.NodeInfo) error {
		if err := p.check(task, node, false); err != nil {
			return api.NewFitErrWithStatus(task, node, &api.Status{Plugin: PluginName, Code: api.Unschedulable, Reason: err.Error()})
		}
		return nil
	}
	ssn.AddPredicateFn(p.Name(), predicate)
	ssn.AddSimulatePredicateFn(p.Name(), func(_ context.Context, _ fwk.CycleState, task *api.TaskInfo, node *api.NodeInfo) error {
		return predicate(task, node)
	})
	// No simulate add/remove handlers are needed: the framework updates the cloned node.Tasks.
	ssn.AddEventHandler(&framework.EventHandler{AllocateFunc: func(e *framework.Event) {
		node := ssn.Nodes[e.Task.NodeName]
		if node == nil {
			e.Err = fmt.Errorf("halfbinpack: allocated node %q missing", e.Task.NodeName)
			return
		}
		if err := p.check(e.Task, node, false); err != nil {
			e.Err = err
			return
		}
		if e.Task.Status != api.Pipelined {
			e.Err = p.check(e.Task, node, true)
		}
	}})
	ssn.AddNodeOrderFn(p.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		if !p.managed(node) || !p.isCPU(task) || p.check(task, node, false) != nil {
			return 0, nil
		}
		quota := p.quota(node)
		if quota <= 0 {
			return 0, nil
		}
		return 100 * p.cfg.Weight * p.projected(task, node, false) / quota, nil
	})
	ssn.AddTaskOrderFn(p.Name(), func(left, right interface{}) int {
		l, r := podPriority(left.(*api.TaskInfo)), podPriority(right.(*api.TaskInfo))
		if l > r {
			return -1
		}
		if l < r {
			return 1
		}
		return 0
	})
	ssn.AddPreemptableFn(p.Name(), victims)
	ssn.AddReclaimableFn(p.Name(), victims)
	ssn.AddUnifiedEvictableFn(p.Name(), func(ctx *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		// Gang eviction has no individual preemptor Pod, so fail closed rather than substitute Job priority.
		return victims(ctx.Task, candidates)
	})
}

func (*plugin) OnSessionClose(_ *framework.Session) {}
