// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0

// Package halfbinpack packs opted-in CPU pods onto mixed NPU nodes using real CPU load.
package halfbinpack

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	PluginName    = "halfbinpack"
	MixedLabel    = "volcano.sh/npu-cpu-mixed"
	WorkloadLabel = "volcano.sh/workload-type"
)

type config struct {
	Address        string  `json:"prometheusAddress"`
	CPUQuery       string  `json:"cpuQuery"`
	TimestampQuery string  `json:"timestampQuery"`
	NPUResources   string  `json:"npuResources"`
	Lower          float64 `json:"lowerThreshold"`
	Upper          float64 `json:"upperThreshold"`
	MaxAgeSeconds  int     `json:"maxAgeSeconds"`
	BlindSeconds   int     `json:"blindSeconds"`
	MinPodMilliCPU float64 `json:"minPodMilliCPU"`
	Weight         float64 `json:"weight"`
}

type sample struct {
	percent  float64
	observed time.Time
}

type reservation struct {
	node string
	cpu  float64
}

type plugin struct {
	cfg       config
	configErr error
	npu       []v1.ResourceName
	mu        sync.RWMutex
	metrics   map[string]sample
	reserved  map[api.TaskID]reservation
}

// New constructs the plugin. Invalid configuration rejects CPU mixing on labeled nodes.
func New(args framework.Arguments) framework.Plugin {
	cfg := config{
		CPUQuery:       `100 * (1 - avg by (node) (rate(node_cpu_seconds_total{mode="idle"}[1m])))`,
		TimestampQuery: `min by (node) (timestamp(node_cpu_seconds_total{mode="idle"}))`,
		NPUResources:   "huawei.com/Ascend910,huawei.com/Ascend310",
		Lower:          40, Upper: 70, MaxAgeSeconds: 60, BlindSeconds: 120,
		MinPodMilliCPU: 250, Weight: 1,
	}
	// Decode into defaults; configuration errors disable CPU mixing on labeled nodes.
	if value, ok := args["config"]; ok {
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{Result: &cfg, TagName: "json", ErrorUnused: true})
		if err == nil {
			err = decoder.Decode(value)
		}
		if err != nil {
			return &plugin{configErr: fmt.Errorf("halfbinpack: invalid config: %w", err)}
		}
	}
	p := &plugin{cfg: cfg}
	for _, resource := range strings.Split(cfg.NPUResources, ",") {
		if name := strings.TrimSpace(resource); name != "" {
			p.npu = append(p.npu, v1.ResourceName(name))
		}
	}
	if cfg.Address == "" || cfg.CPUQuery == "" || cfg.TimestampQuery == "" || len(p.npu) == 0 ||
		!finite(cfg.Lower) || !finite(cfg.Upper) || cfg.Lower < 0 || cfg.Lower >= cfg.Upper || cfg.Upper > 100 ||
		cfg.MaxAgeSeconds <= 0 || cfg.BlindSeconds < cfg.MaxAgeSeconds ||
		!finite(cfg.MinPodMilliCPU) || cfg.MinPodMilliCPU <= 0 || !finite(cfg.Weight) || cfg.Weight < 0 {
		p.configErr = fmt.Errorf("halfbinpack: invalid thresholds, metrics settings, resource names or estimator")
	}
	return p
}

func (*plugin) Name() string { return PluginName }

func (p *plugin) isCPU(task *api.TaskInfo) bool {
	if task.Pod == nil || task.Pod.Labels[WorkloadLabel] != "cpu" {
		return false
	}
	for _, name := range p.npu {
		if task.Resreq.Get(name) > 0 {
			return false
		}
	}
	return true
}

func mixed(node *api.NodeInfo) bool {
	return node.Node != nil && node.Node.Labels[MixedLabel] == "true"
}

func (p *plugin) hasNPU(node *api.NodeInfo) bool {
	for _, name := range p.npu {
		quantity := node.Node.Status.Capacity[name]
		if quantity.Sign() > 0 {
			return true
		}
	}
	return false
}

func (p *plugin) estimate(task *api.TaskInfo) float64 {
	return math.Max(task.Resreq.MilliCPU, p.cfg.MinPodMilliCPU)
}

func (p *plugin) reserve(task *api.TaskInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reserved[task.UID] = reservation{node: task.NodeName, cpu: p.estimate(task)}
}

func (p *plugin) unreserve(task *api.TaskInfo) {
	// Eviction is asynchronous; a terminating pod may still consume CPU.
	if task.Status == api.Releasing {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.reserved, task.UID)
}

func (p *plugin) OnSessionOpen(ssn *framework.Session) {
	p.metrics = make(map[string]sample)
	p.reserved = make(map[api.TaskID]reservation)
	if p.configErr == nil {
		// One batch snapshot per session; never perform network I/O in node predicates.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		metrics, err := p.fetch(ctx, time.Now())
		cancel()
		if err != nil {
			klog.Errorf("halfbinpack metrics unavailable: %v", err)
		} else {
			p.metrics = metrics
		}
	} else {
		klog.Error(p.configErr)
	}
	// Rebuild across sessions and scheduler restarts. Include NPU jobs too: their new CPU
	// consumption must reduce the headroom available to CPU jobs.
	for _, node := range ssn.Nodes {
		if !mixed(node) {
			continue
		}
		for _, task := range node.Tasks {
			if p.unobserved(task, p.metrics[node.Name]) {
				p.reserve(task)
			}
		}
	}
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(e *framework.Event) {
			// Recheck at allocation: scoring and filtering alone cannot reject a
			// candidate whose snapshot expires just before it is allocated.
			if node := ssn.Nodes[e.Task.NodeName]; node != nil && mixed(node) && p.isCPU(e.Task) {
				if _, err := p.projected(e.Task, node, time.Now()); err != nil {
					e.Err = err
					return
				}
			}
			p.reserve(e.Task)
		},
		DeallocateFunc: func(e *framework.Event) { p.unreserve(e.Task) },
	})
	ssn.AddPredicateFn(p.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		if !mixed(node) || !p.isCPU(task) {
			return nil
		}
		_, err := p.projected(task, node, time.Now())
		if err == nil {
			return nil
		}
		return api.NewFitErrWithStatus(task, node, &api.Status{
			Plugin: PluginName, Code: api.Unschedulable, Reason: err.Error(),
		})
	})
	ssn.AddNodeOrderFn(p.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		if !mixed(node) || !p.isCPU(task) {
			return 0, nil
		}
		load, err := p.projected(task, node, time.Now())
		if err != nil {
			return 0, nil
		}
		return p.score(load), nil
	})
}

func (p *plugin) unobserved(task *api.TaskInfo, metric sample) bool {
	switch task.Status {
	case api.Allocated, api.Binding, api.Bound, api.Pipelined:
		return true
	case api.Running, api.Releasing:
		return task.Pod == nil || task.Pod.Status.StartTime == nil ||
			metric.observed.Before(task.Pod.Status.StartTime.Add(time.Duration(p.cfg.BlindSeconds)*time.Second))
	}
	return false
}

func (p *plugin) projected(task *api.TaskInfo, node *api.NodeInfo, now time.Time) (float64, error) {
	if p.configErr != nil {
		return 0, p.configErr
	}
	if !p.hasNPU(node) {
		return 0, fmt.Errorf("halfbinpack: mixed node has no configured NPU resource")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	m, exists := p.metrics[node.Name]
	if !exists || m.observed.IsZero() || now.Sub(m.observed) > time.Duration(p.cfg.MaxAgeSeconds)*time.Second || m.observed.After(now) {
		return 0, fmt.Errorf("halfbinpack: missing, stale or future CPU metrics")
	}
	// The Prometheus percentage is normalized by physical CPU capacity, not allocatable.
	capacity := node.Node.Status.Capacity.Cpu().MilliValue()
	if capacity <= 0 {
		return 0, fmt.Errorf("halfbinpack: CPU capacity is zero")
	}
	estimated := p.estimate(task)
	for uid, r := range p.reserved {
		if r.node == node.Name && uid != task.UID {
			estimated += r.cpu
		}
	}
	load := m.percent + 100*estimated/float64(capacity)
	if load > p.cfg.Upper {
		return load, fmt.Errorf("halfbinpack: predicted CPU %.2f%% exceeds %.2f%%", load, p.cfg.Upper)
	}
	return load, nil
}

func (p *plugin) OnSessionClose(_ *framework.Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metrics = nil
	p.reserved = nil
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func (p *plugin) score(load float64) float64 {
	// The lower threshold is a scoring target, never an admission floor.
	if load < p.cfg.Lower && p.cfg.Lower > 0 {
		return 50 * p.cfg.Weight * load / p.cfg.Lower
	}
	return p.cfg.Weight * (50 + 50*(load-p.cfg.Lower)/(p.cfg.Upper-p.cfg.Lower))
}

// fetch uses a companion raw-sample timestamp query. Instant expression evaluation
// timestamps alone cannot detect an exporter that stopped reporting.
func (p *plugin) fetch(ctx context.Context, now time.Time) (map[string]sample, error) {
	client, err := promapi.NewClient(promapi.Config{Address: p.cfg.Address})
	if err != nil {
		return nil, err
	}
	query := promv1.NewAPI(client)
	read := func(expression string) (map[string]float64, error) {
		value, warnings, err := query.Query(ctx, expression, now)
		if err != nil {
			return nil, err
		}
		if len(warnings) > 0 {
			return nil, fmt.Errorf("partial metrics: %v", warnings)
		}
		vector, ok := value.(model.Vector)
		if !ok {
			return nil, fmt.Errorf("expected an instant vector")
		}
		result := make(map[string]float64)
		for _, point := range vector {
			name := string(point.Metric["node"])
			v := float64(point.Value)
			if name == "" || !finite(v) {
				return nil, fmt.Errorf("invalid metric or missing node label")
			}
			if _, duplicate := result[name]; duplicate {
				return nil, fmt.Errorf("duplicate metric for node %s", name)
			}
			result[name] = v
		}
		return result, nil
	}
	cpu, err := read(p.cfg.CPUQuery)
	if err != nil {
		return nil, err
	}
	timestamps, err := read(p.cfg.TimestampQuery)
	if err != nil {
		return nil, err
	}
	result := make(map[string]sample)
	for name, percent := range cpu {
		ts, ok := timestamps[name]
		if !ok || percent < 0 || percent > 100 || ts <= 0 || ts > float64(now.Unix())+1 {
			continue
		}
		result[name] = sample{percent: percent, observed: time.Unix(int64(ts), 0)}
	}
	return result, nil
}
