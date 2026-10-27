// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0
package halfbinpack

import (
	"context"
	"math"
	"os"
	"testing"

	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	schedframework "k8s.io/kubernetes/pkg/scheduler/framework"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/preempt"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	testutil "volcano.sh/volcano/pkg/scheduler/util"
)

func task(name, cpu, npu string, priority int32) *api.TaskInfo {
	r := v1.ResourceList{v1.ResourceCPU: resource.MustParse(cpu)}
	if npu != "" {
		r["huawei.com/Ascend910"] = resource.MustParse(npu)
	}
	pod := testutil.BuildPod("test", name, "", v1.PodPending, r, "pg", nil, nil)
	pod.Spec.Priority = &priority
	return api.NewTaskInfo(pod)
}
func node() *api.NodeInfo {
	n := testutil.BuildNode("npu", v1.ResourceList{v1.ResourceCPU: resource.MustParse("10"), v1.ResourceMemory: resource.MustParse("32Gi"), v1.ResourcePods: resource.MustParse("100"), "huawei.com/Ascend910": resource.MustParse("8")}, nil)
	// Ratio is against Allocatable 10 cores, not Capacity 20 cores.
	n.Status.Capacity = n.Status.Allocatable.DeepCopy()
	n.Status.Capacity[v1.ResourceCPU] = resource.MustParse("20")
	return api.NewNodeInfo(n)
}
func add(t *testing.T, n *api.NodeInfo, pod *api.TaskInfo, status api.TaskStatus) {
	t.Helper()
	pod.Status = status
	pod.NodeName = n.Name
	if err := n.AddTask(pod); err != nil {
		t.Fatal(err)
	}
}
func newPlugin(ratio float64) *plugin {
	return New(framework.Arguments{"config": map[string]interface{}{"cpuRatio": ratio}}).(*plugin)
}

func TestRatioIndependentOfNPUAllocationAndMetrics(t *testing.T) {
	for _, npu := range []string{"0", "8"} {
		t.Run(npu, func(t *testing.T) {
			p, n := newPlugin(30), node()
			add(t, n, task("existing", "2", "", 1), api.Running)
			if npu != "0" {
				add(t, n, task("npu-job", "1", npu, 100), api.Running)
			}
			if err := p.check(task("exact", "1", "", 1), n, false); err != nil {
				t.Fatal(err)
			}
			if err := p.check(task("over", "1001m", "", 1), n, false); err == nil {
				t.Fatal("CPU quota bypassed")
			}
			if err := p.check(task("npu-incoming", "4", "1", 100), n, false); err != nil {
				t.Fatal("NPU job incorrectly charged to CPU quota", err)
			}
			// Already full NPU resources must not disable the CPU ratio filter.
			add(t, n, task("stable", "2", "", 1), api.Running)
			if err := p.check(task("new", "1m", "", 1), n, false); err == nil {
				t.Fatal("stable running CPU requests omitted")
			}
		})
	}
}

func TestClassificationAndZeroRequests(t *testing.T) {
	p, n := newPlugin(30), node()
	for _, label := range []string{"", "cpu", "npu"} {
		incoming := task("candidate", "4", "", 1)
		incoming.Pod.Labels = map[string]string{WorkloadLabel: label}
		if err := p.check(incoming, n, false); err == nil {
			t.Fatalf("label %q bypassed quota", label)
		}
	}
	if err := p.check(task("zero", "0", "", 1), n, false); err == nil {
		t.Fatal("zero request admitted")
	}
	if !p.managed(n) {
		t.Fatal("unlabeled NPU node not detected")
	}
	n.Node.Status.Capacity = v1.ResourceList{v1.ResourceCPU: resource.MustParse("20")}
	if p.managed(n) {
		t.Fatal("ordinary node classified as NPU")
	}
	if err := p.check(task("ordinary", "4", "", 1), n, false); err != nil {
		t.Fatal(err)
	}
	n.Node.Labels = map[string]string{MixedLabel: "true"}
	if !p.managed(n) {
		t.Fatal("explicit mixed label not honored")
	}
}

func TestInitAndOverheadAccounting(t *testing.T) {
	p, n := newPlugin(30), node()
	incoming := task("init", "1", "", 1)
	incoming.Pod.Spec.InitContainers = []v1.Container{{Name: "init", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2900m")}}}}
	incoming.Pod.Spec.Overhead = v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m")}
	if err := p.check(incoming, n, false); err == nil {
		t.Fatal("init CPU and overhead were not charged")
	}
}

func TestProjectedReleasePipelineRollbackAndRestart(t *testing.T) {
	p, n := newPlugin(30), node()
	old := task("old", "3", "", 1)
	add(t, n, old, api.Releasing)
	incoming := task("replace", "3", "", 10)
	if err := p.check(incoming, n, false); err != nil {
		t.Fatal("future replacement rejected", err)
	}
	if err := p.check(incoming, n, true); err == nil {
		t.Fatal("terminating pod freed actual quota early")
	}
	add(t, n, incoming, api.Pipelined)
	if got := p.projected(incoming, n, false); got != 3000 {
		t.Fatalf("double counted candidate: %v", got)
	}
	if err := p.check(task("other", "1m", "", 1), n, false); err == nil {
		t.Fatal("pipelined reservation ignored")
	}
	n.RemoveTask(incoming)
	if err := p.check(task("other", "3", "", 1), n, false); err != nil {
		t.Fatal("rollback leaked quota", err)
	}
	n.RemoveTask(old)
	add(t, n, old, api.Running)
	restarted := newPlugin(30)
	if err := restarted.check(task("after-restart", "1m", "", 1), n, false); err == nil {
		t.Fatal("restarted plugin lost running CPU accounting")
	}
}

func TestQuotaBoundaries(t *testing.T) {
	for _, tc := range []struct {
		ratio   float64
		cpu     string
		allowed bool
	}{{0, "1m", false}, {100, "10", true}, {100, "10001m", false}, {12.5, "1250m", true}, {12.5, "1251m", false}} {
		p := newPlugin(tc.ratio)
		if got := p.check(task("pod", tc.cpu, "", 1), node(), false) == nil; got != tc.allowed {
			t.Fatalf("%+v allowed=%v", tc, got)
		}
	}
	n := node()
	n.Node.Status.Allocatable[v1.ResourceCPU] = resource.MustParse("1001m")
	if got := newPlugin(30).quota(n); got != 300 {
		t.Fatalf("quota not rounded down to milliCPU: %v", got)
	}
}

func TestConfiguration(t *testing.T) {
	for _, cfg := range []map[string]interface{}{{"cpuRatio": -1}, {"cpuRatio": 101}, {"cpuRatio": math.NaN()}, {"weight": -1}, {"npuResources": "cpu"}, {"npuResources": ""}, {"upperThreshold": 70}, {"prometheusAddress": "http://old"}} {
		if New(framework.Arguments{"config": cfg}).(*plugin).configErr == nil {
			t.Fatalf("invalid config accepted: %v", cfg)
		}
	}
	if newPlugin(30).configErr != nil {
		t.Fatal("valid config rejected")
	}
}

func TestStrictPodPriorityAcrossWorkloads(t *testing.T) {
	incoming := task("incoming", "1", "", 10)
	lowCPU := task("low-cpu", "1", "", 1)
	lowNPU := task("low-npu", "1", "1", 1)
	equal := task("equal", "1", "1", 10)
	higher := task("higher", "1", "", 20)
	result, _ := victims(incoming, []*api.TaskInfo{lowCPU, lowNPU, equal, higher})
	if len(result) != 2 || result[0].UID != lowCPU.UID || result[1].UID != lowNPU.UID {
		t.Fatal("victim selection did not obey Pod priority", result)
	}
	incoming = task("incoming-npu", "1", "1", 10)
	result, _ = victims(incoming, []*api.TaskInfo{lowCPU, lowNPU, equal, higher})
	if len(result) != 2 {
		t.Fatal("NPU identity changed priority filtering")
	}
	never := v1.PreemptNever
	incoming.Pod.Spec.PreemptionPolicy = &never
	if result, _ := victims(incoming, []*api.TaskInfo{lowCPU}); len(result) != 0 {
		t.Fatal("PreemptNever ignored")
	}
}

func session(t *testing.T, pods ...*v1.Pod) (*framework.Session, *uthelper.TestCommonStruct) {
	t.Helper()
	enabled := true
	helper := &uthelper.TestCommonStruct{Plugins: map[string]framework.PluginBuilder{PluginName: New}, Nodes: []*v1.Node{node().Node}, Pods: pods, Queues: []*scheduling.Queue{testutil.BuildQueue("q", 1, nil)}, PodGroups: []*scheduling.PodGroup{testutil.BuildPodGroup("pg", "test", "q", 1, nil, scheduling.PodGroupInqueue)}}
	ssn := helper.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName, EnabledPredicate: &enabled, EnabledNodeOrder: &enabled, EnabledPreemptable: &enabled, EnabledReclaimable: &enabled, EnabledTaskOrder: &enabled}}}}, nil)
	t.Cleanup(helper.Close)
	return ssn, helper
}

func TestSessionPackingAndPreemptionSimulation(t *testing.T) {
	ssn, _ := session(t)
	n := ssn.Nodes["npu"]
	low := task("low", "3", "", 1)
	add(t, n, low, api.Running)
	incoming := task("high", "2", "", 10)
	ctx, state := context.Background(), schedframework.NewCycleState()
	if err := ssn.SimulatePredicateFn(ctx, state, incoming, n); err == nil {
		t.Fatal("simulation admitted over quota")
	}
	clone := n.Clone()
	if err := ssn.SimulateRemoveTaskFn(ctx, state, incoming, low, clone); err != nil {
		t.Fatal(err)
	}
	clone.RemoveTask(low)
	if err := ssn.SimulatePredicateFn(ctx, state, incoming, clone); err != nil {
		t.Fatal("replacement within quota rejected", err)
	}
	oversized := task("oversized", "4", "", 100)
	if err := ssn.SimulatePredicateFn(ctx, state, oversized, clone); err == nil {
		t.Fatal("high priority bypassed CPU quota")
	}
	if err := ssn.SimulateAddTaskFn(ctx, state, incoming, low, clone); err != nil {
		t.Fatal(err)
	}
	if err := clone.AddTask(low); err != nil {
		t.Fatal(err)
	}
	if err := ssn.SimulatePredicateFn(ctx, state, incoming, clone); err == nil {
		t.Fatal("reprieved victim not charged")
	}
	if len(n.Tasks) != 1 {
		t.Fatal("simulation mutated original node")
	}
	if got := ssn.Preemptable(incoming, []*api.TaskInfo{low, task("equal", "1", "1", 10)}); len(got) != 1 {
		t.Fatal("priority hook not registered")
	}
	if got := ssn.Reclaimable(incoming, []*api.TaskInfo{low, task("higher", "1", "1", 20)}); len(got) != 1 {
		t.Fatal("reclaim bypassed Pod priority")
	}
	empty := node()
	empty.Name = "empty"
	add(t, empty, task("some", "1", "", 1), api.Running)
	fullScore, err := ssn.NodeOrderFn(task("candidate", "1", "", 1), empty)
	if err != nil || math.Abs(fullScore-200.0/3) > 1e-6 {
		t.Fatalf("score=%v err=%v", fullScore, err)
	}
}

func TestStatementAllocationAndRollback(t *testing.T) {
	first, second := task("first", "2", "", 10), task("second", "2", "", 10)
	ssn, _ := session(t, first.Pod, second.Pod)
	var a, b *api.TaskInfo
	for _, job := range ssn.Jobs {
		for _, pod := range job.Tasks {
			if pod.Name == "first" {
				a = pod
			}
			if pod.Name == "second" {
				b = pod
			}
		}
	}
	if a == nil || b == nil {
		t.Fatal("session tasks missing")
	}
	n := ssn.Nodes["npu"]
	stmt := framework.NewStatement(ssn)
	if err := stmt.Allocate(a, n); err != nil {
		t.Fatal(err)
	}
	// Direct Allocate tests the event guard, including a caller that skipped Predicate.
	if err := stmt.Allocate(b, n); err == nil {
		t.Fatal("allocation event failed to enforce quota")
	}
	if len(n.Tasks) != 1 {
		t.Fatalf("failed allocation leaked task: %v", len(n.Tasks))
	}
	stmt.Discard()
	if len(n.Tasks) != 0 {
		t.Fatal("gang rollback leaked quota")
	}
	if err := ssn.PredicateFn(b, n); err != nil {
		t.Fatal("quota not restored after rollback", err)
	}
}

func TestExampleConfiguration(t *testing.T) {
	data, err := os.ReadFile("../../../../example/halfbinpack/scheduler.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var cfg conf.SchedulerConfiguration
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tier := range cfg.Tiers {
		for _, option := range tier.Plugins {
			if option.Name == PluginName {
				found = true
				if p := New(option.Arguments).(*plugin); p.configErr != nil {
					t.Fatal(p.configErr)
				}
			}
		}
	}
	if !found {
		t.Fatal("example does not enable halfbinpack")
	}
}

// Compile-time proof that the exported constructor still satisfies the scheduler contract.
var _ framework.Plugin = (*plugin)(nil)

func TestVictimOrderAndTierBypassProtection(t *testing.T) {
	ssn, _ := session(t)
	low := task("low", "1", "1", 1)
	low.Job = "orphan-low"
	higher := task("higher", "1", "", 9)
	higher.Job = "another-job"
	incoming := task("incoming", "1", "", 10)
	queue := ssn.BuildVictimsPriorityQueue([]*api.TaskInfo{higher, low}, incoming)
	if got := queue.Pop().(*api.TaskInfo); got.UID != low.UID {
		t.Fatal("Job or queue order overrode Pod victim priority")
	}
	// Even an earlier-tier plugin returning immediately cannot authorize an equal-priority victim.
	enabled := true
	ssn.AddPreemptableFn("early", func(_ *api.TaskInfo, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) { return candidates, 1 })
	ssn.Tiers = append([]conf.Tier{{Plugins: []conf.PluginOption{{Name: "early", EnabledPreemptable: &enabled}}}}, ssn.Tiers...)
	got := ssn.Preemptable(incoming, []*api.TaskInfo{low, task("equal", "1", "1", 10), task("higher", "1", "", 20)})
	if len(got) != 1 || got[0].UID != low.UID {
		t.Fatal("earlier tier bypassed strict priority", got)
	}
	if got := ssn.UnifiedEvictable(&api.EvictionContext{Kind: api.EvictionKindGangPreempt}, []*api.TaskInfo{low}); len(got) != 0 {
		t.Fatal("gang path substituted Job priority for Pod priority")
	}
}

func TestSelectVictimsOnNodeRespectsQuota(t *testing.T) {
	ssn, _ := session(t)
	n := ssn.Nodes["npu"]
	low := task("victim", "3", "", 1)
	add(t, n, low, api.Running)
	for _, tc := range []struct {
		cpu     string
		success bool
	}{{"2", true}, {"4", false}} {
		incoming := task("preemptor", tc.cpu, "", 100)
		selected, status := preempt.SelectVictimsOnNode(context.Background(), schedframework.NewCycleState(), incoming, &api.QueueInfo{}, n.Clone(), ssn, nil, framework.NewStatement(ssn))
		if tc.success {
			if status.Code != api.Success || len(selected) != 1 || selected[0].UID != low.UID {
				t.Fatalf("legal replacement: victims=%v status=%v", selected, status)
			}
		} else if status.Code == api.Success || len(selected) > 0 {
			t.Fatalf("oversized high-priority CPU pod authorized victims=%v status=%v", selected, status)
		}
	}
	if len(n.Tasks) != 1 {
		t.Fatal("victim selection changed live node quota")
	}
}
