// Copyright 2026 The Volcano Authors.
// SPDX-License-Identifier: Apache-2.0

package halfbinpack

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
)

func fixture() (*plugin, *api.TaskInfo, *api.NodeInfo, time.Time) {
	now := time.Now().Truncate(time.Second)
	p := New(framework.Arguments{"config": map[string]interface{}{"prometheusAddress": "http://localhost"}}).(*plugin)
	p.metrics = map[string]sample{"npu-1": {percent: 50, observed: now}}
	p.reserved = map[api.TaskID]reservation{}
	task := &api.TaskInfo{UID: "cpu-1", Resreq: api.NewResource(v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}), Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{WorkloadLabel: "cpu"}}}}
	task.NodeName = "npu-1"
	node := api.NewNodeInfo(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "npu-1", Labels: map[string]string{MixedLabel: "true"}}, Status: v1.NodeStatus{
		Capacity:    v1.ResourceList{v1.ResourceCPU: resource.MustParse("10"), "huawei.com/Ascend910": resource.MustParse("1")},
		Allocatable: v1.ResourceList{v1.ResourceCPU: resource.MustParse("8"), "huawei.com/Ascend910": resource.MustParse("1")},
	}})
	return p, task, node, now
}

func TestAdmissionAndReservations(t *testing.T) {
	p, task, node, now := fixture()
	if p.configErr != nil {
		t.Fatal(p.configErr)
	}
	// Physical CPU is the denominator: 50% + 2/10 = 70%, exactly admitted.
	if got, err := p.projected(task, node, now); err != nil || got != 70 {
		t.Fatalf("load=%v err=%v", got, err)
	}
	p.reserve(task)
	p.reserve(task) // idempotent events
	other := *task
	other.UID = "cpu-2"
	if _, err := p.projected(&other, node, now); err == nil {
		t.Fatal("second pod exceeded threshold")
	}
	// A terminating newly assigned pod still reserves CPU.
	task.Status = api.Releasing
	p.unreserve(task)
	if _, err := p.projected(&other, node, now); err == nil {
		t.Fatal("eviction freed headroom too early")
	}
	task.Status = api.Pending
	p.unreserve(task)
	if _, err := p.projected(&other, node, now); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsUnavailable(t *testing.T) {
	for _, scenario := range []string{"missing", "stale", "future"} {
		t.Run(scenario, func(t *testing.T) {
			p, task, node, now := fixture()
			switch scenario {
			case "missing":
				delete(p.metrics, node.Name)
			case "stale":
				p.metrics[node.Name] = sample{50, now.Add(-61 * time.Second)}
			case "future":
				p.metrics[node.Name] = sample{50, now.Add(time.Second)}
			}
			if _, err := p.projected(task, node, now); err == nil {
				t.Fatal("invalid metrics admitted")
			}
		})
	}
}

func TestTaskClassificationAndWarmup(t *testing.T) {
	p, task, node, now := fixture()
	if !p.isCPU(task) || !p.hasNPU(node) {
		t.Fatal("CPU or NPU classification failed")
	}
	task.Resreq = api.NewResource(v1.ResourceList{"huawei.com/Ascend910": resource.MustParse("1")})
	if p.isCPU(task) {
		t.Fatal("NPU job classified as CPU job")
	}
	task.Status = api.Running
	task.Pod.Status.StartTime = &metav1.Time{Time: now.Add(-30 * time.Second)}
	if !p.unobserved(task, p.metrics[node.Name]) {
		t.Fatal("recent pod lost its reservation")
	}
	task.Pod.Status.StartTime = &metav1.Time{Time: now.Add(-3 * time.Minute)}
	if p.unobserved(task, p.metrics[node.Name]) {
		t.Fatal("observed pod counted twice")
	}
	task.Status = api.Bound
	if !p.unobserved(task, p.metrics[node.Name]) {
		t.Fatal("bound pod not reserved")
	}
}

func TestConfiguration(t *testing.T) {
	for _, values := range []map[string]interface{}{
		{"upperThreshold": 101}, {"lowerThreshold": 70}, {"maxAgeSeconds": 0},
		{"weight": math.NaN()}, {"minPodMilliCPU": 0}, {"typo": 1},
	} {
		values["prometheusAddress"] = "http://localhost"
		if New(framework.Arguments{"config": values}).(*plugin).configErr == nil {
			t.Fatalf("accepted invalid config %v", values)
		}
	}
}

func TestPackingScores(t *testing.T) {
	p, task, node, now := fixture()
	previous := -1.0
	for _, load := range []float64{0, 20, 39.99, 40, 55, 69.99, 70} {
		score := p.score(load)
		if score < 0 || score > 100 || score <= previous {
			t.Fatalf("non-monotonic score: load=%v score=%v previous=%v", load, score, previous)
		}
		previous = score
	}
	// Empty mixed nodes must be able to enter the packing interval.
	p.metrics[node.Name] = sample{percent: 0, observed: now}
	if _, err := p.projected(task, node, now); err != nil {
		t.Fatal(err)
	}
	// No request still receives a nonzero estimate.
	task.Resreq = api.EmptyResource()
	if got, err := p.projected(task, node, now); err != nil || got != 2.5 {
		t.Fatalf("zero-request load=%v err=%v", got, err)
	}
}

func TestSessionHooks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := "50"
		if r.FormValue("query") == "timestamp" {
			value = fmt.Sprint(time.Now().Unix())
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"node":"npu-1"},"value":[%d,"%s"]}]}}`, time.Now().Unix(), value)
	}))
	defer server.Close()
	_, task, node, _ := fixture()
	test := uthelper.TestCommonStruct{
		Plugins: map[string]framework.PluginBuilder{PluginName: New},
		Nodes:   []*v1.Node{node.Node},
	}
	enabled := true
	ssn := test.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{
		Name: PluginName, EnabledPredicate: &enabled, EnabledNodeOrder: &enabled,
		Arguments: framework.Arguments{"config": map[string]interface{}{
			"prometheusAddress": server.URL, "cpuQuery": "cpu", "timestampQuery": "timestamp",
		}},
	}}}}, nil)
	defer test.Close()
	if err := ssn.PredicateFn(task, node); err != nil {
		t.Fatal(err)
	}
	if score, err := ssn.NodeOrderFn(task, node); err != nil || score != 100 {
		t.Fatalf("score=%v err=%v", score, err)
	}
	// Standard CPU nodes are a viable fallback, even without metrics.
	ordinary := api.NewNodeInfo(node.Node.DeepCopy())
	delete(ordinary.Node.Labels, MixedLabel)
	if err := ssn.PredicateFn(task, ordinary); err != nil {
		t.Fatal(err)
	}
	if score, err := ssn.NodeOrderFn(task, ordinary); err != nil || score != 0 {
		t.Fatalf("ordinary score=%v err=%v", score, err)
	}
	// Missing metrics reject CPU mixing but never reject an NPU pod.
	node.Name = "unmonitored"
	if err := ssn.PredicateFn(task, node); err == nil {
		t.Fatal("CPU mixing without metrics admitted")
	}
	task.Resreq = api.NewResource(v1.ResourceList{"huawei.com/Ascend910": resource.MustParse("1")})
	if err := ssn.PredicateFn(task, node); err != nil {
		t.Fatalf("NPU pod affected: %v", err)
	}
}

func TestPrometheusSnapshot(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	for _, scenario := range []string{"valid", "missing-label", "duplicate", "nan", "empty", "http-error"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario == "http-error" {
					http.Error(w, "unavailable", 503)
					return
				}
				value := "50"
				if r.FormValue("query") == "timestamp" {
					value = fmt.Sprint(now.Unix())
				}
				labels := `{"node":"npu-1"}`
				if scenario == "missing-label" {
					labels = `{}`
				}
				if scenario == "nan" {
					value = "NaN"
				}
				row := fmt.Sprintf(`{"metric":%s,"value":[%d,"%s"]}`, labels, now.Unix(), value)
				if scenario == "duplicate" {
					row += "," + row
				}
				if scenario == "empty" {
					row = ""
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[%s]}}`, row)
			}))
			defer server.Close()
			p, _, _, _ := fixture()
			p.cfg.Address, p.cfg.CPUQuery, p.cfg.TimestampQuery = server.URL, "cpu", "timestamp"
			metrics, err := p.fetch(context.Background(), now)
			switch scenario {
			case "valid":
				if err != nil || metrics["npu-1"].percent != 50 || !metrics["npu-1"].observed.Equal(now) {
					t.Fatalf("metrics=%v err=%v", metrics, err)
				}
			case "empty":
				if err != nil || len(metrics) != 0 {
					t.Fatalf("metrics=%v err=%v", metrics, err)
				}
			default:
				if err == nil {
					t.Fatal("invalid response accepted")
				}
			}
		})
	}
}
