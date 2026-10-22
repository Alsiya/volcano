# NPU / CPU 混部一期：halfbinpack

## 需求与口径

允许 CPU 业务利用 NPU 节点的空闲 CPU，并在指定负载范围内优先装箱。经确认，阈值采用**监控采集的真实节点 CPU 使用率**。CPU requests 仅用于新 Pod 的负载预估和 Kubernetes 资源可行性检查，不替代真实使用率。

源码基线：Volcano master，2026-10-22 查询到提交 `ab52d0d95ff59357a5ce4bf2d1fc4f4364d75f64`。本工作区通过 master 源码归档获取，不包含 Git 历史。部署到其他 release 前需适配并验证框架 API。

上游参考：

- [binpack](https://github.com/volcano-sh/volcano/blob/ab52d0d95ff59357a5ce4bf2d1fc4f4364d75f64/pkg/scheduler/plugins/binpack/binpack.go)：按 requests 装箱，没有真实负载准入阈值。
- [usage](https://github.com/volcano-sh/volcano/blob/ab52d0d95ff59357a5ce4bf2d1fc4f4364d75f64/pkg/scheduler/plugins/usage/usage.go)：具有真实负载过滤和影子负载，但倾向低负载节点，且指标过期时放行。
- [Prometheus 适配器](https://github.com/volcano-sh/volcano/blob/ab52d0d95ff59357a5ce4bf2d1fc4f4364d75f64/pkg/scheduler/metrics/source/metrics_client_prometheus.go)：查询缺失时仍可能刷新 MetricsTime，不适合直接承担本需求的严格指标有效性检查。

本实现为新增的自定义插件，名称沿用需求中的 halfbinpack；没有假定上游已有同名插件。

## 一期行为

| 对象 | 行为 |
| --- | --- |
| Node 带 `volcano.sh/npu-cpu-mixed=true` 且 Capacity 有配置的 NPU 资源 | CPU 混部目标节点 |
| Pod 带 `volcano.sh/workload-type=cpu` 且不请求配置的 NPU 资源 | 受插件约束的 CPU 业务 |
| CPU Pod → 混部节点 | 检查指标有效性、预测负载上限，再按装箱得分排序 |
| CPU Pod → 普通节点 | 本插件放行、返回 0 分；仍须通过其他插件 |
| NPU Pod | 不受本插件准入阈值限制；新 NPU Pod 的 CPU 预估会占用后续 CPU 混部余量 |
| 缺失、过期、未来时间或无效指标 | 暂停在对应混部节点接纳 CPU 业务 |
| 查询错误、部分查询告警、重复节点数据 | 本轮暂停所有混部节点的 CPU 准入，普通节点与 NPU 业务继续走其他调度规则 |

混部是显式启用的。CPU 业务的标签需由提交模板或准入策略保证；未标记 Pod 不受本插件限制。因此，集群必须保持原有 NPU 节点污点，并仅向受控 CPU 业务授予匹配容忍，避免未标记业务绕过阈值。

示例阈值为 `[40%, 70%]`：40% 是目标区间下界，70% 是预测负载准入上限。低于 40% 仍允许启动装箱，否则空闲节点无法进入目标区间。上限包含等号；超过上限拒绝。通过筛选的节点中，预测负载越接近上限分数越高。

## 负载计算

设：

- `U`：Prometheus 监控的真实 CPU 使用率，百分数 0–100。
- `C`：节点物理 CPU Capacity，单位 milliCPU。与示例 PromQL 的物理 CPU 归一化口径一致；不能用 allocatable 替换。
- `E(p)=max(p.Resreq.MilliCPU, minPodMilliCPU)`：新 Pod 的预估 CPU 消耗。
- `R`：已分配但尚未被监控完整覆盖的 Pod 预估 CPU 总量。

准入公式：`U_projected = U + 100 × (R + E(p)) / C`，要求 `U_projected <= upperThreshold`。

例如节点 64 核、真实负载 50%、上限 70%，新 CPU Pod 请求 4 核：预测为 56.25%。首次分配后预留 4 核，第二个同等 Pod 预测为 62.5%，第三个为 68.75%，第四个为 75%，被拒绝。实际 requests 是否可用，仍由标准资源过滤判断。

打分为 0–100 乘 `weight`：低于 lower 的区间映射到 0–50，目标区间映射到 50–100。默认分数会柔性偏好可接纳的混部节点，其他插件的分数可能改变最终排序；不承诺“所有 NPU 节点填满后才能用普通节点”。如需严格两阶段选池，应额外设计节点选择策略。

## 接入与预留

`OnSessionOpen` 在 5 秒超时内批量查询一次 CPU 和原始采样时间，生成本轮快照。Predicate 和 NodeOrder 不执行网络请求。PromQL 必须按 `node` 返回唯一序列，且该标签与 Kubernetes 节点名完全一致。默认 node-exporter 只有 `instance` 标签时，需要增加采集 relabel；不能把 `IP:9100` 直接当节点名。

独立查询 `timestamp(node_cpu_seconds_total)` 的数值，避免把 PromQL 求值时间误当成采样时间。`timestampQuery` 应与 `cpuQuery` 使用相同 exporter、同一节点集合；如配置 recording rule，必须保留对底层 exporter 样本新鲜度的检查。

每轮扫描 Allocated、Binding、Bound、Pipelined 和尚未覆盖完整观测窗口的 Running Pod 重建预留。Allocate 事件按 Pod UID 幂等加入预留，Deallocate 回滚移除预留。异步驱逐进入 Releasing 时保留预留，不提前把尚未释放的 CPU 当作空闲。预留同时计算 CPU 与 NPU 新业务，防止两类业务连续分配时低估负载。

`blindSeconds` 至少覆盖 CPU rate 窗口、采集间隔和监控传播延迟；默认 120 秒。它可能造成短期保守的双计数，但减少新 Pod 尚未进入监控时的超量调度。每轮重建也覆盖调度器重启场景。过期指标即使仍有预留也不会放行。

配置错误会关闭标记节点上的 CPU 混部准入并记录日志。Prometheus 地址与查询仅接受运维配置，不接受业务 Pod 覆盖。当前客户端使用默认系统 TLS 校验；需要认证的集群可通过已受控的监控代理暴露查询服务，尚未新增 bearer token 配置。

## 部署

1. 按实际 NPU Device Plugin 的资源名修改 `npuResources`，不要假定示例覆盖所有 Ascend 机型和虚拟 NPU。
2. 为目标 Node 添加 `volcano.sh/npu-cpu-mixed=true` 标签。
3. 使用包含本插件的源码构建 scheduler 镜像；`factory.go` 已注册插件。
4. 合并 `example/halfbinpack/scheduler.yaml` 到现有 scheduler ConfigMap，保留业务所需的其他插件。开启 Predicate 和 NodeOrder。
5. 一期使用 enqueue / allocate / backfill；preempt / reclaim 的模拟路径需单独验证后启用。
6. CPU 业务设置 `schedulerName: volcano` 和 workload 标签。按真实污点授予精确 toleration，并移除排除 NPU 节点的硬性 nodeSelector / affinity。不要 blanket 容忍全部污点。
7. 将 Prometheus node 标签与 Kubernetes 名称对齐，验证 CPU 数值为百分数而非 0–1 比例，并测试 exporter 中断后 60 秒内停止新 CPU 混部。
8. 灰度到少量节点，再观测 NPU 任务延迟、吞吐和 CPU throttling，调整上限与 CPU limits。

一期没有动态超卖：当现有 Pod requests 已占满 allocatable 时，即使实际 CPU 空闲也不能继续调度。要释放这部分空闲，需要合理调整 requests；超卖资源上报应另立需求。

## 验证与边界

单元测试覆盖上限等值、连续分配、重复事件、回滚、异步驱逐、跨轮暖启动、NPU 分类、指标缺失/过期/未来时间、无效配置，以及 Prometheus 空响应、异常值、重复节点和 HTTP 失败。运行：

```sh
CGO_ENABLED=0 go test ./pkg/scheduler/plugins/halfbinpack
CGO_ENABLED=0 go test ./pkg/scheduler/plugins
```

集群验收至少包括：普通 CPU 节点回退、NPU 正常调度、真实压力升高后停止 CPU 准入、指标恢复后恢复准入、gang 分配回滚、调度器重启及连续批量 Pod 分配。源码测试不能替代这些集群验收。

2026-10-22 本地验证：使用临时 Go 1.26.0、`CGO_ENABLED=0`，上述两个包的 `go test` 均通过；新增 Go 文件及 factory.go 的 gofmt 检查通过。HTTP 测试使用本地模拟 Prometheus，并已验证框架 Predicate / NodeOrder 注册与调用。尚未部署到真实 Kubernetes / NPU 集群，未进行端到端或真实性能验证。

真实 CPU 负载受存量业务突增、监控延迟及实际用量超过 requests 等因素影响。本插件提供**调度时的准入控制与装箱偏好**，无法保证运行时 CPU 使用率永久不超过上限；一期不含自动驱逐、迁移或 NPU 业务 CPU 硬隔离。需要运行时保障时，应配合 CPU limits、优先级以及后续驱逐/资源隔离策略。
