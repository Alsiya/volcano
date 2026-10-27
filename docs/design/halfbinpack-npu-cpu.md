# NPU / CPU 混部一期：按资源分配比例装箱

## 需求与实施口径

本版本替换此前按真实 CPU 使用率装箱的实现。资源分配比例与实时使用率不是同一个指标：真实负载再低，也不能突破纯 CPU 业务的 requests 配额。插件不再查询 Prometheus，也不需要监控服务。

集群只配置一个 `cpuRatio` 百分数，默认 30，取值 0–100；同一比例独立应用到每个混部节点。这里的“集群粒度”指统一策略，而非允许单个节点借用其他节点的额度。暂不支持业务、队列、命名空间、Pod 或单节点覆盖。

```
节点纯 CPU 配额(milliCPU) = floor(节点物理 CPU Allocatable(milliCPU) × cpuRatio / 100)
纯 CPU 业务 requests 总量 + 新纯 CPU Pod requests ≤ 节点纯 CPU 配额
```

使用 Node.status.allocatable 的物理 CPU，排除 Capacity 与 Volcano 合成的超卖资源。按 milliCPU 向下取整。requests 使用 Volcano 的 Pod 资源计算器，包含普通容器、init 容器、可重启 sidecar 和 Pod overhead，并采用其原地扩缩容资源计算逻辑。

| 一期需求 | 实现 |
| --- | --- |
| NPU / CPU 混部 | 在 NPU 节点允许两类 Pod 调度，保留标准资源、亲和性、污点等可行性检查 |
| 为 NPU 保留 CPU 分配空间 | 纯 CPU 业务最多分配 r%，不占用剩余 1-r 的 CPU requests 空间；NPU Pod 不消耗纯 CPU 配额 |
| 按比例装箱 | 合法节点得分为分配后纯 CPU requests / 纯 CPU 配额，越接近配额分数越高 |
| 与 NPU 是否分配无关 | 按节点 Capacity 是否声明 NPU 识别，不按 Idle NPU 数量开关配额 |
| 集群统一比例 | scheduler 插件配置统一传入 cpuRatio，不读取 Pod 覆盖参数 |
| Pod 优先级抢占与配额 | 严格筛选更低 Pod 优先级受害者，并优先选择更低优先级；普通分配、流水线预留、抢占模拟均检查 CPU 配额 |

## 识别范围与安全边界

只要 Node Capacity 有 `npuResources` 配置中的任一 NPU 资源，该节点即受管理，无需依赖业务标签。也可通过 `volcano.sh/npu-cpu-mixed=true` 显式纳入节点，即使 NPU 设备暂时未报告。

请求任一配置 NPU 资源的 Pod 视为 NPU 业务；其他 Pod 均按纯 CPU 业务计入配额。`volcano.sh/workload-type` 标签可供业务识别，但不能授予配额豁免，删除标签或伪造为 npu 都不能绕过配额。NPU 资源名必须与 Device Plugin 实际报告一致，包括虚拟 NPU 型号；不在列表内的加速资源按纯 CPU 业务保守处理。

已有非 NPU DaemonSet、系统 Pod 和由其他调度器绑定的非 NPU Pod，在 Volcano 节点快照可见时也计入纯 CPU requests。新纯 CPU Pod 必须声明正数 CPU requests，否则拒绝进入混部节点。CPU-only Pod 的零 requests 不会被“估计成 250m”，以免把估计值误当成配额。

本插件仅能控制经过此 Volcano 调度器的分配。ERS 必须保证混部业务使用 `schedulerName: volcano`，禁止通过直接设置 spec.nodeName 或其他调度器绕过准入，并通过准入策略控制已绑定 Pod 的原地扩容，避免扩容请求绕过调度器配额检查。不存在调度插件能够约束所有外部直接绑定操作的保证。

## 状态记账、释放与抢占模拟

直接读取传入的 `NodeInfo.Tasks`，持续计算所有活动纯 CPU Pod 的 requests，已运行很久的 Pod 仍计费，Succeeded / Failed 不计费。没有会在 session 结束时清空的独立配额缓存。调度快照自带的事务回滚、任务移除和重启重建会自然恢复正确额度。

为兼容异步抢占和 Volcano Pipeline，分别检查：

- 当前分配账：计入 Releasing，排除尚未实际运行的 Pipelined；实际 Allocate 事件必须通过此检查。受害 Pod 尚未删除时，不允许新 Pod 提前使用它的 CPU 分配额度。
- 未来预留账：排除 Releasing，计入 Pipelined；Predicate、NodeOrder、Pipeline 事件与抢占模拟使用此账。可以为替换受害 Pod 预留额度，但不是提前绑定运行。之后 Allocate 仍须通过当前账及标准物理空闲资源检查。

候选 Pod 在节点快照中已存在时只计一次。Allocate 事件再次校验配额，调用方即使跳过 Predicate 也不能突破分配上限。NPU 业务不计入纯 CPU 配额，仍需通过物理资源可行性检查。

新增 `SimulatePredicateFn`，与普通 Predicate 使用相同配额规则。抢占框架对克隆 NodeInfo 执行 RemoveTask / AddTask，插件直接读取克隆后的 Tasks，因此无需另设易泄漏的模拟配额缓存；移除受害者、恢复受害者、取消事务都会反映在账中。即使抢占者优先级很高，单个纯 CPU Pod 请求量超过节点配额仍无法通过模拟和最终分配。

## Pod 优先级与 ERS

启用 halfbinpack 的 session 使用 `StrictPodPreemption`：

1. Preemptable / Reclaimable 入口统一过滤，受害 Pod 的实际 spec.priority 必须严格小于抢占者。未设置视为 0；相同优先级不可抢占。其他 tier 提前作出决策也不能绕过此过滤。
2. 受害者队列优先比较实际 Pod 优先级，避免跨 Job / Queue 排序覆盖 Pod 优先级。相同优先级再使用原有排序作为平局规则。
3. 通算 / 智算身份不会参与优先级比较；支持 PreemptNever。
4. 样例不启用 stock priority 插件的 Job 优先级受害者过滤，以免跨 Job 使用与需求不同的口径。halfbinpack 提供 Pod TaskOrder 和受害者过滤；队列、公平性、gang 就绪、PDB 和可抢占性等其他调度约束继续生效。
5. 此阶段只支持 Pod 级 preempt / reclaim。gangpreempt / gangreclaim 没有具体抢占者 Pod 上下文，严格模式下拒绝该类驱逐，不用 Job 优先级冒充 Pod 优先级。

ERS / 业务负责创建或选择 PriorityClass、授予业务使用权限，并决定是否启用 preempt。推荐 NPU 使用较高 PriorityClass，但插件没有硬编码 NPU 优先。见 `example/halfbinpack/priority-classes.yaml`。本仓库没有 ERS 客户端/API；这里提供调度侧约束和配置契约，不声称完成外部 ERS 平台集成。

“CPU 业务不影响 NPU 调度”在本期表示预留 CPU requests 空间并支持优先级腾挪，而非任何 NPU Pod 都保证立即调度。NPU Pod 仍可能因自身请求过大、NPU 已占用、内存不足、亲和性不符等原因 Pending；低优先级 NPU 不得抢占更高优先级 CPU 业务。

## 配置与升级

使用 `example/halfbinpack/scheduler.yaml`。halfbinpack 应位于首个参与受害者决策的 tier，与 gang 规则共同工作，并开启 Predicate、NodeOrder、TaskOrder、Preemptable / Reclaimable。示例关闭 allocate / preempt 的 task-role 失败缓存，避免同角色不同请求量 Pod 共享过期的配额失败结果。

旧配置的 prometheusAddress、upperThreshold、lowerThreshold、blindSeconds 等会被拒绝，不能静默映射为 cpuRatio。配置错误会拒绝调度并记录日志，运维须修正配置。

升级或调低比例前，检查每节点现有纯 CPU requests。若存量已超额，插件不会自行驱逐，阻止进一步超额分配；由 ERS 根据 Pod 优先级排空/替换存量后达到新上限。不要把热更新配置解释为即时自动驱逐。

纯 CPU Pod 示例使用正数 requests 和 CPU limits，并引用推荐 PriorityClass。业务模板还需匹配实际污点容忍和亲和性。生产中需统一配置所有有权操作该节点池的调度器实例。

## 验证与范围

测试包括：NPU 空闲/占满时一致的纯 CPU 上限，稳定运行 Pod 持续计费，无标签与伪造标签不能豁免，init/overhead 计费，0%/100%/小数比例及 milliCPU 边界，纯 CPU/NPU 跨类型优先级、同级拒绝、跨 Job 受害者排序、tier 绕过防护、PreemptNever、抢占模拟移除/恢复、过大高优先级 CPU Pod 拒绝、Pipeline 与 Releasing 分账、Allocate 事件守卫、事务回滚和重启重建。

```sh
CGO_ENABLED=0 go test ./pkg/scheduler/plugins/halfbinpack ./pkg/scheduler/plugins ./pkg/scheduler/framework
```

调度分配比例不等于运行时 CPU 使用率硬限制。requests 配额不会自动约束 CPU 瞬时使用量；运行时 CPU 隔离需由 CPU limits/cgroup 或独立 CPU 池实现。本期也没有内存比例隔离。需要这些运行时保障时必须在节点/平台侧另行配置。

真实 NPU 集群验收应覆盖 ERS 提交模板与 PriorityClass 权限、NPU/CPU 跨类型抢占、CPU 配额边界、受害者删除延迟、调度器重启、原地扩容准入、节点资源变更和多实例统一策略。

2026-10-27 本地验证：Go 1.26.0、CGO_ENABLED=0，上述插件/注册包/框架三个包，以及 actions/allocate、actions/backfill、actions/preempt、actions/reclaim 四个包的测试均通过；gofmt 检查通过。新增测试直接调用上游 SelectVictimsOnNode 验证合法替换与高优先级超配额拒绝。尚未在真实 NPU 集群或 ERS 环境验收。
