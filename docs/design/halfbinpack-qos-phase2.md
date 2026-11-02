# halfbinpack 第二阶段：运行时 QoS 支持性评估与 Agent 能力抽象

## 本阶段交付与边界

一期 halfbinpack 控制纯 CPU requests 配额与 Pod 优先级抢占。二期的目标是在节点上支持 NPU / CPU 混部的 CPU、内存、网络吞吐、磁盘 IO 隔离和压制。调度配额不能替代这些运行时控制。

本次需求明确依赖 HCE OS 基线支持性评估，详细诉求随后由业务问题驱动。当前尚未提供 HCE OS 镜像版本、内核、cgroup 模式及业务 SLO，因此本次交付定位为**只读评估工具、独立 Adapter 契约、现有 Agent 复用评估及验收方案**。不是“四维隔离已完成”的生产实现。

代码不改变一期 CPU 比例和抢占规则，不自动设置 volcano.sh/qos-level，不启动压测，不写 cgroup、sysctl、TC，不加载内核模块，也不注册新的 Agent 执行处理器。探测工具不发布调度标签或节点 Ready 状态；不存在以人工标签代替执行状态核验的流程。

基于一期 PR #2 的代码（提交 2b49e707cb05c3ae003c93981b4962ef34f3452f）及同一仓库 Agent 源码完成评估。PR #2 尚未合并时，本阶段变更应作为其后续独立 PR 审阅。

## 源码与 OS 支持矩阵

| 维度 | 已有 Agent 能力 | 本次评估结论 | HCE 节点上待确认 |
| --- | --- | --- | --- |
| CPU QoS | cpuqos 处理器写 cpu.qos_level；其他资源处理器有 CPU quota 路径 | 已有定制接口适配代码，但不是任意 Linux / HCE 版本都具备该接口 | 接口值域、CPU 调度优先级/SMT 语义、兼容开关、cgroup v1/v2、NPU 宿主线程行为 |
| Memory QoS | memoryqos 写 memory.qos_level；memoryqosv2 处理 memory.low/min/high | 存在定制与标准 v2 两类路径，不能直接视为同等隔离语义 | 父子保护预算、回收行为、swap、OOM、页缓存以及 NPU 锁页/固定内存的归属 |
| 网络吞吐 | networkqos 使用 net_cls.classid 和 network-qos 管理器 | classid 是分类标记，不是独立的带宽保证；当前 OS 检查对 openEuler 版本有约束，不能直接推断 HCE 可用 | 内核/TC 数据面、实际启用状态、CNI、网卡队列、收/发方向、hostNetwork、SR-IOV 和 RDMA 路径 |
| 磁盘 IO | 当前 pkg/agent 未发现独立磁盘 IO QoS 执行处理器 | 需要新增适配，不应把标准 io.max 存在误报为现有 Agent 已支持 | 块设备 major:minor、IO 调度器、文件系统/writeback、直写/缓存写、CSI/云盘、多路径、BPS/IOPS 与延迟目标 |

源码依据：

- `pkg/agent/events/handlers/cpuqos/cpuqos_linux.go`
- `pkg/agent/events/handlers/memoryqos/memoryqos_linux.go`
- `pkg/agent/events/handlers/memoryqosv2/memoryqosv2_linux.go`
- `pkg/agent/events/handlers/networkqos/networkqos_linux.go`
- `pkg/agent/features/feature_gate.go`、`pkg/networkqos/networkqos.go`

既有 CPU / Memory / Network handler 对部分“接口不存在”场景会返回 nil，因此现有 handler 返回成功也不能单独作为压制已生效的验收证据。二期后续执行适配必须显式区分 Unsupported、ApplyFailed、Applied、Verified 和 RestoreFailed。

官方资料：

1. [HCE 定制内核参数](https://support.huaweicloud.com/usermanual-hce/hce_18_0002.html)：记录了 CPU QoS 的兼容开关。仅凭 HCE 版本名称不足以决定等级映射。
2. [Volcano v1.16 云原生混部](https://volcano.sh/docs/v1.16.0/keyfeatures/cloudnativecolocation/)：已有 Agent 的分级隔离是可复用起点，仍需匹配内核和部署配置。
3. [Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)：memory.low/min 是保护机制，memory.high 是回收/节流边界；io.max 是按设备限制 BPS/IOPS，不能替代应用级存储延迟保障。

结论为“源码存在相关能力且具备适配路径”，不是特定 HCE 镜像已认证。CPU weight 与 CPU quota、内存保护与内存硬限额、网络分类与吞吐限制是不同机制，不能隐式互相降级。

## 独立能力抽象

新增无 Kubernetes 客户端、无 CRD、无 DaemonSet 依赖的 `pkg/agent/qos`：

- Report：OS ID、OS 版本、内核、cgroup 标记和逐机制接口元数据。检测不到某个目标文件只说明该路径没有该接口，也可能是路径不正确或控制器未委派，不能推出整个内核不支持。
- Policy / Requirement：按机制明确表达需求，不仅用 cpu/memory/network/io 维度代替具体语义。当前只用于评估，没有生产限额默认值。
- Assess：列出缺失接口和未验证前置条件。所有接口存在时仍不能输出 EnforcementReady=true；接口探测也不能将 EnforcementVerified 设置为 true。
- Adapter / PreparedChange：为后续执行实现预留 Prepare、Apply、Verify、Restore 生命周期。当前没有内置 writer，也没有自动注册到已有 Agent handler。

后续执行层应先为所有必需维度完成参数、身份、路径、能力、权限和冲突校验，再执行任何写操作；执行失败需逆序恢复已修改的自有状态，恢复失败必须报告，不能假装多资源操作原子成功。Prepare 需验证运行时解析得到的 Pod UID 与 cgroup 归属，禁止修改 root/system/kubelet 等无关组。

能力报告只检查文件元数据，不验证 cgroup 挂载类型、读写委派、控制器父链、网络 TC/BPF 链路或磁盘实际归属；这些均保留为未知。现有 Handler 路径只是源码线索，不是“Adapter 已加载”的状态。

## 只读工具

独立入口 `cmd/agent-qos-probe`，不需要启动整个 Volcano-agent：

```sh
# 在源码根目录构建适用于目标节点架构的二进制。
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o agent-qos-probe ./cmd/agent-qos-probe

# 先检查帮助；不存在 --apply 参数。
./agent-qos-probe --help
```

在目标 HCE 节点上运行。下面的 Pod cgroup 路径是占位示例，必须替换为已存在的真实业务 cgroup；不能复制执行并期待自动寻找 Pod。工具不会创建测试 cgroup。

```sh
# cgroup v2 示例：各控制器指向同一现有 Pod cgroup。
./agent-qos-probe \
  --cgroup-root sys/fs/cgroup \
  --cpu-cgroup sys/fs/cgroup/REPLACE_WITH_REAL_POD_CGROUP \
  --memory-cgroup sys/fs/cgroup/REPLACE_WITH_REAL_POD_CGROUP \
  --network-cgroup sys/fs/cgroup/REPLACE_WITH_REAL_POD_CGROUP \
  --io-cgroup sys/fs/cgroup/REPLACE_WITH_REAL_POD_CGROUP \
  --policy example/halfbinpack/qos/assessment-policy.json > qos-report.json
```

cgroup v1 需分别指定 CPU、memory、net_cls、blkio 的实际挂载及 Pod 路径，含 cpu,cpuacct 合并挂载和 systemd slice 等差异。容器内采集必须把宿主文件系统只读挂载到 `/host`，然后指定 `--host-root /host`；内核版本与 OS 身份也从此挂载读取，避免误读工具容器镜像的 OS。

路径参数相对 host-root，不接受绝对路径或 `..`。所选 host-root 及其符号链接由管理员控制；该工具不是针对不可信目录的安全沙箱。输出 JSON 有敏感部署路径时，分享前按组织要求脱敏。

评估策略示例列出四维机制，刻意不宣称它们能在同一种 cgroup 模式下同时可用。例如 network.netcls.v1 与 io.max.v2 的组合在纯 v2 基线上可能出现缺口，这正是评估应暴露的问题。操作员必须按真实基线选择机制，不能通过删除未就绪维度把结果当作完整四维支持。

命令退出 0 只表示成功生成报告，不表示全部维度可用、基线已通过或策略已经执行。缺失/未知能力留在报告中；无效策略或输入退出 2。后续平台应读取 JSON 状态而非仅依赖退出码。

## 调度优先级、运行时 QoS 与控制权

一期 Pod 优先级决定调度/抢占资格；二期 QoS 决定已运行任务争用时的资源份额与压制。二者不互相覆盖。ERS/业务显式决定保护对象和压制对象，NPU 并非天然最高优先级，纯 CPU 也不必然是 BestEffort。自动 NPU→LC、CPU→BE 的映射尚未实现，也不应未经业务确认默认启用。

后续设计优先复用现有 volcano.sh/qos-level 及 ColocationConfiguration，避免建立重复控制面。需要注意现有 BE 流程可能引入扩展资源使用约束，不能直接套用到一期普通 CPU requests 配额上。

Agent 应抽象为库加可选宿主集成层。库可以独立探测、验证和准备策略；宿主层负责 informer/运行时解析、配置 watch、状态与指标上报、权限和生命周期。是否新增 CRD、是否由现有 ConfigMap/CR 管理、是否允许 Pod 自定义阈值，待详细需求确认后决定，不在当前提交扩大 Kubernetes 权限。

## 继续开发前所需信息

| 项目 | 必需输入 |
| --- | --- |
| HCE 基线 | 版本、镜像 ID、完整 uname -r、架构、内核定制补丁/模块、相关 sysctl 与 cgroup 模式 |
| 容器平台 | kubelet/CRI、systemd/cgroupfs、Pod 到 cgroup 的解析方法、现有 Agent / CNI / 节点资源管理器 |
| CPU | 权重/硬限额/独占 cpuset/SMT 压制的具体诉求与可接受 NPU 性能变化 |
| 内存 | 保护/回收/硬上限、swap/OOM 策略、NPU 固定内存/驱动内存可控性 |
| 网络 | 入口/出口、共享链路容量、吞吐/突发预算、NPU 数据面、RDMA/SR-IOV/hostNetwork 覆盖 |
| IO | 目标设备与卷、BPS/IOPS、读写、direct/buffered、多租户共享卷和延迟指标 |
| 恢复 | Agent 重启、配置撤销、Pod 删除、基线不支持、局部执行失败的恢复和告警要求 |

## 验收设计

同一 OS 镜像/内核/节点硬件上先跑 NPU 单业务基线，再同时压测 CPU、内存、网络和 IO 干扰，比较无隔离、启用单维、启用四维、撤销恢复四组结果。业务侧先定义 P95/P99 时延、吞吐、训练步时/推理时延可接受变化，不预设“5%以内”之类未经确认的指标。

- CPU：争用与 SMT 同核场景，检查节流/份额、NPU 宿主线程及其业务指标。
- 内存：匿名页/页缓存压力，观察 memory.events、回收、PSI、OOM 与 NPU 固定内存。
- 网络：双向 iperf 等流量测试，并对真实 NPU 数据面验证；不能用 TCP 测试代替 RDMA 结论。
- IO：按目标设备/卷执行 fio 的 direct/buffered、读写、BPS/IOPS 与延迟测试；排除其他盘和网络存储的错误归因。
- 故障：缺接口、权限不足、配置错误、执行中失败、重启、Pod UID 重用、控制器冲突、恢复失败，均须报告明确状态。

本次仅在文件系统 fixture 上验证只读性、缺失/权限错误、v1/v2 标记、部分控制器、策略参数检查及“不误报 Ready”。未在 HCE OS / NPU / ERS 实机验证，不提供四维压制效果承诺。

2026-11-02 本地验证：Go 1.26.0，`CGO_ENABLED=0 go test ./pkg/agent/qos ./cmd/agent-qos-probe` 通过；gofmt 检查及 Linux arm64 / amd64 交叉编译通过。没有运行需要节点特权的执行或压力测试。
