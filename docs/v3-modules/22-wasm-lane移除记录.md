# Wasm lane 移除记录：代码审计与清理

> 触发：审计发现 Wasmtime 那条执行路径与项目目标（workspace + 构建 + 运行）不符，
> 且围绕它的抽象层大量是死代码/装饰性字段。本文记录**代码审计结论**、**删除清单**、
> **连带影响**、**留下了什么证据**，以及对早期文档的更正说明。

## 1. 审计方法

不只读文档，逐文件读代码 + 用 `grep` 确认每个符号的**真实调用者**。

## 2. 代码实测：真的部分 vs 装饰部分

| 项 | 结论 | 代码坐标 |
|---|---|---|
| 输入是**已编译字节码**，不是 workspace | 事实（`execute_code(bytecode)`） | `wasm.rs` |
| WASI 未接（无文件系统/网络） | 事实 | `Linker::new(&engine)` 后无 `add_wasi_*`，无 preopens |
| `stdout`/`stderr` 恒为空 | 事实 | `execute_code` 返回 `Vec::new()`（trap 只写日志不写结果） |
| 无 `_start` 导出时**静默成功** | 缺陷 | `wasm.rs` `Err(_) => Epilog::Ok` |
| `StoreLimits` 内存上限 | **真实现** | 单测 `initial_memory_over_limit_traps` 实测硬失败 |
| `epoch_interruption` 掐死死循环 | **真实现** | 单测 `infinite_loop_is_interrupted_by_epoch` 断言 exit 124 |
| `task.resource_limits` 生效 | **否** | `execute_wasm` 只 `let _ = timeout_ms;`，用 `WasmtimeSandbox::new()` 默认值 |
| `task.routing` / `pinned_node` 生效 | **否** | 执行只按最小 `raft_id` 选节点 |
| `Sandbox::prepare_env` 被调用 | **否** | 全仓唯一调用点在 `registry.rs`，而 registry 无人使用；自身实现也只 `debug!` |
| `SandboxRegistry` / `default_registry` / `SandboxFactory` | **死代码** | 只在自身单测出现 |
| `LinuxContainerSandbox` | **诚实 stub** | `execute_code` 直接返回 error |
| `TaskScheduler` / `PreferWasm` / round-robin | **死代码** | 从未被实例化（`mod.rs` 用 `#![allow(dead_code)]` 兜着） |
| `SubmitTask/AssignTask/CompleteTask` + `ClusterState` 三个队列字段 | 仅服务 Wasm 执行 | `state_machine.rs` / `proposal.rs` |

**结论**：真实价值只剩两条——「已编译模块能跑通」（`integration_3node_test.sh` +
`examples/submit_task.rs`）和「资源限额可证伪」（两个单测）。其余是壳。

## 3. 删除清单

| 文件/符号 | 处理 |
|---|---|
| `crates/sandbox/src/wasm.rs` | 删除（含 4 个单测） |
| `crates/sandbox/src/registry.rs` | 删除（工厂注册表，无人调用） |
| `crates/sandbox/src/container.rs` | 删除（诚实 stub，无实现计划） |
| `crates/sandbox/Cargo.toml`：`wasmtime = "45"`、dev-dep `wat` | 删除依赖 |
| `eo_core::traits::Sandbox` | 删除（实现者已全部消失） |
| `eo_core::types::{RuntimeKind, RoutingStrategy, ScheduledTask}` | 删除；`Capabilities.runtimes` 改为 `Vec<String>`（保留为节点兼容性比较用的自由标签） |
| `eo_core::types::ResourceLimits` | **保留**：仍是资源策略类型，工程任务仍声明它（当前 qlean 未消费，见 §5） |
| `crates/node/src/orchestration/scheduler.rs` | 删除（`TaskScheduler` 死代码） |
| `node/src/orchestration/runtime_loop.rs` | 去掉 coordinator/executor 的 Wasm 执行体；保留注册循环，两个循环改为**显式 scaffold**，不再伪造工作 |
| `ClusterState::{task_queue, assigned_tasks, completed_tasks}`、`AssignedTask`、`ClusterSnapshot` 对应字段 | 删除 |
| `Proposal::{SubmitTask, AssignTask, CompleteTask}`、`ApplyResult::{TaskSubmitted, TaskCompleted}` | 删除 |
| IPC `submit_to_cas_and_raft` / `fetch_execution_result` | 删除（连同 `SubmitParams` 与 3 个单测）——它们只服务 Wasm 任务 |
| `crates/node/examples/submit_task.rs` | 删除（Wasm lane 的示例客户端） |
| `scripts/integration_3node_test.sh` | 第 4 步从「提交 wasm 任务」改为「验证复制状态里的节点注册」；raft 选主/重选第 5 步保留 |
| `configs/cluster-node-{1,2,3}.yaml` 的 `runtimes: ["Wasm"]` | 改为 `["qlean"]`（原来就是过时/误导的标签） |

## 4. 连带影响（必须明确）

1. **Raft 共识现在只复制节点注册与角色变更**，不再有任务调度。
   `role_engine`（角色分配/失效重分配）与 `pick_executor` 仍可用，但**没有任务流经它们**。
   这正是「ProjectTask 接入 raft 调度」被列为下一步第 2 优先级的原因；
   `spawn_runtime` 保留了显式的 scaffold 注释与 `tracing::debug!`，避免再出现「看起来在跑其实没有」。
2. **删掉的两个 IPC 方法**是唯一能提交 wasm 模块的入口。对外接口现在只有
   `submit_project` / `fetch_project_result`（工程 lane）与只读的 `get_cluster_topology`。
3. **失去的东西**：全项目唯一能「证伪资源限额」的实现（`StoreLimits` + epoch）与它的两个单测。
   qlean 目前没有 cgroup/内存上限，guest 内死循环只能靠杀 VM 兜底（实测还会因 apt 被 `kill -9`
   而污染复用 VM 的锁）——**这是这次清理最重要的代价，已记入待办**。
4. **收益**：依赖树 524 → 约 400 crate（wasmtime 相关 35 个），构建/CI 变快；
   代码少约 540 行；不再有「声明了但没生效」的字段（`resource_limits`/`routing`/`pinned_node`
   在 wasm lane 里是装饰性的，删除后这条歧义消失）。

## 5. 留下的债务（诚实清单）

- `Capabilities.runtimes` 现在没有任何执行路径消费它，只用于角色重分配时的兼容性比较；
- `ResourceLimits` 被 `ProjectSpec` 携带但 qlean 未消费（无 cgroup/内存强制）；要真正生效需要
  qlean 侧接入 cgroup v2 或 QEMU `-m` 硬限制；
- `runtime_loop::{coordinator_loop, executor_loop}` 已不产生任何工作；
  `get_cluster_topology` 的 `tasks_completed` 仍恒为 0（字段本身也该跟随清理）；
- CAS blob 分发（`RequestBlob`）在 node 侧的调用点随 Wasm 执行一并消失，
  目前只有协议层与 blob 提供方（`BlobProvider`）还在——正好是「快照走 CAS 分发」要复用的东西。

## 6. 早期文档更正

以下文档写于 Wasm lane 还是主执行路径时，**不再与代码一致**，阅读时请以本文为准：

| 文档 | 现在的问题 |
|---|---|
| `docs/v3-modules/00-总览与基线.md`、`01-M1砍骨.md` | 把「Wasmtime 真限额」列为已完成交付物 |
| `docs/v3-modules/02-M2沙箱真限额.md` | 描述的实现已删除（证据保留在本文件 §2） |
| `docs/v3-modules/04-M3真分布式.md` | 任务链路是 Wasm 的，现已不存在 |
| `docs/v3-architecture.md` | 执行层写的是 `Sandbox trait` / Wasmtime / container stub |
| `plan-v2/02、03、04、06` | 选型与里程碑以 Wasmtime 为主 |

处置：这些文档作为**历史记录保留**，在各文件顶部加了指向本文的说明行；
`README.md` 的「亮点/架构/crate 表」已改成当前真实架构。
