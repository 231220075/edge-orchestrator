# Edge-Cloud Orchestrator — 架构设计文档

## 1. 系统概述

Edge-Cloud Orchestrator 是一个自建的分布式边云编排平台——一个轻量级、去中心化的替代方案，对标 Kubernetes 或 Knative 等平台。它使异构设备（Mac、Linux 虚拟机、iPhone、云实例、树莓派）能够组成 P2P 集群，动态调度和执行沙箱化的工作负载，并通过 AI 辅助的自然语言进行控制。

### 1.1 核心价值

- **设备抽象**：所有设备都被抽象为具有声明式能力标签的通用计算节点。角色是动态分配的，而非硬编码。
- **去中心化控制**：Raft 共识算法维护集群状态，无单点故障。
- **多态执行**：沙箱化代码可通过 Wasmtime 跨平台运行，或在 Linux 上通过容器原生运行。
- **AI 原生运维**：基于 LangGraph 的智能体提供带自愈能力的自然语言编排。

### 1.2 关键质量属性

| 属性 | 优先级 | 策略 |
|-----------|----------|----------|
| **可扩展性** | 最高 | 多态 trait（Sandbox、ObjectStore），基于能力调度，crate 工作空间 |
| **韧性** | 高 | Raft 共识，自愈重试循环，故障转移编排 |
| **可维护性** | 高 | 模块化 crate 架构，trait 驱动设计，全面的测试覆盖 |
| **性能** | 中 | 内容寻址去重，zlib 压缩，LRU 缓存 |
| **互操作性** | 中 | 基于 UDS 的 JSON-RPC 2.0，YAML 配置，协议无关传输层 |

## 2. 架构总览

```
┌──────────────────────────────────────────────────────────────────┐
│                       外部客户端                                    │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │  eo-agent    │  │  iOS         │  │  HTTP Relay          │   │
│  │  (Python)    │  │  Shortcuts   │  │  (Flask/FastAPI)     │   │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘   │
│         │                 │                      │               │
│         └─────────────────┼──────────────────────┘               │
│                           │                                      │
│              JSON-RPC 2.0 over Unix Domain Socket                │
└───────────────────────────┬──────────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────────┐
│                     IPC 层 (crates/ipc)                           │
│                                                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐│
│  │ IpcServer    │  │ JsonRpc-     │  │ 方法:                    ││
│  │ (UnixListener)│  │ Handler      │  │ • get_cluster_topology   ││
│  │              │  │              │  │ • submit_to_cas_and_raft ││
│  │ 每连接       │  │ 分发到       │  │ • fetch_execution_result ││
│  │ 独立 task    │  │ 子系统       │  │                          ││
│  └──────────────┘  └──────────────┘  └──────────────────────────┘│
└───────────────────────────┬──────────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────────┐
│              编排层 (crates/orchestration)                        │
│                                                                    │
│  ┌───────────────┐  ┌──────────────┐  ┌─────────────────────────┐│
│  │ RoleOrchestra-│  │ TaskScheduler│  │ Health Reporter          ││
│  │ tionEngine    │  │              │  │                          ││
│  │              │  │ 路由策略:    │  │ • 节点健康追踪           ││
│  │ • 故障转移   │  │ • RoundRobin │  │ • 集群摘要               ││
│  │ • 拓扑       │  │ • PreferWasm │  │ • 不健康节点检测         ││
│  │   diff/apply │  │ • PreferNative│ │                          ││
│  │              │  │ • Pinned     │  │                          ││
│  └───────────────┘  └──────────────┘  └─────────────────────────┘│
└──────┬──────────┬──────────────┬──────────────┬──────────────────┘
       │          │              │              │
┌──────▼──┐ ┌─────▼────┐ ┌──────▼──────┐ ┌─────▼─────────────────┐
│  RAFT   │ │   P2P    │ │  STORAGE    │ │  SANDBOX               │
│  共识   │ │  网格    │ │  (CAS)      │ │                        │
│         │ │          │ │             │ │                         │
│ tikv/   │ │ libp2p   │ │ Git 模型    │ │ Wasmtime (全平台)       │
│ raft-rs │ │ TCP+Noise│ │ SHA-256     │ │ Linux ns/cgroups (Linux)│
│         │ │ +Yamux   │ │ +zlib       │ │                         │
│ 集群    │ │ mDNS     │ │ Blob/Tree/  │ │ Sandbox trait →         │
│ 状态机  │ │ Identify │ │ Commit/Tag  │ │ 多态分发                │
│         │ │ Ping     │ │             │ │                         │
└─────────┘ └──────────┘ └─────────────┘ └────────────────────────┘
```

### 2.1 分层架构

系统遵循严格的四层分层架构：

1. **外部客户端层** — Python eo-agent CLI、iOS 快捷指令、HTTP 中继
2. **IPC 层** — 基于 Unix Domain Socket 的 JSON-RPC 2.0，无状态一次性连接
3. **编排层** — 角色引擎、任务调度器、拓扑调谐、健康上报
4. **基础设施层** — Raft 共识、P2P 网络、CAS 存储、沙箱执行

每层仅依赖其直接下层。横切关注点（类型、错误、trait）集中在 `eo-core` 中。

## 3. Crate 架构（Rust 工作空间）

### 3.1 依赖图

```
node (二进制入口)
├── ipc
│   └── eo-raft, storage, sandbox, orchestration
├── orchestration
│   └── eo-raft, storage, sandbox
├── p2p
├── eo-raft
│   └── storage
├── storage
└── sandbox
    └── eo-core

eo-core (无工作空间依赖 — 基础层)
```

### 3.2 Crate 职责

| Crate | 职责 | 关键类型/Trait |
|-------|---------------|-----------------|
| `eo-core` | 共享类型、trait、错误 | `NodeDescriptor`、`Sandbox`、`ObjectStore`、`CoreError` |
| `p2p` | P2P 网络 | `EdgeOrchBehaviour`、`SwarmHandle`、`Event` |
| `eo-raft` | Raft 共识 | `RaftNode`、`Proposal`、`ClusterState`、`CasRaftStorage` |
| `storage` | 内容寻址存储 | `LocalObjectStore`、`Blob`、`Tree`、`Commit`、`Tag` |
| `sandbox` | 多态执行 | `WasmtimeSandbox`、`LinuxContainerSandbox`、`SandboxRegistry` |
| `orchestration` | 调度与角色 | `RoleOrchestrationEngine`、`TaskScheduler`、`Reporter` |
| `node` | 二进制入口 | 节点启动、配置解析、事件监控 |
| `eo-ipc` | IPC 服务 | `IpcServer`、`JsonRpcHandler` |

## 4. 关键设计模式

### 4.1 策略模式 — 多态沙箱

`Sandbox` trait 定义了执行不可信代码的统一接口。平台相关的实现在运行时通过 `SandboxRegistry` 选择：

```rust
pub trait Sandbox: Send + Sync {
    fn prepare_env(&self, limits: ResourceLimits) -> Result<()>;
    fn execute_code(&self, bytecode: Vec<u8>) -> Result<ExecutionResult>;
    fn destroy(&self) -> Result<()>;
}
```

这使得编排层可以在不知道任务将在 Wasmtime（Mac）还是 Linux 容器中运行的情况下调度任务——正确的后端根据节点能力自动选择。

### 4.2 Actor 模型 — 基于通道的并发

长生命周期的子系统（P2P swarm、Raft 节点、IPC 服务器）各自运行在独立的 tokio 任务上：

- **P2P Swarm**：通过 mpsc sender 接收 `SwarmCommand`，通过 mpsc receiver 发出 `Event`
- **Raft Node**：通过 mpsc 接收 `Proposal`，按间隔 tick，读取传输消息
- **IPC Server**：为每个连接创建独立任务，分发到 `JsonRpcHandler`

这提供了清晰的隔离性、通过有界通道实现的背压，以及独立的生命周期管理。

### 4.3 事件溯源 — Raft 状态机

所有集群变更都通过经 Raft 共识提交的 `Proposal` 值流转。`ClusterState` 确定性状态机重放这些变更：

```
Proposal → Raft 日志条目 → 提交 → apply() → ClusterState
```

这为我们提供了：审计追踪、快照/恢复、确定性重放和分布式一致性。

### 4.4 声明式调谐 — 拓扑 Diff

集群拓扑以 YAML（`ClusterTopologySpec`）声明，与当前状态做 diff，然后作为 Raft 提案应用。这遵循了 Kubernetes 的调谐模式：

```yaml
version: "1.0"
assignments:
  - node_selector: { node_id: "mac-01" }
    roles: [Storage, Inference]
  - node_selector: { node_id: "linux-01" }
    roles: [Storage, Execution]
```

### 4.5 内容寻址不可变性

遵循 Git 模型，所有存储对象通过 SHA-256 哈希进行内容寻址。这提供了：

- **去重**：相同内容 → 相同哈希 → 单一存储副本
- **完整性验证**：存储的哈希必须与读取时计算的哈希匹配
- **P2P 同步就绪**：哈希使得高效的内容分发成为可能

## 5. AI 智能体架构（eo-agent）

Python eo-agent 使用 LangGraph 实现了推理+行动（ReAct）工作流：

```
                 ┌──────────┐
                 │ PLANNER  │  ← 解析用户自然语言意图，检查拓扑
                 └────┬─────┘
                      │
                 ┌────▼─────┐
      ┌──────────│  CODER   │  ← 生成/重写执行代码
      │          └────┬─────┘
      │               │
      │          ┌────▼─────┐
      │          │  DEPLOY  │  ← 提交到 CAS + Raft 调度
      │          └────┬─────┘
      │               │
      │          ┌────▼─────┐
      │   ┌──────│ EVALUATE │  ← 获取结果，检查退出码
      │   │      └──────────┘
      │   │
      │   │ 退出码 ≠ 0 且重试 < 3:  循环回到 CODER（自愈）
      │   │ 退出码 = 0 或重试 ≥ 3:  → END
      │   │
      └───┘
```

### 5.1 自愈重试循环

当执行失败（非零退出码）时，评估器将错误反馈给编码器，编码器重写代码以修复问题。此过程自动进行最多 3 次，体现了瞬时错误应在无需人工干预的情况下处理的原则。

## 6. 网络架构

### 6.1 P2P 协议栈

```
┌─────────────────────────────────┐
│ 描述符交换协议                    │  ← 自定义: /edge-orch/descriptor/1.0.0
├─────────────────────────────────┤
│ Identify 协议                    │  ← libp2p: agent 版本、协议列表
├─────────────────────────────────┤
│ Ping 协议                        │  ← libp2p: 保活、延迟探测
├─────────────────────────────────┤
│ mDNS 发现                        │  ← libp2p: LAN 节点发现 (UDP 5353)
├─────────────────────────────────┤
│ Yamux 多路复用                    │  ← TCP 上的流多路复用
├─────────────────────────────────┤
│ Noise XX 握手                    │  ← 认证加密
├─────────────────────────────────┤
│ TCP 传输                         │  ← 可靠流传输
└─────────────────────────────────┘
```

### 6.2 发现流程

1. 节点 A 生成 Ed25519 密钥对 → 派生出 PeerId
2. 节点 A 绑定 TCP 监听器 → mDNS 广播 `_ipfs-discovery._udp` 服务
3. 节点 B 的 mDNS 浏览器发现该广播
4. 节点 B 发起 TCP 连接 → Noise 握手 → Identify 交换
5. 节点 B 通过 `/edge-orch/descriptor/1.0.0` 请求描述符
6. 节点 A 回复完整的 `NodeDescriptor`（能力、角色、操作系统）
7. 双方现在拥有完整的对等节点元数据 → Raft 可以吸收新节点

## 7. 数据流：端到端任务执行

```
1. 用户输入自然语言提示 → eo-agent REPL
2. eo-agent Planner → 调用 get_cluster_topology 工具 → IPC → Rust 节点
3. Planner → 生成结构化执行计划 (JSON)
4. Coder → 从计划生成代码 (Python/Wasm)
5. Deploy → base64 编码代码 → submit_to_cas_and_raft → IPC → Rust 节点
6. Rust IPC handler → 计算 SHA-256 哈希 → 将 blob 存入 CAS
7. Rust IPC handler → 构造 ScheduledTask → 向 Raft 提案
8. Raft 共识 → 复制任务 → 提交 → ClusterState.task_queue
9. 编排调度器 → 出队任务 → 按策略路由到执行器
10. 执行器节点 → 从 CAS 获取代码 blob → sandbox.execute_code()
11. Sandbox → 捕获 stdout/stderr/exit_code/timing
12. 将 ExecutionResult 作为 blob 存入 CAS → Raft CompleteTask 提案
13. eo-agent Evaluator → fetch_execution_result → 检查 exit_code
14. exit_code = 0 → 向用户展示 final_answer
15. exit_code ≠ 0 → 错误反馈给 Coder 进行重试（最多 3 次）
```

## 8. 设计决策与权衡

| 决策 | 理由 | 代价 |
|----------|-----------|-----------|
| **基础设施用 Rust** | 内存安全、零成本抽象、异步运行时 | 更陡的学习曲线、更长的编译时间 |
| **AI 智能体用 Python** | 丰富的 LLM 生态（LangChain/LangGraph）、快速迭代 | 独立的运行时、IPC 开销 |
| **Raft 而非其他共识算法** | 久经考验（tikv）、清晰的领导者选举 | 比 gossip 协议更复杂 |
| **Git 模型 CAS 而非数据库** | 不可变性、去重、P2P 就绪 | 无 SQL 查询能力、GC 复杂 |
| **Wasm 沙箱而非纯 Docker** | 跨平台、轻量、确定性 | 系统调用受限、无 GPU 访问 |
| **UDS IPC 而非 HTTP** | 零网络开销、文件系统权限 | 仅限单机、无远程访问 |
| **mDNS 发现而非静态配置** | LAN 环境下零配置 | 跨子网不可用、企业 AP 隔离 |

## 9. 测试策略

### 9.1 Rust：单元测试 + 集成测试

- **51 个单元测试** 覆盖 8 个 crate，涵盖序列化往返、状态机确定性、哈希碰撞抵抗、沙箱隔离、路由正确性、错误处理
- **4 个 P2P 集成测试** 在 localhost 上使用真实 libp2p swarm

### 9.2 Python：基于 Mock 的测试

- **14 个测试** 覆盖完整图遍历、路由逻辑、重试耗尽、工具错误处理
- **Mock IPC 客户端** 使得无需运行 Rust 节点即可测试

### 9.3 CI 流水线

- GitHub Actions: Ubuntu + macOS 矩阵
- Rust: `cargo fmt --check` → `cargo clippy -- -D warnings` → `cargo test --workspace` → `cargo build --release`
- Python: `pytest tests/ -v` 配合 `EO_MOCK_MODE=true`

## 10. 演进路线图

| 阶段 | 范围 | 状态 |
|-------|-------|--------|
| 0.1.0 | 核心 P2P、Raft、CAS、沙箱、IPC、eo-agent | ✓ 已实现 |
| 0.2.0 | 多节点 Raft、P2P blob 同步、持久化身份 | 计划中 |
| 0.3.0 | 容器运行时（完整）、GPU 推理、WASI preview 2 | 计划中 |
| 0.4.0 | 跨子网对等连接、静态加密、认证令牌 | 计划中 |
| 1.0.0 | 动态 Raft 成员变更、生产加固、指标仪表盘 | 计划中 |
