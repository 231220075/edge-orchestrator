# v3 架构总览

> 单二进制 node；控制面 Raft + 数据面 CAS + libp2p 传输 + 沙箱执行。

## 模块与数据流

```
CLI/IPC(JSON-RPC/UDS) -> node
  node:
    - raft: RawNode + ClusterState(节点表/角色/任务队列/分配表)
    - p2p: mDNS/Noise/Yamux + descriptor/raft 协议
    - orchestration: coordinator_loop(调度/重路由) + executor_loop(执行)
    - sandbox: Wasmtime(真限额) / container(诚实 stub)
    - storage: CAS(blob/tree/commit)
```

## 一次任务的生命周期

1. submit_to_cas_and_raft: code 存 CAS -> propose SubmitTask
2. Raft 提交 -> ClusterState.task_queue
3. coordinator_loop: 分配 -> propose AssignTask(executor_raft_id)
4. 目标节点 executor_loop: 读 code_inline/CAS -> Wasm 执行 -> propose CompleteTask
5. Raft 提交 CompleteTask -> 移出队列 + 记 completed_tasks

## 故障自愈(可演示)

- kill leader -> 剩余节点 election_timeout 内重选主
- 任务超时未完成 -> coordinator 重新 Assign 给存活执行者(at-least-once)

## 关键约束与诚实边界

- 静态 3 节点，成员变更 stub
- blob 分发未接 P2P(用 code_inline 内联)
- container 是文档化 stub
