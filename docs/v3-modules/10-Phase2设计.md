# Phase 2 设计：项目快照 + 跨节点 project 执行协议

## 目标

master(任意环境) 把「一个项目目录」打包、存 CAS、通过 P2P 让 server(Linux+KVM) 获取并解包，
再由 QleanSandbox 编译运行，返回 ProjectResult。

## 复用现有资产

- CAS：put_blob/get_blob（存任意二进制，content-addressed）；
- blob 协议 /edge-orch/blob/1.0.0：RequestBlob(hash) / BlobResponse(hash,found,data)，
  跨节点按 hash 拉二进制，响应事件 BlobResponseReceived 已落地；
- 节点路由：Raft ClusterState 有 nodes + roles + assigned_tasks；
- QleanSandbox：run_project(ProjectSpec) 已实现目录 upload + build/run + 超时。

## 新增数据模型（core/types.rs）

1. ProjectSnapshot {
     hash: Hash,            // 项目 tar 的 CAS hash
     tar_bytes: Vec<u8>,    // Phase 2 简化：直接把 tar 字节带上（避免先手动推 CAS）
   }

2. ProjectTask {
     task_id: TaskId,
     snapshot: ProjectSnapshot,
     work_dir: String,      // 沙箱内解包目录，如 /root/project
     build_cmd: Vec<String>,
     run_cmd: Vec<String>,
     timeout_ms: u64,
     resource_limits: ResourceLimits,
     pinned_node: Option<NodeId>,
   }

3. ProjectResult {
     task_id: TaskId,
     exit_code: i32,
     stdout: Vec<u8>,
     stderr: Vec<u8>,
     execution_time_ms: u64,
     executed_on: NodeId,
   }

## 新增协议 /edge-orch/project/1.0.0

- Request：ProjectTask；
- Response：ProjectResult（server 同步执行完返回；Phase 2 简化，不引入异步轮询）。

## 传输链路

master 端：
  目录 -> tar -> ProjectSnapshot{hash, tar_bytes} -> project 协议发给目标 server。
server 端（Linux）：
  收到 ProjectTask -> 把 snapshot.tar_bytes put 进本地 CAS（或直接解包到临时目录）
  -> 用 QleanSandbox.run_project 执行（local_project_dir 指向临时解包目录）
  -> 组装 ProjectResult 回传。

## Phase 2 明确简化（诚实边界）

- 不引入异步轮询：request-response 同步等待执行完成，适合分钟级以内任务；
- tar_bytes 内嵌在请求里（最小可用）；后续真大项目再走「先推 CAS + 只发 hash」。
- 不新增流式 stdout：ProjectResult 一次性带回。
