# M3 补强：P2P 数据面 blob 分发

> 状态：blob 协议 codec + swarm 服务端/客户端已接线，集成测试通过。

## 实现

- 协议 /edge-orch/blob/1.0.0 的消息：BlobRequest{hash} / BlobResponse{hash, found, data}。
- p2p::BlobProvider trait：node 用 CasBlobProvider 包装本地 CAS，注入 new_swarm。
- swarm 收到 request 时同步查 provider 并 send_response，同时发出 BlobRequestReceived 事件供观测。
- node 侧收到 BlobResponseReceived 时把远端 blob 写入本地 CAS（跨节点补齐）。
- SwarmCommand::RequestBlob 让执行节点主动向有数据节点拉取。

## 测试证据

- p2p_integration test blob_is_served_between_two_swarms：节点 1 持有 blob，节点 2 bootstrap 直连并请求，校验取回内容一致。

## 诚实边界

- executor_loop 尚未接入 RequestBlob 拉取路径（任务仍走 code_inline 内联）；把 executor 缺失时拉取远程 blob 作为下一步。
