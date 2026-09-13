# 重构：轻客户端 master（Mac 作为跨机入口）

## 目标

让 Mac（或任意非集群成员）作为 master：本地打包工作区、经 libp2p 把 ProjectTask 发给 Linux 执行节点、收结果。

## 问题

此前 ProjectClient 只从 Raft ClusterState 解析执行节点，只有集群成员（有 raft_id）才有该状态。Mac 若不加入 Raft，就没有拓扑，无法路由。

## 方案：Catalog 抽象

把“如何把目标解析为 PeerId”抽象为 Catalog 两种来源：

- Raft { state, registry }：集群成员用，node_id -> raft_id -> PeerId；
- Peers(map)：轻客户端用，来自 mesh 上交换的 peer descriptor（peer_id -> descriptor）。

resolve_peer 对两种来源都支持：显式 target（按 node_id 匹配）或自动选（具备 project_sandbox 且非自身，按 raft_id 最小确定性选取）。

## 实现要点

- bootstrap 维护 peer_id -> descriptor 表（known_peers），在 DescriptorReceived 时写入；
- 有 raft_id 的节点用 Catalog::Raft，无 raft_id 的轻客户端用 Catalog::Peers；
- ProjectClient 现在总是创建（不再只在集群成员上）；
- 轻客户端仍会：拨 bootstrap peers -> 建连后请求 descriptor -> 学到执行节点能力；
- 发送 ProjectTask 复用同一 request-response 连接，结果原路返回。

## 验证方式

- 同机验证脚本 scripts/verify_light_master.sh：3 个执行节点 + 1 个轻客户端 master（都在本机 loopback），经轻客户端 IPC 提交项目；
- 单测：Catalog::Peers 路径的解析（显式 target 与自动选取）。

## 跨机（Mac -> VMware Linux）网络前提

- 执行节点需监听非 loopback 地址（0.0.0.0）并让 Mac 可达；
- VMware 默认 NAT 下 Mac 无法直连 guest：需要改为桥接（Bridged）或配置端口转发；
- 轻客户端 config 的 bootstrap_peers 要填执行节点的实际 IP:port 与 PeerId。

## 诚实边界

- 轻客户端不做 Raft，因此不参与共识、无 cluster 级任务队列视图（只做点对点 project 提交）；
- 轻客户端依赖 descriptor 交换拿到执行节点；若节点能力变化，需要重新交换 descriptor。


## 实测结果（同机 loopback）

- 3 个执行节点 + 1 个轻客户端 master 同时运行；
- 经轻客户端 IPC 提交项目，master 日志出现 project result 且 exit=0；
- 说明轻客户端成功：拨 bootstrap -> 学 descriptor -> 解析执行节点 -> 发 ProjectTask -> 收结果。
