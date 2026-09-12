# iPhone 通信可行性评估（v2 重写 · 务实结论）

> 结论先行：**「iPhone 作为提交任务的远程触发器」完全可行，但 iPhone 绝不参与 P2P/Raft 拓扑；**
> 建议实现 `LAN HTTP + JSON-RPC 2.0`，与现有 UDS 共用同一套 `JsonRpcHandler`（同一套 submit 逻辑）。
> 推荐将它做成一个**独立的、可裁剪的 M5（可选项）**，不阻塞 M1~M4 主线。

---

## 一、先分清「三种完全不同难度的 iPhone 参与方式」

| 方式 | 技术含义 | 难度 | 结论 |
|---|---|---|---|
| 方式 A：远程触发/查看任务 | iPhone 给某个节点发一条"提交任务"请求，再查询结果 | ★ 低（1~2 天） | **推荐** |
| 方式 B：iPhone 作为 P2P 节点 | iPhone 跑 libp2p，进 mesh，参与 mDNS/Noise | ★★★★ 极高 | 不建议 |
| 方式 C：iPhone 作为 Raft 投票成员 | iPhone 持久化 Raft 日志、参与选举 | ★★★★★ 几乎不可行/无意义 | 坚决不做 |

**你的"跨设备交流"需求本质上只要方式 A**——RUNBOOK 里 iPhone 的角色本来就是 "Event Trigger / Remote CLI / Shortcuts"。
没有任何理由让 iPhone 承担方式 B/C：移动端唤醒不确定、NAT 穿越、无持久化保证、后台被杀，这些都和 Raft 的强一致前提冲突。

### 方式 A 的技术要点（推荐路线）

- 在 Rust `node` 里新增一个 **HTTP 服务器（axum，监听 `0.0.0.0:8970`）**，向上复用现有 `JsonRpcHandler`；
- 与 UDS 的差异 **只在于传输层**（UDS vs TCP），JSON-RPC 方法名、参数、返回结构**完全一致**；
- iPhone 用 **快捷指令 Shortcuts 的「获取 URL 内容」动作**，发一个 POST JSON 即可，**无需写 Swift**；
- 查询任务结果：`get_cluster_topology` / `fetch_execution_result` 也是同一个 HTTP 端点。

### 为什么原始 UDS 路径对 iPhone 不通（说清楚即可）

- `/tmp/eo_control.sock` 是 **Unix Domain Socket，仅存在于 Mac 本机文件系统**；
- iPhone 无法挂载该文件，也不能走 HTTP-over-UDS；
- 移动端也不应持有集群节点身份（无稳定 IP、会休眠、无法被 mDNS 稳定发现）。

---

## 二、方案比对：iPhone 触发这条端到底怎么连

> 下表只讨论「方式 A（远程触发）」的实现选型；方式 B/C 直接淘汰（理由见上）。

| 方案 | 原型 | 延迟 | iPhone 侧成本 | Rust 侧成本 | 演示前风险 | 推荐 |
|---|---|---|---|---|---|---|
| **HTTP + JSON-RPC（TCP）** | axum/warp | 低（局域网 <5ms） | 零代码（Shortcuts） | 低（约 150 行） | 极低 | ✅ **首选** |
| mDNS 广播触发 | 简单 UDP | 低但需可靠投递 | 低 | 中（要写重试） | 高（丢包） | 次选 |
| 轻量 CoAP | CoAP server | 低 | 需 App | 中 | 中 | 不考虑 |
| iPhone 跑 libp2p | go-libp2p via Swift bindings | — | 极高 | 极高 | 极高 | ❌ 淘汰 |
| 公网穿透（Tailscale/内网穿透） | 第三方 | 低 | 低 | 无 | 依赖网络 | 作为「不在同一 WiFi」时的备选 |

**选型结论**：先做 `HTTP + JSON-RPC`（同一 Wi-Fi 内够用），演示时如果 iPhone 和 Mac 不在同一网段，再用 **Tailscale** 兜底；
这条链路本质上和你本地 UDS 是**同一个 handler**，不引入任何新的分布式复杂度。

### iPhone 触发样例（Shortcuts 无需写代码）

```
POST http://<mac-ip>:8970/rpc
Content-Type: application/json
{"jsonrpc":"2.0","id":"1","method":"submit_to_cas_and_raft","params":{"code":"<base64>","required_runtime":"Wasm","routing":"AnyExecutor","timeout_ms":30000}}
```

> 只需要在 Shortcuts 里把"文本"填成上面 JSON，行动选择"获取 URL 内容 > POST > JSON"。

---

## 三、可行性总评

| 维度 | 结论 |
|---|---|
| 要不要做 | 要做，但只做「触发 + 查结果」，不做拓扑参与 |
| 难度 | 低（1~2 天，即 M5 可选项） |
| 是否会引入分布式复杂度 | 不会，HTTP 端点与 UDS 共用同一个 JsonRpcHandler |
| 是否阻塞主线 | 不阻塞，放 M4 之后做 |
| 何时可以暂时去掉 | **若时间紧张，可在 v3 交付时暂时去掉 iPhone，用 Mac 本机 CLI 充当触发器；架构上预留 `Gateway` 层，后续再加 HTTP 端点即可** |

### 代码落点建议（v3 规划）

```
crates/node/src/
  gateway/
    mod.rs        # 定义 trait TaskSubmitter（UDS 与 HTTP 都实现）
    uds_server.rs # 现有 IpcServer 重构为 gateway 的一个 impl
    http_server.rs# axum 端点，复用 JsonRpcHandler（新增，约 150 行）
```

---

## 四、一句话总结

- iPhone 参与方式只选「远程触发」；HTTP+JSON-RPC 是性价比最高的路径，且与现有 UDS 逻辑完全复用。
- 如果在演示质量与工期之间取舍：**先保证 M3 自愈 demo，iPhone 链路做成可裁剪的 M5**；去掉 iPhone 不影响任何分布式/沙箱亮点。
