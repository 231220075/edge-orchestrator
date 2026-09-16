# 快照 CAS 分发：把 workspace 从协议里请出去

> 背景（方案 A 第 1 项）：`ProjectTask.snapshot.tar_bytes` 原本把整个 workspace 内嵌在
> request-response 消息里（上限 64MB）。这让「大项目」直接不可用，也让协议承担了存储的活儿。
> 本文记录改造后的数据流、实测证据、以及仍然存在的边界。

## 1. 改造前 vs 改造后

| | 改造前 | 改造后 |
|---|---|---|
| 消息内容 | `tar_bytes` 全量内嵌 | **只有 `hash`** |
| 大小上限 | 协议 64MB（硬上限） | 无（受 CAS 磁盘限制） |
| 谁持有字节 | 双方各一份（消息里） | master 的 CAS；executor 按需拉取并缓存 |
| 失败表现 | 超限直接发不出去 | 拉取失败有明确原因（无 peer / 超时） |

`ProjectSnapshot` 现在是 hash-first：`hash` + 兼容用的 `tar_bytes`（新代码留空）。
`sender` 打包后写入自己的 CAS，只把 hash 放进任务；`receiver` 先查本地 CAS，
命中直接用（同机/重复提交场景零网络），未命中就**向已知 peer 拉取**。

## 2. 为什么必须用 pull（而不是 push）

代码实测：blob 协议只有 `BlobRequest → BlobResponse` 一个方向，
**没有上传/推送**（`grep PutBlob|UploadBlob|PushBlob` 为空）。
所以 executor 必须主动要，这带来一个实现上的坑：

> 事件循环处理 `BlobRequest` 时无法同步取回数据——它只能把请求丢给 swarm，
> 然后**等**。而"等"不能发生在事件循环里（否则整节点冻结，这正是之前修过的坑）。

解法是 `crates/node/src/cas_fetch.rs` 的 `BlobFetcher`：
- 调用方 `ensure_blob(hash, timeout)`：先查本地 CAS → 未命中则登记一个 waiter（oneshot）
  → 向每个已知 peer 发 `RequestBlob` → 等 waiter 或超时；
- swarm 事件循环收到 `BlobResponseReceived` 后调 `on_blob_received`：写入 CAS 并唤醒 waiter；
- 超时是**显式**的（默认 300s，可传参），而不是无限等待——「永远 pending」是本项目反复踩过的坑。

## 3. 顺带修掉的配置问题

`blob_exchange` 的 request-response 超时原来是默认值 **10s**：对"传分片代码"够用，
对"传几十 MB 的工程快照"远远不够，会在半路静默失败。现在提到 **900s**，
与 project 协议一致。

## 4. 实测证据

单测（`cargo test -p node cas_fetch`，4 个）：

| 测试 | 覆盖 |
|---|---|
| `local_hit_needs_no_network` | 本地命中不联网 |
| `missing_blob_without_peers_fails_fast_and_clearly` | 无 peer 时**立即**给出可读错误，不留下 waiter |
| `arriving_blob_wakes_the_waiter_and_lands_in_cas` | 响应到达 → 唤醒等待者 **且**写入 CAS |
| `fetch_timeout_reports_the_wait_not_a_silent_pending` | 超时是可诊断错误，而不是静默 pending |

端到端（`scripts/diagnose_cluster.sh` 新增 stage F）：构造一个 **~4MB** 的 workspace 提交，
日志里应出现：

```
project <id>: snapshot <hash> ready locally (N bytes)      # executor 侧：已可用
cas: requesting blob <hash> from raft <n> (<peer>)        # 若是首次，会看到拉取
cas: blob <hash> fetched from <peer> (N bytes)
```

判定要点：**executor 的日志里 `ready locally` 的字节数应等于 workspace 打包大小**，
而 `cas: blob ... fetched` 出现即证明走的是 CAS 分发而非内嵌。

## 5. 仍然存在的边界

- 快照在 master 的 CAS 里**没有回收策略**（`gc` 未接入）：反复提交会累积 tar blob；
- 拉取是"向所有已知 peer 广播请求、先到先用"：多 peer 同时持有同一 hash 时会重复传输
  （简单但浪费；正确做法是记录"谁有"或改成分块/去重传输）；
- 没有完整性校验：`BlobCodec` 只传 hash 与 bytes，接收端不校验内容是否等于 hash
  （CAS 的 `put_blob` 会按内容重新算 hash 存储，但请求的 hash 与实际内容不匹配时不会报错）；
- `tar_bytes` 兼容字段还在协议里，旧 peer 仍可内嵌发送（executor 两种都能处理）。
