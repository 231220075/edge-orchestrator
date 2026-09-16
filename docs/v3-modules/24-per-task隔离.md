# 每任务隔离：reuse 与 fresh 两种模式

> 背景（方案 A 第 2 项）：我们此前在**复用同一个 VM** 的模式下踩过一个真实的隔离事故——
> 一次被超时 `kill -9` 的 `apt` 把 `/var/lib/apt/lists/lock` 留在系统盘里，
> 之后每个任务都在毫秒级 `Could not get lock` 失败（exit 228）。这不是 bug，是**设计代价**。

## 1. 先澄清一个误解：qlean 本来就是「每台 machine 一个独立盘」

读 `qlean::Machine::new` 的实现可以看到：

```rust
qemu-img create -f qcow2 -b <base-image> -F qcow2 <run_dir>/overlay.img
```

每台 machine 都有自己的 **qcow2 overlay**，写操作只落在自己那层。
所以「每任务独立系统盘」**不需要我们自己实现 overlay**——只需要**每任务一台 machine**。
我们踩的坑不是因为共享镜像，而是因为**复用了同一台 machine**。

| | reuse（默认） | fresh |
|---|---|---|
| machine 生命周期 | 常驻 1 台，跨任务复用 | 每任务新建、任务结束即丢弃 |
| 系统盘 | 同一 overlay（任务可见彼此痕迹） | 每个任务独立 overlay |
| 热任务耗时 | **~60ms**（实测） | 每次 +~14s（VM 启动）+ 首次装工具链 |
| 工具链 | 装一次永久可用 | 基础镜像没有工具链时**每次都要装**（分钟级） |
| 典型风险 | 状态泄漏（锁残留、`/root/project` 残留） | 无跨任务泄漏 |

## 2. 配置与代码位置

```yaml
capabilities:
  project_sandbox: true
  # reuse = 一台热 VM 跨任务复用（快，任务共享磁盘）
  # fresh = 每任务新 machine（隔离，每次多 ~14s 启动）
  project_vm_mode: reuse
```

- 解析与校验：`crates/node/src/config.rs` 的 `VmMode::parse`（拼错不会被猜，回退 reuse 并 warn）；
- 语义实现：`crates/sandbox/src/qlean.rs` 的 worker 循环——
  `fresh` 模式下任务结束后 `machine.shutdown()` 并 `machine = None`，日志打 `machine discarded (fresh mode)`；
- 启动可见：节点启动时打印 `Project sandbox policy: enabled=…, vm_mode=…`，
  这样"为什么我的任务行为不一样"有据可查。

## 3. 怎么验证

`scripts/diagnose_cluster.sh` 新增 stage G：连续提交两个任务，第一个写
`/root/project/marker-from-task-1`，第二个去 `ls` 它。

| 观察 | 结论 |
|---|---|
| probe #2 打印 `MARKER-GONE` | 走了 fresh 模式，**每任务隔离成立** |
| probe #2 列出了 marker 文件 | reuse 模式，任务共享 overlay（当前默认，符合预期） |

单测覆盖：`VmMode::parse` 的两种取值与拼写错误、`allows_reuse()` 的语义、
节点配置里 `project_vm_mode` 的解析与回退（`crates/node/src/config.rs` 测试）。

## 4. 为什么默认仍是 reuse（诚实说明）

- 热任务 60ms vs 每次 14s，差 200+ 倍；
- **fresh 模式只有在基础镜像自带工具链时才实用**：否则每个任务都要 `apt install`，
  实测一次带换源的 gcc 安装 ~96s，两个任务就是 3 分钟；
- 所以"隔离 + 快"必须靠**预装工具链的镜像模板**（`ImageConfig.source/digest` 已支持指向自定义镜像），
  那是下一步工作，而不是把默认值改成 fresh。

## 5. 仍然存在的边界

- fresh 模式**每任务都会重新走一遍 image prepare**（实测 31s 首次、之后几十毫秒），
  若镜像未缓存则任务代价更高；
- 我们没有做 **VM 池**（预热 N 台、取用即换），因此 fresh 模式无法同时"隔离 + 免启动等待"；
  池化是让 fresh 可用的关键前提；
- reuse 模式下即使清了 apt 锁，任务仍能看到上一个任务的 `/root/project` 残留内容
  （`execute_on_vm` 开头有 `rm -rf work_dir`，但只覆盖 work_dir）；
- 隔离仅覆盖**可写磁盘**：CPU/内存限额、网络访问目前都没有强制（guest 可自由出网），
  要真正做到"沙箱"还需要 cgroup/网络策略，见阶段总结里的待办。
