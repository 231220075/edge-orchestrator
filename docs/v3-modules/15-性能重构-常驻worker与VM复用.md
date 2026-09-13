# 性能重构：常驻 worker + VM 复用

## 做了什么

把 QleanSandbox 从「每次任务新建并销毁 VM」改为「常驻 worker 线程持有一个已启动 VM，跨任务复用」。

## 为什么慢（129s/任务）

耗时构成（约 129s）：

1. with_machine 每次都 Machine::new + init()，其中 init() 是 cloud-init 首次启动，最慢（数十秒到上百秒）；
2. guest 里 apt-get update + install gcc，约 30-60s；
3. 编译 + 运行本身不到 1s。

所以瓶颈是「VM 冷启动 + 现场装工具链」，而这两件事都只需做一次。

## 如何做

- QleanSandbox 现在只持有一个 std::sync::mpsc 的 Job 发送端；
- new() 时启动一条常驻线程 qlean-worker，线程内建 current_thread tokio runtime；
- worker 循环里用 rt.block_on(run_job(...)) 处理每个任务，run_job 持有 Option(Image) 与 Option(Machine)；
- 首次任务：Image::new -> Machine::new -> init()（冷启动）；
- 后续任务：检测 machine.is_running()，直接复用同一个 VM 执行 exec/upload；
- 每个任务前 rm -rf work_dir，避免项目残留；
- run_project 仍是同步接口（发 Job、阻塞等 reply），由 p2p 的 spawn_blocking 调用。

## 遇到的坑

1. 依赖是 Linux-only：mac 上 cfg 门控不编译，Linux 编译错误暴露滞后（已固化规则：平台门控代码必须目标平台编译）；
2. qlean 的 Machine 不是 Send（内含 SSH session），所以整个 VM 生命周期不能跨线程，必须封在专用线程 + 专用 runtime；
3. worker 用 std 同步 channel 收任务、tokio runtime 跑 async：线程内 block_on 不能嵌套在 async 上下文，这里用「纯线程 + 反复 block_on」规避；
4. 路由不确定会让热复用失效（第二次可能打到别的节点），改为按 raft_id 最小确定性选执行节点。

## 预期效果

- 冷任务：仍需一次 VM 启动 + 装工具链（首次）；
- 热任务：复用 VM + 已装好的 gcc，预计从约 129s 降到约 1-5s；
- 验证脚本改为连提两次，对比执行耗时。

## 诚实边界

- 单个 worker 串行执行，未做并发（多 VM 池留作后续）；
- 节点进程重启会丢 VM，需重新冷启动；
- 任务执行期间仍会阻塞该节点的 swarm 事件循环（非阻塞化是下一步）。


## 实测结果（Linux + KVM）

| 运行 | execution_time_ms | 说明 |
|---|---|---|
| 冷启动（首次） | 51505 ms | 启动 VM + guest 内 apt 安装 gcc |
| 热复用（第二次） | 68 ms | 复用同一 VM + 已装 gcc（command -v gcc 命中，跳过 apt） |

- 两次任务命中的是同一执行节点（确定性路由生效）；
- 热路径约 68ms，相比冷路径约 51.5s，提速约 750 倍；
- 冷路径也从改造前的约 129s 降到约 51.5s（镜像已缓存 + 少一次不必要的 apt）。
