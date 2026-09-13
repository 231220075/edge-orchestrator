# M3 后续：qlean 集成 · 阶段 1 记录

> 目标：master(任意环境,agent入口) -> 服务发现 -> server(Linux+KVM+qlean) -> 沙箱编译运行项目 -> 结果回传。
> 本阶段：引入 qlean 依赖 + ProjectSandbox 抽象 + QleanSandbox 最小骨架，mac 可编译、Linux 上可跑通编译运行链路。

## 决策（已确认）

1. qlean 作为 crates.io 依赖（qlean = "0.3"），不 clone 仓库；
2. 镜像策略：先用 qlean 默认 Debian cloud 镜像跑通，再做预装工具链镜像；
3. 不拆二进制：用 roles + capabilities 区分 master(coordinator) 与 server(execution+qlean)。

## 本阶段改动

- core/types.rs：新增 ProjectSpec（snapshot_hash + work_dir + build_cmd + run_cmd + timeout + limits）；
- core/traits.rs：新增 ProjectSandbox trait（run_project）；
- sandbox/qlean.rs：QleanSandbox 骨架 —— Linux 上用 qlean::with_machine 依次 exec build/run，收集 stdout/stderr/exit/time；非 Linux 是 UnsupportedPlatform stub；
- sandbox/Cargo.toml：qlean 依赖，配置 cfg(target_os = "linux") 门控，mac 上不链接 KVM 依赖。

## 验收

- mac 上 cargo build -p sandbox / test / clippy / fmt 全绿；
- ProjectSpec + ProjectSandbox 已导出。

## 下一步（阶段 2）

- 在 VMware Ubuntu 上 cargo test 验证 qlean 官方 test 通过；
- 补 upload 项目目录（tar）进 VM + 从 CAS 读 snapshot；
- 加 build/run 超时看门狗（qlean exec 无命令级超时）；
- 预装工具链镜像或 first-boot apt 方案。

## 阶段 1 收尾（真实 Linux 验证后补充）

- ProjectSpec 新增 local_project_dir（Phase 1 本地目录路径，snapshot_hash 留 Phase 2 跨节点）；
- QleanSandbox.run_project 增加：目录 upload 到 work_dir（按 qlean upload 的 mirror 语义上传到 parent）、build/run 用 cd work_dir 进入、手动超时判断（qlean exec 无超时）；
- 独立 demo scripts/qlean-upload-demo 验证 upload 目录语义；
- Linux 验证清单见 08-Phase1-Linux验证清单.md。

