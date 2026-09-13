# Phase 2 完成：master 提交项目 -> server 沙箱执行 -> 结果回传

## 已完成

- Capabilities.project_sandbox：声明式能力，master 据此路由（不选无能力节点）。
- ProjectClient（master 端）：目录打包 tar -> 解析目标执行节点（显式 target 或首个具备 project_sandbox 的远程节点）-> 发送 ProjectTask -> 记录回传结果。
- QleanProjectExecutor（server 端, Linux）：解包 tar 到临时目录（目录名对齐 work_dir 的 basename）-> QleanSandbox 执行 build/run -> 返回 ProjectResult。
- bootstrap：Linux 节点注入 QleanProjectExecutor；集群成员创建 ProjectClient；ProjectResultReceived 写入结果表。
- IPC：submit_project（本地目录+命令 -> task_id）与 fetch_project_result（按 task_id 查结果）。
- 单测：路由解析、打包并发送任务（3 个）。

## Linux 端到端验证

一键脚本：

    ./scripts/verify_project_e2e.sh

手工等价步骤：

1. 构建：cargo build -p node --example submit_project
2. 起 3 节点（configs/cluster-node-{1,2,3}.yaml, 均 project_sandbox: true）。
3. 提交项目（示例客户端会轮询结果）：

    ./target/debug/examples/submit_project /tmp/eo-proj/n1.sock scripts/qlean-project-demo/testproj "apt-get update -qq && apt-get install -y -qq gcc && gcc main.c -o app" "./app"

4. 预期：submit 返回 task_id；result 状态 completed，stdout base64 解码为 project-hello，exit_code 0。

## 诚实边界

- 执行是同步 request-response：master 发任务后等待 server 执行完成再返回；长任务需异步化（后续）。
- 快照以 tar_bytes 内嵌在协议里（上限 64MB）；大项目应改为先推 CAS 只发 hash（已预留 ProjectSnapshot.hash）。
- QleanSandbox 的超时检查是命令之间的软检查，不能中断单条长时间命令（如 apt 安装）；硬超时需 watchdog + 杀 VM。
- 每次任务新建 VM（无池化），首启 + 工具链安装慢；生产应预热 MachinePool + 预装工具链镜像。
