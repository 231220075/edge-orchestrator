# Phase 2 端到端验证通过（真实 Linux + KVM）

## 验证结果（实测）

- submit_project 经 UDS 提交本地目录 -> task_id；
- master 打包 tar 并经 libp2p 发 ProjectTask 到远程 executor；
- executor 启动 KVM 虚机、上传项目、apt 装 gcc、编译 main.c、运行；
- 结果回传 master：status=completed, exit_code=0, execution_time_ms=129440, stdout 结尾为 project-hello。

## 关键修复（本轮）

1. libp2p request-response 默认请求超时 10s，项目执行需分钟级 -> project 协议超时改为 1800s（本轮超时的根因）；
2. 路由选执行节点时排除自身，避免拨号自己；
3. executor 解包目录名对齐 work_dir 的 basename，匹配 qlean upload 的 mirror 语义；
4. verify 脚本启动前清理旧节点 + trap EXIT 清理；
5. build_cmd 加 DEBIAN_FRONTEND=noninteractive，消除 debconf 无 TTY 噪音。

## 已知性能问题（下一阶段优化）

- 每次任务新建 VM + 装工具链，约 129s；应改为 MachinePool 预热 + 预装工具链镜像；
- stdout 会带上 apt 安装日志（几百 KB），agent 分析时需截断/过滤；
- 执行仍是同步 request-response，长任务需异步化。
