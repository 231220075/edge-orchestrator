# Agent 层：eo-agent（读工作区 -> 规划 -> 提交 -> 分析）

## 做了什么

新增 crates/agent（二进制 eo-agent）：把「一句话/一个工作区 -> 在集群上构建运行 -> 看懂结果」做成一条命令。

## 工作流（确定性骨架 + 两处可选 LLM）

1. scan_workspace：递归列出工作区文件（跳过 .git/target/node_modules，深度<=3，最多 200 个）；
2. plan：
   - 未配置 LLM：启发式规则判断项目类型，生成 build_cmd/run_cmd/work_dir；
   - 配置了 LLM：把文件列表 + 目标发给 LLM，要求只返回 JSON {build_cmd, run_cmd, work_dir}；
3. submit：经节点 IPC（UDS JSON-RPC）调用 submit_project，轮询 fetch_project_result 直到 completed；
4. analyze：
   - 未配置 LLM：退出码 0 则报告成功 + 最后几行 stdout；否则报告失败 + stderr 尾部；
   - 配置了 LLM：把结果交给 LLM 用 2-3 句总结是否成功；
5. 失败重试（有界，默认 2 次）：若配置了 LLM，让其根据错误修订命令后重跑。

## 启发式规则

- Cargo.toml -> cargo build --release / cargo run --release；
- 单个顶层 .c 文件 -> gcc X.c -o app / ./app；
- Makefile -> make / ./app；
- 单个 .py -> python3 X.py；
- package.json -> npm install / node index.js；
- 缺工具时在 build_cmd 里先 apt 安装（DEBIAN_FRONTEND=noninteractive），适配全新的 Debian guest。

## LLM 接入方式

- 环境变量：EO_LLM_BASE_URL（OpenAI 兼容 base，如 http://host/v1）、EO_LLM_API_KEY、EO_LLM_MODEL；
- 通过 curl 调 POST {base}/chat/completions，temperature=0，解析 choices[0].message.content；
- 不引入 HTTP 依赖（reqwest/ureq 首次拉取依赖太慢），用系统 curl，保持零新依赖。

## 遇到的问题

1. 引入 ureq 后 cargo 依赖解析/下载超时（环境网络受限）-> 改用 curl，去掉新依赖；
2. 启发式原先 Makefile 优先，但示例工程的 Makefile 没有 run 目标 -> 调整为先判断单个顶层 .c 用 gcc，Makefile 的 run 用 ./app；
3. Rust 生成的多处细节（chr(47)/chr(10) 泄漏、宏占位）通过“目标平台/编译验证”逐个修掉。

## 验证

- 单测：extract_json、三种启发式（cargo/make/single-c）；
- Linux 端到端：scripts/verify_agent.sh（3 执行节点 + eo-agent 启发式模式跑示例 C 工程）。

## 诚实边界

- LLM 路径未在本环境实测（无 API key），仅代码就绪；
- 重试策略简单（最多 N 次，仅 LLM 模式会修订命令）；
- agent 只读工作区，不回写；不做多智能体编排（遵循 AGENT_PLAN.md 的结论）。


## 复核与修复（第二次检查）

对 agent 做了全面复核，发现并修复：

1. CLI 形状与调用不一致：脚本用 eo-agent run --workspace，但原 CLI 是扁平参数。已改为标准子命令（Cli + Commands::Run(RunArgs)），并提供 --help；
2. 工作区路径未规范化：agent 直接把相对路径发给节点，节点按自己的 CWD 解析 -> 已改为 canonicalize 成绝对路径，并在不存在/非目录时提前报错；
3. 缺少真实 IPC 交互测试：新增 mock UDS 节点测试，覆盖 submit_project + 两次 fetch（pending -> completed）+ base64 解码断言；
4. 长 socket 测试路径在 macOS 超过 SUN_LEN -> 测试改用 /tmp 短路径。

说明：本项目设计下，agent 的 --socket 必须指向“与 agent 同主机”的节点（同机直连或 Mac 上的轻客户端节点），因为项目目录由该节点打包。


## 第三轮修复：轮询无输出 + LLM 配置机制

### 「卡住」的初判（已被第四轮推翻）
第三轮的判断是：未配置 LLM 时不会调用 LLM（走启发式），所以不是 LLM 卡住；真正原因是 submit_and_wait 每 3s 轮询一次、期间不打印任何东西，而首次任务要冷启动 VM + 装 gcc（约 90s），看起来像卡死。

修复（保留，日志仍然有用）：
- 提交后打印 task_id 与说明；
- 每 15s 打印一次 still running... Ns；
- 30 分钟超时后给出明确错误。

**但这条结论只对冷启动 90s 那一次成立。** 后续实测出现 540s+ 仍未完成、且 `verify_project_e2e.sh`（不经过 agent）同样卡在 pending，说明问题不在 agent，见 `20-项目执行卡死排查.md`。

### LLM API Key 配置机制
- 优先级：环境变量 > 配置文件；
- 支持 EO_LLM_BASE_URL / EO_LLM_API_KEY / EO_LLM_MODEL，以及 OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL 别名；
- 配置文件默认 ~/.config/eo-agent/config.env（KEY=VALUE），可用 EO_AGENT_CONFIG 覆盖；
- 若配置文件对 group/other 可读会打印告警并提示 chmod 600；
- key 只通过 curl 参数传递，不写入日志（启动日志只显示 key=set/none）；
- 模板见 configs/eo-agent.env.example；
- 本地无鉴权端点（如 Ollama）可留空 api_key。

### 新增 --dry-run
只打印计划不提交，便于区分「计划问题」和「执行问题」（本地即可验证，无需集群）。

## 第四轮修复：把「pending 到永远」变成可定位的失败

背景：submit_project 返回 task_id 只代表「本地打包 + 寻址 + 入队」成功，不代表任务发出去了，更不代表有人在跑。链路上有 6 处静默失败路径，全部表现为一个 `pending`。详见 `20-项目执行卡死排查.md`。

本轮改动：

1. **agent 识别终态失败**：`fetch_project_result` 现在可能返回 `status=failed` + `error`，agent 立刻带原因退出，不再轮询到 30 分钟；轮询输出也带上 status；
2. **执行器不再静默丢包**：`crates/p2p/src/swarm.rs` 中无 project executor 的节点会回 `exit_code=101` 的失败结果（原来直接 return，什么都不发），并打出 warn；
3. **执行移出事件循环**：项目请求的 ResponseChannel 登记进 pending map，任务在 detached task 上跑完把结果回投事件循环，由事件循环调用 `send_response`。此前「spawn + 等 oneshot」仍会占住事件循环（ResponseChannel 只是投递给 request-response handler，必须再次 poll swarm 才会写出响应）；
4. **全阶段 tracing**：swarm 侧记录 accepted/finished/输出尾部，sandbox 侧记录 image/boot/upload/build/run 每阶段开始结束与耗时；
5. **执行层超时**：`crates/sandbox/src/qlean.rs` 的 image（300s）、boot（600s）、upload（300s）、build/run 都加了 `tokio::time::timeout`，超时明确报错并丢弃 VM，不再无限等待；
6. **能力与真实执行能力一致**：节点拿不到可用沙箱时，注册前把 `project_sandbox` 降级为 false，避免 master 永远路由到一个跑不了的节点；
7. **master 侧兜底**：`ProjectClient` 记录任务状态（dispatched/failed/done），超过协议窗口（1920s）仍无结果则报 failed，不留永久 pending。


