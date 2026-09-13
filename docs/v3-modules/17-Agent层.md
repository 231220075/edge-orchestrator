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
