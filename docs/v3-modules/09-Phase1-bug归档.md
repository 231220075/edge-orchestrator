# Phase 1 开发 bug 归档（Rust async + 平台门控经验）

> 背景：QleanSandbox 的 Linux 真实现，在 macOS 开发机上因 cfg 门控不编译，
> 三个 bug 直到真实 Linux 上 cargo build 才逐个暴露。本文归档根因与解法。

## Bug 1：qlean 的 API 是 async，被当成同步调用

- 现象：Linux 上 cargo build 报 E0599 map_err/map 不存在（Image::new 返回 impl Future）。
- 根因：qlean::Image::new、qlean::with_machine 都是 async fn 返回 Future；
  代码按同步 Result 调 .map_err()/.map()。mac 上该分支被 cfg(not linux) 排除，从不编译。
- 解法：所有 qlean async API 前加 .await；QleanSandbox::new 改为 async fn；
  ProjectSandbox::run_project 改为 async trait 方法，并在 trait 上加
  #[allow(async_fn_in_trait)] 抑制公共 trait async fn 的 clippy lint。

## Bug 2：括号结构被部分编辑破坏

- 现象：cargo build 报 unclosed delimiter。
- 根因：用 python 按字符串替换函数时，end 锚点误匹配到外层 impl 的闭合括号，
  删多了或漏了花括号。
- 解法：重写整个函数块时，先 cat -n 看清行号，按行号切片替换；替换后再 sed 检查括号配对。

## Bug 3（核心）：async move 闭包 + 外部变量 = partial move

- 现象：Linux 上 E0382 use of partially moved value: out；
  并伴随一堆 unused assignment 警告（out.exit_code 赋值被覆盖）。
- 根因：ExecutionResult out 定义在闭包外，闭包用 async move 捕获并按字段修改，
  闭包外再用 res.map(|_| out) 整体读取 -> Vec 字段被 move，整体不可用。
- 解法（最佳实践）：在 with_machine 闭包内「构造并返回最终结果」，
  闭包 Output 直接是 ExecutionResult，外部只 await 拿值。不要闭包外定义可变结果再 share。

## 第四次踩同一个坑（2025-09，VM 池/模板镜像那轮）

- 现象：`error[E0728]: await is only allowed inside async functions and blocks`
  —— worker 是同步函数，我却写了一个 `async { ... }` 块并以 `.await` 结尾（漏了 `block_on`）。
  同一轮里还有一次把循环尾部整段覆盖掉（池动作 + `job.reply.send` 消失）。
- 为什么本地全绿：`mod linux` 整个模块带 `#[cfg(target_os = "linux")]`，
  macOS 上连编译都不编译；`cargo fmt`/`clippy`/`test` 同样看不到。
  甚至 `cargo check -p sandbox --target x86_64-unknown-linux-gnu` 也走不通
  （qlean → libssh2 的 cc-rs 需要 Linux 的 C 工具链）。
- 应对（已固化）：
  1. `scripts/preflight.sh::target_platform_build_check` 在跑任何验证前执行
     `cargo check --workspace --all-targets`，非 Linux 平台会明确打印"跳过"；
  2. 结构上减少出错面：worker（同步）只做「调 `block_on` 包住的 async 阶段函数」，
     async 拆成 `ensure_image` / `boot_machine` / `run_on_machine` 三个独立阶段，
     既避免跨 await 持有 `&mut machine`，也让"漏 block_on"这种错误一眼能看出；
  3. 结论仍是同一条：**目标平台的编译是唯一可信门禁**。

## 第五次：连续 6 个提交把 CI 推红（2025-09，方案 A 收尾）

- 事实：从 CAS 分发那轮起，**每一次 push 的 CI 都是 failure**，而我完全没看 CI，
  只看本地 `fmt`/`clippy`/`test`（三者都看不到 Linux 门控代码）。失败原因逐个不同：

| 提交 | CI 真实失败原因 |
|---|---|
| CAS 分发 | `-D warnings` 下 `cas_fetch` 的 `get_local`/`put`/`pending_fetches` 是 dead code |
| 隔离模式 | 同上（另一组方法） |
| VM 池 | `error[E0728]`：同步函数里写了 `async {}.await`（漏 `block_on`） |
| 模板镜像 | `error[E0061]`：`make_project_executor` 调用点少了两个参数 |
| block_on 修复 | 同一个 E0061（修复没覆盖到调用点）|
| CAS 清理 | `error[E0599]`：测试里还残留 `pending_fetches()`（rustfmt 拆行导致替换漏掉）|

- 应对（已落地）：
  1. **把 CI 当作 Linux 编译门禁**：`gh run watch <id> --exit-status` 在 push 后确认；
     ubuntu job 会 `cargo clippy --workspace --all-targets -- -D warnings` + `cargo test`，
     这两步覆盖了所有 `#[cfg(target_os = "linux")]` 代码；
  2. 本地无法起 Linux 容器（docker daemon 未运行）、交叉 target 也走不通
     （qlean → libssh2 需要 Linux C 工具链），所以 CI 是目前唯一可靠通道；
  3. 后续凡改动 Linux 门控文件（`qlean.rs`、`cas_fetch.rs`、`bootstrap.rs` 的 Linux 段、
     `project_executor.rs`），**必须看到 CI 绿再报告完成**。

- 教训（比技术更重要）：**"本地全绿"在跨平台项目里几乎等于没验证**。
  要把 CI 结果当作验收证据，而不是把"我这边过了"当结论。

## 核心经验（3 条）

1. 平台 cfg 门控的代码（cfg(target_os)，feature）开发机不编译，必须推到目标平台 cargo build 验证；
2. async 库的 API 先查签名区分 Future 与 Result；不确定就 await；
3. 异步闭包共享可变状态优先「闭包内构造+返回」，避免 async move + 外部可变捕获。

## 附：测量方法论（三次「测错」换来的规则）

本项目在验证隔离时**连续三次**得到「看起来正确、实则空洞」的结论：

| 次数 | 错误 | 为什么看起来是对的 |
|---|---|---|
| 1 | 标记写在 `work_dir` 里 | `execute_on_vm` 两种模式都 `rm -rf work_dir`，reuse 也报「已消失」 |
| 2 | 探针变量带 `M=` 前缀（`'M=/root/...'`） | bash 试图执行名为 `M=/root/...` 的文件，标记从未写入，读回必然「消失」 |
| 3 | fresh 对照集群用了 `--listen-address` 改端口 | bootstrap_peers 仍指向旧端口 → 集群没形成，`no remote node with project_sandbox capability` |

三次都不是"程序错了"，而是**测量设计错了**。固化的规则：

1. **每个结论必须有一个能失败的对照**：只有「隔离成立」不算证据，还必须证明
   「不隔离时会失败」——本次就是 reuse 组 `MARKER-SURVIVED` 对 fresh 组 `MARKER-GONE`，
   同一探针、同一路径、唯一变量是模式；
2. **payload 必须能被肉眼核对**：`bash -n` 抓不到 shell 元字符泄漏（规则 2 就是这么漏过去的），
   所以给脚本加了 `EO_DRY_RUN=1`，打印真实 project 目录与 build 命令；
3. **读被观测系统的日志**：三次都是靠 executor 打印的 `build=[...]` 数组才定位到问题——
   自建脚本的"成功"输出不可信，被观测方的原始记录才可信；
4. **瞬时失败不要写进结论**：一次 `syntax error` 之后同命令复跑 4 次全部正常，
   记为疑似瞬时问题而不是缺陷（也已用生成器复现 `bash -n` 通过）。
