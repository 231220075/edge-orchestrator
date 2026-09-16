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

## 核心经验（3 条）

1. 平台 cfg 门控的代码（cfg(target_os)，feature）开发机不编译，必须推到目标平台 cargo build 验证；
2. async 库的 API 先查签名区分 Future 与 Result；不确定就 await；
3. 异步闭包共享可变状态优先「闭包内构造+返回」，避免 async move + 外部可变捕获。
