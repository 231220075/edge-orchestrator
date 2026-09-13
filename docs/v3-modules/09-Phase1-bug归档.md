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

## 核心经验（3 条）

1. 平台 cfg 门控的代码（cfg(target_os)，feature）开发机不编译，必须推到目标平台 cargo build 验证；
2. async 库的 API 先查签名区分 Future 与 Result；不确定就 await；
3. 异步闭包共享可变状态优先「闭包内构造+返回」，避免 async move + 外部可变捕获。
