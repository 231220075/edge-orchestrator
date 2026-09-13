# Phase 1 在 Linux 上的验证清单

qlean demo 已跑通（QEMU 6.2 + KVM + bridge 全部 OK）。接下来验证主项目 sandbox crate 的
QleanSandbox 在 Linux 上能真实编译并跑通 upload 目录 + build/run + 超时。

## 前置

1. 拉主项目（若还没有）：git clone 你的仓库 edge-orchestrator，cd 进去；
2. 装 Rust（已装）；
3. 装 qlean 宿主依赖（qemu 6.2 已装，bridge helper 已配）。

## 验证 1：编译 sandbox crate（含 qlean 真实现）

cd edge-orchestrator
cargo build -p sandbox

期望：无错误。若报 qlean 相关编译错误，把错误贴回。

## 验证 2：upload 目录语义（独立 demo）

cd edge-orchestrator/scripts/qlean-upload-demo
cargo run --release

期望最后打印 RESULT: PASS，content=hello-upload。
这一步验证 QleanSandbox.run_project 里 upload(dir, parent) 的关键路径正确。

## 验证 3（可选）：主项目 QleanSandbox 端到端

后续 Phase 2 前可加一个 node 内测试调 QleanSandbox.run_project 传入本地项目。
