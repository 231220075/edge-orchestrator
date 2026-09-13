# Phase 2 在 Linux 上的验证清单

前置：qlean demo 已通过（QEMU 6.2 + KVM + bridge 正常）。

## 验证 1：主项目编译（含 qlean 真实现）

cd edge-orchestrator
cargo build -p sandbox -p node

期望：无错误（Linux 分支会真正编译 QleanSandbox 与 QleanProjectExecutor）。

## 验证 2：project 端到端 demo（tar -> qlean 编译运行）

cd scripts/qlean-project-demo
cargo run --release

期望：打印 stdout=project-hello 与 RESULT: PASS。
这条验证 Phase 2 完整数据链：目录 tar 打包 -> 临时解包 -> qlean upload -> make -> run -> 回传。

## 验证 3（可选）：p2p project 协议集成测试

集成测试已在 mac 上通过（mock executor）。真实链路需后续 Phase 3 把 QleanProjectExecutor 注入 new_swarm。
