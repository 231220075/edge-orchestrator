# qlean-demo：在 VMware Ubuntu 上验证 qlean 可行性

这个独立小工程只依赖 qlean，用来证明：
在 Linux 宿主（有 KVM）上，qlean 能启动 VM、安装/使用 gcc、编译 C 并运行、回传结果。

## 前置

1. 确认 KVM 可用：
   kvm-ok -- 期望输出 KVM acceleration can be used
   ls -l /dev/kvm -- 期望存在
2. 装 Rust：rustup 安装后 rustc --version
3. 装 qlean 要求的宿主工具（若 qlean 报错再补）：
   sudo apt install -y qemu-system-x86 qemu-utils qemu-bridge-helper xorriso libvirt-clients libvirt-daemon-system
   sudo chmod u-s /usr/lib/qemu/qemu-bridge-helper
   sudo setcap cap_net_admin+ep /usr/lib/qemu/qemu-bridge-helper
   sudo mkdir -p /etc/qemu && sudo sh -c "echo allow qlbr0 > /etc/qemu/bridge.conf"

## 运行

cd scripts/qlean-demo
cargo run --release

首次运行会下载 Debian cloud 镜像（几百 MB），需要联网和时间。

## 成功标准

看到：
  STEP 5: running compiled binary...
  stdout=hello-from-qlean-vm
  RESULT: PASS

## 失败排查

- 卡在 STEP 1 下载：网络问题，或换镜像源 / 重试；
- STEP 2 卡住超时：vsock 或 bridge 配置问题，回 qlean README 的 host setup 检查；
- gcc 安装失败：guest 无网络，检查 VM bridge 出网；
- 报 KVM unavailable：回到前置第 1 步。
