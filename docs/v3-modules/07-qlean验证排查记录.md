# qlean demo 验证：问题排查记录（可借鉴）

> 背景：在 VMware Ubuntu 上验证 qlean（QEMU/KVM 隔离库）能否启动 KVM 虚机、编译并运行 C 程序。
> 最终结果：RESULT: PASS。

## 逐层遇到的问题与解法

### 1. rustup 安装卡在 downloading installer
- 现象：curl sh.rustup.rs 只显示 info downloading installer 后无进展。
- 判断：网络下载慢或被干扰。
- 方法：用国内镜像环境变量 RUSTUP_DIST_SERVER=RUSTUP_UPDATE_ROOT 指向清华/中科大；或先 curl 测连通。

### 2. failed to connect to hypervisor /var/run/libvirt-sock permission denied
- 现象：qlean 定义 qlbr0 网络失败。
- 根因：当前用户不在 libvirt 与 kvm 组。
- 解法：sudo usermod -aG libvirt kvm 加组后重新登录（newgrp 或重开终端）。

### 3. QEMU exited with error code Some(1)，runs 目录无日志
- 现象：QEMU 在创建日志前就退出。
- 排查路线：分层看 CPU 标志 / KVM / 各 QEMU 参数 / 完整命令前台跑。
- 关键结论：单独参数（accel kvm、-cpu host、vsock、bridge）都停得住，误导为参数没问题；真正原因是完整命令前台跑才暴露 bridge tun 权限错误。

### 4. 系统源混源导致 QEMU 版本过旧（4.2.1）
- 现象：系统 jammy 22.04 但 /etc/apt/sources.list 是 focal 20.04 源，装上 QEMU 4.2.1。
- 解法：备份 sources.list 后 sed 把 focal 全局改成 jammy；删除失效 Jacob PPA；apt update 后 qemu-img 升级到 6.2。

### 5. failed to create tun device: Operation not permitted (bridge helper failed)
- 现象：完整 QEMU 命令前台跑，报 bridge helper failed。
- 根因：qemu-bridge-helper 的 cap_net_admin 没生效，或 /etc/qemu/bridge.conf 没有 allow qlbr0。
- 解法：chmod u-s 去掉 suid 再 setcap cap_net_admin+ep；mkdir -p /etc/qemu 后写 allow qlbr0；确认 getcap 有输出。修复后 demo 通过。

## 核心经验

1. 底层隔离类库排查要「前台跑真实命令」，不要背景加 kill（Stopped 会掩盖秒崩与真实错误）；
2. KVM 可用（/dev/kvm 存在）不等于 QEMU 完整参数可用，需逐步叠加参数并看前台错误；
3. QEMU 版本很重要：20.04 的 4.2 无法满足 qlean；系统与源版本一致（jammy）才能拿到 6.2+；
4. 网络桥的权限分三层：helper 二进制 cap、bridge.conf allow、用户组，缺一不可。
