#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

cleanup() { pkill -f "cluster-node-" 2>/dev/null || true; }
trap cleanup EXIT

echo "[0/4] cleanup old nodes"
cleanup
sleep 1

echo "[1/4] build"
cargo build -p node --quiet
cargo build -p node --example submit_project --quiet

echo "[2/4] start 3 nodes"
rm -rf /tmp/eo-proj
mkdir -p /tmp/eo-proj
for i in 1 2 3; do
  RUST_LOG=info ./target/debug/node --config configs/cluster-node-$i.yaml --store-dir /tmp/eo-proj/n$i --ipc-socket /tmp/eo-proj/n$i.sock > /tmp/eo-proj/n$i.log 2>&1 &
done
sleep 12

PROJ=scripts/qlean-project-demo/testproj
BUILD="(command -v gcc >/dev/null 2>&1 || (DEBIAN_FRONTEND=noninteractive apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq gcc)) && gcc main.c -o app"

echo "[3/4] submit #1 (cold: boots the VM, first run installs gcc)"
./target/debug/examples/submit_project /tmp/eo-proj/n1.sock "$PROJ" "$BUILD" "./app"

echo "[3b/4] submit #2 (warm: reuses the booted VM)"
./target/debug/examples/submit_project /tmp/eo-proj/n1.sock "$PROJ" "$BUILD" "./app"

echo "[4/4] evidence"
grep -h "project result" /tmp/eo-proj/n1.log /tmp/eo-proj/n2.log /tmp/eo-proj/n3.log | tail -3 || true
