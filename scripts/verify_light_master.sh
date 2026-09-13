#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

cleanup() { pkill -f "cluster-node-" 2>/dev/null || true; pkill -f "light-master.yaml" 2>/dev/null || true; }
trap cleanup EXIT

echo "[0/5] cleanup old nodes"
cleanup
sleep 1

echo "[1/5] build"
cargo build -p node --quiet
cargo build -p node --example submit_project --quiet

echo "[2/5] start 3 executors"
rm -rf /tmp/eo-light
mkdir -p /tmp/eo-light
for i in 1 2 3; do
  RUST_LOG=info ./target/debug/node --config configs/cluster-node-$i.yaml --store-dir /tmp/eo-light/n$i --ipc-socket /tmp/eo-light/n$i.sock > /tmp/eo-light/n$i.log 2>&1 &
done
sleep 12

echo "[3/5] start light master (no raft_id, resolves executors from descriptors)"
RUST_LOG=info ./target/debug/node --config configs/light-master.yaml --store-dir /tmp/eo-light/master --ipc-socket /tmp/eo-light/master.sock > /tmp/eo-light/master.log 2>&1 &
sleep 8

echo "[4/5] submit via light master IPC"
PROJ=scripts/qlean-project-demo/testproj
BUILD="(command -v gcc >/dev/null 2>&1 || (DEBIAN_FRONTEND=noninteractive apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq gcc)) && gcc main.c -o app"
./target/debug/examples/submit_project /tmp/eo-light/master.sock "$PROJ" "$BUILD" "./app"

echo "[5/5] evidence"
grep -h "project result" /tmp/eo-light/*.log | tail -3 || true
