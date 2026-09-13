#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

cleanup() { pkill -f "cluster-node-" 2>/dev/null || true; }
trap cleanup EXIT

echo "[0/4] cleanup"
cleanup
sleep 1

echo "[1/4] build node + eo-agent"
cargo build -p node --quiet
cargo build -p eo-agent --quiet

echo "[2/4] start 3 executors"
rm -rf /tmp/eo-agent
mkdir -p /tmp/eo-agent
for i in 1 2 3; do
  RUST_LOG=info ./target/debug/node --config configs/cluster-node-$i.yaml --store-dir /tmp/eo-agent/n$i --ipc-socket /tmp/eo-agent/n$i.sock > /tmp/eo-agent/n$i.log 2>&1 &
done
sleep 12

echo "[3/4] run eo-agent against the sample C project (heuristic planner)"
./target/debug/eo-agent run --workspace scripts/qlean-project-demo/testproj --socket /tmp/eo-agent/n1.sock

echo "[4/4] evidence"
grep -h "project result" /tmp/eo-agent/*.log | tail -3 || true
