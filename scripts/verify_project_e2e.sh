#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

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

echo "[3/4] submit project (first run installs gcc in the guest; may take minutes)"
PROJ=scripts/qlean-project-demo/testproj
./target/debug/examples/submit_project /tmp/eo-proj/n1.sock "$PROJ" "apt-get update -qq && apt-get install -y -qq gcc && gcc main.c -o app" "./app"

echo "[4/4] evidence"
grep -h "project result" /tmp/eo-proj/n1.log /tmp/eo-proj/n2.log /tmp/eo-proj/n3.log | tail -3 || true
pkill -f "cluster-node-" || true
