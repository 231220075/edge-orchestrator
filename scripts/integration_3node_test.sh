#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[1/5] build"
cargo build -p node --quiet

echo "[2/5] start 3 nodes"
rm -rf /tmp/eo-it
mkdir -p /tmp/eo-it
for i in 1 2 3; do
  RUST_LOG=info ./target/debug/node --config configs/cluster-node-$i.yaml --store-dir /tmp/eo-it/n$i --ipc-socket /tmp/eo-it/n$i.sock > /tmp/eo-it/n$i.log 2>&1 &
done
sleep 10

echo "[3/5] leader check"
LEADER=$(grep -h RAFT_LEADER /tmp/eo-it/n1.log /tmp/eo-it/n2.log /tmp/eo-it/n3.log | head -1)
echo "  $LEADER"
test -n "$LEADER" || { echo FAIL-no-leader; exit 1; }

# NOTE: this script used to submit a Wasm task to prove "submit -> raft
# schedule -> execute -> result". That execution backend was removed (see
# docs/v3-modules/22-wasm-lane移除记录.md); the replicated log now carries node
# registration and role changes, so this script verifies exactly that. Project
# task execution is covered by scripts/verify_project_e2e.sh and
# scripts/verify_agent.sh.
echo "[4/5] replicated state after startup (node registration)"
grep -h "Node registered" /tmp/eo-it/n1.log | tail -3 || true

echo "[5/5] kill leader and re-elect"
LEADER_ID=$(echo "$LEADER" | sed "s/.*raft_id=//")
pkill -f "cluster-node-$LEADER_ID" || true
sleep 10
NEW=$(grep -h RAFT_LEADER /tmp/eo-it/n1.log /tmp/eo-it/n2.log /tmp/eo-it/n3.log | tail -1)
echo "  $NEW"
test -n "$NEW" || { echo FAIL-no-re-election; exit 1; }
echo PASS
pkill -f cluster-node || true
