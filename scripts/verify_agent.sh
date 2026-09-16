#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

LOG_DIR=/tmp/eo-agent
cleanup() { pkill -f "cluster-node-" 2>/dev/null || true; }
trap cleanup EXIT

dump_logs() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    echo
    echo "==================== FAILED (exit $status) ===================="
    for f in "$LOG_DIR"/n*.log; do
      [[ -f "$f" ]] || continue
      echo "--- $f (tail 25) ---"
      tail -25 "$f"
    done
    echo "=============================================================="
    echo "Full logs kept in $LOG_DIR (this script no longer deletes them)."
    echo "Where the pipeline stops tells you which layer broke:"
    echo "  no 'project task ... accepted'  -> request never reached an executor"
    echo "  accepted but no 'qlean: ...'    -> executor blocked before the sandbox"
    echo "  'qlean boot/upload/build timed out' -> VM or guest network problem"
  fi
}
trap 'dump_logs' EXIT

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/preflight.sh
source "$SCRIPT_DIR/preflight.sh"

echo "[0/5] preflight (Linux + KVM + qemu + bridge helper + host network)"
preflight_network "$SCRIPT_DIR" || preflight_abort_if_broken
[[ "$(uname -s)" == "Linux" ]] || { echo "FATAL: needs Linux (qlean is KVM-only)"; exit 1; }
[[ -e /dev/kvm ]] || echo "WARN: /dev/kvm missing - VM boot will fail"
[[ -r /dev/kvm && -w /dev/kvm ]] || echo "WARN: no rw access to /dev/kvm - is $USER in the kvm group?"
command -v qemu-system-x86_64 >/dev/null || echo "WARN: qemu-system-x86_64 not found"
if command -v getcap >/dev/null && command -v qemu-bridge-helper >/dev/null; then
  getcap "$(command -v qemu-bridge-helper)" || echo "WARN: qemu-bridge-helper has no cap_net_admin+ep"
fi
[[ -f /etc/qemu/bridge.conf ]] && grep -q allow /etc/qemu/bridge.conf \
  || echo "WARN: /etc/qemu/bridge.conf missing an 'allow' line (bridge networking will fail)"

echo "[1/5] cleanup"
cleanup
sleep 1

echo "[2/5] build gate + build"
target_platform_build_check "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
cargo build -p node --quiet
cargo build -p eo-agent --quiet

echo "[3/5] start 3 executors"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"
for i in 1 2 3; do
  # info + p2p logs: the swarm/executor lifecycle messages needed for triage.
  RUST_LOG=info,p2p=debug ./target/debug/node \
    --config "configs/cluster-node-$i.yaml" \
    --store-dir "$LOG_DIR/n$i" \
    --ipc-socket "$LOG_DIR/n$i.sock" > "$LOG_DIR/n$i.log" 2>&1 &
done
sleep 12

echo "[4/5] run eo-agent against the sample C project (heuristic planner)"
./target/debug/eo-agent run --workspace scripts/qlean-project-demo/testproj --socket "$LOG_DIR/n1.sock"

echo "[5/5] evidence"
grep -h "project result" "$LOG_DIR"/*.log | tail -3 || true
grep -h "project task .* accepted" "$LOG_DIR"/*.log | tail -3 || true
