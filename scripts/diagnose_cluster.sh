#!/usr/bin/env bash
# Layered triage for "the project pipeline hangs with status=pending".
#
# It replaces guesswork with staged evidence: environment -> request arrival ->
# VM boot -> toolchain -> build/run. Each stage is timed, so the slowest (or
# stuck) layer is obvious from the output.
#
# Usage (on the Linux+KVM host):
#   ./scripts/diagnose_cluster.sh            # full run
#   ./scripts/diagnose_cluster.sh --keep     # leave nodes running afterwards
set -uo pipefail
cd "$(dirname "$0")/.."

LOG_DIR=/tmp/eo-diag
KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

cleanup() { pkill -f "cluster-node-" 2>/dev/null || true; }
[[ $KEEP -eq 0 ]] && trap cleanup EXIT

section() { echo; echo "==================== $* ===================="; }

section "0. environment"
echo "kernel      : $(uname -sr)"
echo "user/groups : $(id -un) / $(id -Gn)"
if [[ -e /dev/kvm ]]; then
  echo "/dev/kvm    : present ($(ls -l /dev/kvm))"
  [[ -r /dev/kvm && -w /dev/kvm ]] && echo "              rw OK" || echo "              NO rw ACCESS -> join the kvm group"
else
  echo "/dev/kvm    : MISSING -> qlean cannot boot a VM here"
fi
echo "qemu        : $(command -v qemu-system-x86_64 || echo MISSING)"
if command -v qemu-system-x86_64 >/dev/null; then
  qemu-system-x86_64 --version | head -1
fi
if command -v qemu-bridge-helper >/dev/null; then
  echo "bridge ctl  : $(getcap "$(command -v qemu-bridge-helper)" 2>/dev/null || echo 'no capabilities')"
fi
[[ -f /etc/qemu/bridge.conf ]] && echo "bridge.conf : $(tr '\n' ' ' < /etc/qemu/bridge.conf)" \
                              || echo "bridge.conf : MISSING"
echo "stale nodes : $(pgrep -af 'target/debug/node' | wc -l | tr -d ' ')"
echo "stale qemu  : $(pgrep -c qemu-system 2>/dev/null || echo 0)"
if [[ -n "$(pgrep -af 'target/debug/node' || true)" ]]; then
  echo "WARNING: old node processes are alive; killing them (they hold /dev/kvm and the mesh ports)"
  cleanup; sleep 1
fi

section "1. build"
cargo build -p node --quiet || { echo "FATAL: node build failed"; exit 1; }
cargo build -p node --example submit_project --quiet || { echo "FATAL: example build failed"; exit 1; }

section "2. start 3 executors (RUST_LOG=info,p2p=debug)"
rm -rf "$LOG_DIR"; mkdir -p "$LOG_DIR"
for i in 1 2 3; do
  RUST_LOG=info,p2p=debug ./target/debug/node \
    --config "configs/cluster-node-$i.yaml" \
    --store-dir "$LOG_DIR/n$i" \
    --ipc-socket "$LOG_DIR/n$i.sock" > "$LOG_DIR/n$i.log" 2>&1 &
done
sleep 12
echo "waiting for the mesh to form..."
for i in 1 2 3; do
  echo "n$i: $(grep -c 'Connection established' "$LOG_DIR/n$i.log" || true) established connections, \
$(grep -c 'Mapped raft id' "$LOG_DIR/n$i.log" || true) raft-id mappings"
done
if ! grep -q "Mapped raft id" "$LOG_DIR/n1.log"; then
  echo "WARN: n1 has no raft-id -> PeerId mapping; project routing will fail (resolve_peer)."
  grep -E "Descriptor|dial|Dial" "$LOG_DIR/n1.log" | tail -5
fi

PROJ=scripts/qlean-project-demo/testproj
submit() {  # submit <label> <build_cmd>
  local label="$1" build="$2"
  section "3. $label"
  local start=$SECONDS
  ./target/debug/examples/submit_project "$LOG_DIR/n1.sock" "$PROJ" "$build" "./app" || true
  echo "elapsed: $((SECONDS - start))s"
  echo "--- executor-side evidence ---"
  grep -hE "project task .* (accepted|finished)|qlean:" "$LOG_DIR"/n*.log | tail -12
  echo "--- master-side evidence ---"
  grep -hE "dispatched to peer|result received" "$LOG_DIR"/n*.log | tail -4
}

# Stage A: no toolchain work at all -> isolates "VM boot + upload + exec".
# Expected: exit=1 with "./app: No such file or directory" on stderr, because
# build_cmd=true never produces an app. (Before, a signal-killed command was
# reported as exit 0.)
submit "stage A: VM boot + upload + exec only (build_cmd = true)" "true"

# Stage B: is the guest able to reach the outside world at all? A failing/hanging
# apt-get is the most common reason this pipeline looks stuck, and `-qq` hides
# its output, so probe explicitly.
#
# No `timeout` binary is assumed (minimal cloud images lack it): `run_t` is a
# watchdog subshell. NOTE: `TMO=90 run_t ...` is correct, but `TMO=90 run_t() {}`
# is NOT valid bash — a function definition cannot follow an assignment prefix.
GUEST_LIB='run_t() { "$@" & p=$!; ( sleep ${TMO:-60}; kill -9 $p 2>/dev/null ) & w=$!; wait $p; s=$?; kill $w 2>/dev/null; return $s; }; apt_bin_update() { DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::IndexTargets::deb-src::DefaultEnabled=false update -qq; }; apt_install() { DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$@"; }'

submit "stage B: guest network probe (dns + tcp + apt, watchdogs)" \
  "$GUEST_LIB; echo '--- ip'; ip -4 addr show | grep -E 'inet |state'; echo '--- default route'; ip route | head -3; echo '--- resolv.conf'; cat /etc/resolv.conf; echo '--- ping gw'; TMO=10 run_t ping -c1 -W3 10.0.2.2 || true; echo '--- dns'; TMO=20 run_t getent hosts deb.debian.org || echo 'DNS FAILED'; echo '--- tcp 80'; TMO=20 run_t bash -c 'exec 3<>/dev/tcp/deb.debian.org/80' && echo 'TCP OK' || echo 'TCP FAILED'; echo '--- apt-get update, deb-src DISABLED (120s cap)'; TMO=120 run_t apt_bin_update && echo 'APT-BIN UPDATE OK' || echo 'APT-BIN UPDATE FAILED/TIMED OUT'; echo '--- apt-get update, deb-src enabled (120s cap)'; TMO=120 run_t apt-get update -qq && echo 'APT-FULL UPDATE OK' || echo 'APT-FULL UPDATE FAILED/TIMED OUT'; echo '-- probe done --'"

# Stage C: the real plan. No deb-src indexes, apt output visible, bounded so a
# broken guest fails fast instead of burning the whole task budget.
BUILD="(command -v gcc >/dev/null 2>&1 || (TMO=180 run_t apt_install gcc && echo gcc-installed)) && gcc main.c -o app"
submit "stage C: install gcc (deb-src off, 180s cap) + compile + run" "$GUEST_LIB; $BUILD"

# Stage D: exactly what eo-agent's heuristic planner now generates.
submit "stage D: eo-agent planner build_cmd" \
  "$GUEST_LIB; (command -v gcc >/dev/null 2>&1 || (DEBIAN_FRONTEND=noninteractive apt-get install -y -qq gcc >/dev/null 2>&1 || (DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::IndexTargets::deb-src::DefaultEnabled=false update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq gcc))) && gcc main.c -o app"

section "4. where did it stop?"
for f in "$LOG_DIR"/n*.log; do
  echo "--- $f ---"
  grep -E "project task|qlean|Outbound project|Inbound project|panicked|ERROR" "$f" | tail -20
done
echo
echo "Logs kept in $LOG_DIR."
[[ $KEEP -eq 1 ]] && echo "Nodes left running (--keep); clean up with: pkill -f cluster-node-"
