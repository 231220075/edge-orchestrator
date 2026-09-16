#!/usr/bin/env bash
# Layered triage for "the project pipeline hangs with status=pending".
#
# Env overrides (single source of truth for a non-default setup):
#   EO_MIRRORS="https://my.mirror/debian http://deb.debian.org/debian"
#   EO_BRIDGE=qlbr0
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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/preflight.sh
source "$SCRIPT_DIR/preflight.sh"

section "0a. preflight (mandatory: a broken host network makes every guest check lie)"
preflight_network "$SCRIPT_DIR" || preflight_abort_if_broken

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
submit() {  # submit <label> <build_cmd> [project_dir]
  local label="$1" build="$2" proj="${3:-$PROJ}"
  section "3. $label"
  local start=$SECONDS
  ./target/debug/examples/submit_project "$LOG_DIR/n1.sock" "$proj" "$build" "./app" || true
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
GUEST_LIB='run_t() { "$@" & p=$!; ( sleep ${TMO:-60}; kill -9 $p 2>/dev/null ) & w=$!; wait $p; s=$?; kill $w 2>/dev/null; return $s; }; reap() { pkill -9 -x apt-get 2>/dev/null; pkill -9 -x http 2>/dev/null; sleep 1; rm -f /var/lib/apt/lists/lock /var/lib/dpkg/lock /var/lib/dpkg/lock-frontend; }; apt_bin_update() { DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::IndexTargets::deb-src::DefaultEnabled=false update -qq; }; apt_install() { DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$@"; }; pick_mirror() { MIRROR=""; for m in ${EO_MIRRORS:-https://mirrors.tuna.tsinghua.edu.cn/debian https://mirrors.ustc.edu.cn/debian https://mirrors.aliyun.com/debian http://deb.debian.org/debian}; do if curl -sf -o /dev/null --max-time 8 "$m/dists/trixie/Release" || curl -sf -o /dev/null --max-time 8 "$m/dists/stable/Release"; then MIRROR="$m"; break; fi; done; if [ -n "$MIRROR" ]; then for f in /etc/apt/sources.list /etc/apt/sources.list.d/*.sources; do [ -f "$f" ] || continue; sed -i.bak -E "s#https?://(deb|security|ftp)[.]debian[.]org/debian(-security)?#$MIRROR#g" "$f" && rm -f "$f.bak"; done; echo "apt mirror: $MIRROR"; else echo "apt mirror: none reachable"; fi; }'

submit "stage B: guest network probe (dns + tcp + apt, watchdogs)" \
  "$GUEST_LIB; reap; echo '--- ip'; ip -4 addr show | grep -E 'inet |state'; echo '--- default route'; ip route | head -3; echo '--- resolv.conf'; cat /etc/resolv.conf; echo '--- ping gw'; TMO=10 run_t ping -c1 -W3 10.0.2.2 || true; echo '--- dns'; TMO=20 run_t getent hosts deb.debian.org || echo 'DNS FAILED'; echo '--- tcp 80'; TMO=20 run_t bash -c 'exec 3<>/dev/tcp/deb.debian.org/80' && echo 'TCP OK' || echo 'TCP FAILED'; echo '--- NIC and route sanity (apt lies: it exits 0 even when NOTHING resolves)'; if ip route | grep -q '^default'; then echo 'ROUTE OK'; else echo 'ROUTE MISSING -> guest has no usable NIC: check qlbr0 on the HOST'; fi; if TMO=25 run_t curl -sf -o /dev/null http://deb.debian.org/debian/dists/trixie/Release; then echo 'HTTP REACHABLE'; echo '--- apt-get update, deb-src DISABLED, 300s cap'; TMO=300 run_t apt_bin_update && echo 'APT-BIN UPDATE OK' || echo 'APT-BIN UPDATE FAILED/TIMED OUT'; reap; else echo 'HTTP UNREACHABLE -> skipping apt verdicts (they would be meaningless)'; fi; echo '-- probe done --'"

# Stage C: MIRROR SPEED, measured in the guest. The image defaults to
# deb.debian.org; on a slow international link the ~10 MB index alone takes
# minutes, which is the real reason this pipeline looks stuck.
submit "stage C: debian mirror speed from inside the guest" \
  "$GUEST_LIB; for m in mirrors.tuna.tsinghua.edu.cn mirrors.ustc.edu.cn mirrors.aliyun.com mirrors.cloud.tencent.com deb.debian.org; do \
     u=\"http://\$m/debian/dists/trixie/main/binary-amd64/Packages.xz\"; \
     line=\$(curl -s -o /dev/null -w 'http=%{http_code} bytes=%{size_download} time=%{time_total}s speed=%{speed_download}B/s' --max-time 30 \"\$u\"); \
     echo \"\$m \$line\"; \
   done; \
   echo '--- sources format on this image'; ls /etc/apt/sources.list /etc/apt/sources.list.d/ 2>/dev/null; head -6 /etc/apt/sources.list.d/*.sources 2>/dev/null || head -6 /etc/apt/sources.list 2>/dev/null"

echo
echo "--- same measurement from the HOST (decides whether a local cache/prewarmed image is worth it) ---"
for m in $(echo "${EO_MIRRORS:-https://mirrors.tuna.tsinghua.edu.cn/debian https://mirrors.ustc.edu.cn/debian https://mirrors.aliyun.com/debian http://deb.debian.org/debian}" | tr ' ' '\n' | sed -E 's#https?://##; s#/debian$##'); do
  printf '%-32s ' "$m"
  curl -s -o /dev/null -w 'http=%{http_code} bytes=%{size_download} time=%{time_total}s speed=%{speed_download}B/s
' \
    --max-time 30 "http://$m/debian/dists/trixie/main/binary-amd64/Packages.xz" || echo "unreachable"
done

# Stage D: the real plan. No deb-src indexes, apt output visible, bounded so a
# broken guest fails fast instead of burning the whole task budget.
BUILD="(command -v gcc >/dev/null 2>&1 || (reap; pick_mirror; TMO=600 run_t apt_install gcc)) && gcc main.c -o app"
submit "stage D: install gcc (fast mirror, deb-src off, 600s cap) + compile + run" "$GUEST_LIB; $BUILD"

# Stage E: exactly what eo-agent's heuristic planner now generates.
submit "stage E: eo-agent planner build_cmd (mirror switch + lock cleanup)" \
  "$GUEST_LIB; (command -v gcc >/dev/null 2>&1 || (reap; pick_mirror; apt_install gcc || (reap; apt_bin_update || true; apt_install gcc))) && gcc main.c -o app"

# Stage F: snapshot distribution through the CAS. The master sends only the
# hash; the executor must pull the bytes. A ~4 MB workspace makes that visible in
# the logs ("snapshot ... ready locally" vs a fetch through the blob protocol).
BIG=$(mktemp -d)
cp scripts/qlean-project-demo/testproj/main.c scripts/qlean-project-demo/testproj/Makefile "$BIG"/ 2>/dev/null || true
head -c 4000000 /dev/urandom > "$BIG/blob.bin"
submit "stage F: CAS snapshot distribution (~4 MB workspace)" "true" "$BIG"
echo "--- snapshot path evidence (master packs it, executor pulls it) ---"
grep -hE "snapshot .* ready locally|cas: blob .* fetched|cas: requesting blob" "$LOG_DIR"/n*.log | tail -6
rm -rf "$BIG"

section "4. where did it stop?"
for f in "$LOG_DIR"/n*.log; do
  echo "--- $f ---"
  grep -E "project task|qlean|Outbound project|Inbound project|panicked|ERROR" "$f" | tail -20
done
echo
echo "Logs kept in $LOG_DIR."
[[ $KEEP -eq 1 ]] && echo "Nodes left running (--keep); clean up with: pkill -f cluster-node-"
