#!/usr/bin/env bash
# Host-side network self-check and repair for the qlean sandbox.
#
# Why this exists: when the qlbr0 bridge or the bridge helper loses its
# privileges, the guest boots WITHOUT a NIC — `ip addr` shows only `lo`, DNS
# fails, and every apt command still exits 0 with warnings, so the pipeline looks
# "fine" while installing nothing. That is the exact failure mode that cost a
# whole debugging session, so it is checked and repaired automatically here.
#
# Usage:
#   ./scripts/host_network_check.sh          # report only (safe, no changes)
#   sudo ./scripts/host_network_check.sh --fix
#
# Exit code: 0 = healthy (or repaired), 1 = problems remain.
set -uo pipefail

BRIDGE="${EO_BRIDGE:-qlbr0}"
BRIDGE_IP="${EO_BRIDGE_IP:-192.168.221.1/24}"
HELPER="$(command -v qemu-bridge-helper || echo /usr/lib/qemu/qemu-bridge-helper)"
NEED_FIX=0
[[ "${1:-}" == "--fix" ]] && FIX=1 || FIX=0

ok()   { echo "  [ ok ] $*"; }
bad()  { echo "  [FAIL] $*"; NEED_FIX=1; }
info() { echo "  [info] $*"; }

as_root() {
  if [[ $EUID -eq 0 ]]; then "$@"; else sudo "$@"; fi
}

echo "== host network check (bridge=$BRIDGE helper=$HELPER) =="

echo "-- bridge"
if ip link show "$BRIDGE" >/dev/null 2>&1; then
  state=$(cat "/sys/class/net/$BRIDGE/operstate" 2>/dev/null || echo unknown)
  ok "$BRIDGE exists (operstate=$state)"
  if ! ip -4 addr show "$BRIDGE" | grep -q "inet "; then
    if [[ $FIX -eq 1 ]]; then
      as_root ip addr add "$BRIDGE_IP" dev "$BRIDGE" && ok "assigned $BRIDGE_IP to $BRIDGE"
    else
      bad "$BRIDGE has no IPv4 address (guest DHCP will fail)"
    fi
  fi
else
  bad "$BRIDGE is missing (guest gets no NIC at all)"
  if [[ $FIX -eq 1 ]]; then
    as_root ip link add name "$BRIDGE" type bridge && \
    as_root ip addr add "$BRIDGE_IP" dev "$BRIDGE" && \
    as_root ip link set "$BRIDGE" up && ok "created $BRIDGE with $BRIDGE_IP"
  fi
fi

echo "-- bridge helper privileges (three layers must all hold)"
if [[ -x "$HELPER" ]]; then
  ok "helper present: $HELPER"
  caps=$(getcap "$HELPER" 2>/dev/null || true)
  if [[ "$caps" == *cap_net_admin* ]]; then
    ok "capability: $caps"
  else
    bad "helper lacks cap_net_admin+ep (got: '${caps:-none}')"
    if [[ $FIX -eq 1 ]]; then
      # suid must be cleared first, otherwise setcap is ignored
      as_root chmod u-s "$HELPER" && as_root setcap cap_net_admin+ep "$HELPER" && \
        ok "setcap applied: $(getcap "$HELPER")"
    fi
  fi
else
  bad "qemu-bridge-helper not found (install qemu-system-common)"
fi
if [[ -f /etc/qemu/bridge.conf ]]; then
  if grep -qE "^[[:space:]]*allow[[:space:]]+$BRIDGE" /etc/qemu/bridge.conf; then
    ok "/etc/qemu/bridge.conf allows $BRIDGE"
  else
    bad "/etc/qemu/bridge.conf does not allow $BRIDGE"
    [[ $FIX -eq 1 ]] && { echo "allow $BRIDGE" | as_root tee -a /etc/qemu/bridge.conf >/dev/null && ok "appended allow $BRIDGE"; }
  fi
else
  bad "/etc/qemu/bridge.conf is missing"
  [[ $FIX -eq 1 ]] && { echo "allow $BRIDGE" | as_root tee /etc/qemu/bridge.conf >/dev/null && ok "created /etc/qemu/bridge.conf"; }
fi

echo "-- groups"
for g in kvm libvirt; do
  if id -nG | tr ' ' '\n' | grep -qx "$g"; then ok "$USER is in $g"; else
    info "$USER is NOT in $g (needed for /dev/kvm and libvirt)"
    [[ $FIX -eq 1 ]] && as_root usermod -aG "$g" "$USER" && info "added $USER to $g — LOG OUT AND BACK IN for it to take effect"
  fi
done
if [[ -r /dev/kvm && -w /dev/kvm ]]; then ok "/dev/kvm is readable and writable"; else bad "/dev/kvm not rw (KVM boot will fail)"; fi

echo "-- forwarding / NAT (bridge guests need this for outbound traffic)"
if [[ "$(cat /proc/sys/net/ipv4/ip_forward 2>/dev/null)" == "1" ]]; then
  ok "ip_forward=1"
else
  bad "ip_forward=0 -> guest has a link but no route out"
  [[ $FIX -eq 1 ]] && as_root sysctl -w net.ipv4.ip_forward=1 >/dev/null && \
    info "enabled for this boot; make it persistent with /etc/sysctl.d/99-eo-forward.conf"
fi
if command -v nft >/dev/null && nft list ruleset 2>/dev/null | grep -q masquerade; then
  ok "nftables masquerade rule present"
elif iptables -t nat -S 2>/dev/null | grep -q MASQUERADE; then
  ok "iptables MASQUERADE rule present"
else
  info "no MASQUERADE rule found: guest may reach the bridge but not the internet"
fi

echo "-- upstream reachability from the host"
for m in deb.debian.org mirrors.ustc.edu.cn; do
  if curl -sf -o /dev/null --max-time 10 "http://$m/debian/dists/trixie/Release"; then ok "$m reachable"; else bad "$m unreachable from the HOST (then no guest config can help)"; fi
done

echo
if [[ $NEED_FIX -eq 0 ]]; then
  echo "== result: host network looks healthy =="
  exit 0
fi
if [[ $FIX -eq 1 ]]; then
  echo "== result: repairs applied, re-run without --fix to confirm =="
  exit 1
fi
echo "== result: problems found — re-run with --fix (sudo) =="
echo "   sudo $0 --fix"
exit 1
