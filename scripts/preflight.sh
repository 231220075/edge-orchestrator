#!/usr/bin/env bash
# Preflight helper: sourced by the verification scripts so no run ever starts on a
# broken host network. Checks, then repairs automatically when it is allowed to.
#
# Behaviour:
#   EO_AUTOFIX=1 (default)  try `sudo -n` first (passwordless); if sudo needs a
#                           password, print the exact command instead of hanging
#                           on a prompt, and stop.
#   EO_AUTOFIX=0            report only, never touch the host.
#
# Exports (for the caller):
#   EO_PREFLIGHT_STATUS = ok | fixed | broken
#   EO_PREFLIGHT_HINT   = human-readable next step when broken
preflight_network() {
  local script_dir="$1"
  EO_AUTOFIX="${EO_AUTOFIX:-1}"

  local check="$script_dir/host_network_check.sh"
  if [[ ! -x "$check" ]]; then
    echo "[preflight] $check not found or not executable — skipping"
    EO_PREFLIGHT_STATUS=ok
    return 0
  fi

  echo "[preflight] host network check (EO_AUTOFIX=$EO_AUTOFIX)"
  if "$check" >/tmp/eo-preflight.log 2>&1; then
    grep -E '^\s+\[(FAIL|info)\]' /tmp/eo-preflight.log | sed 's/^/[preflight]   /' || true
    echo "[preflight] host network healthy"
    EO_PREFLIGHT_STATUS=ok
    return 0
  fi

  # Show what is wrong; the guest cannot be trusted without these.
  grep -E '^\s+\[(FAIL|info)\]' /tmp/eo-preflight.log | sed 's/^/[preflight]   /' || true

  if [[ "$EO_AUTOFIX" != "1" ]]; then
    EO_PREFLIGHT_STATUS=broken
    EO_PREFLIGHT_HINT="host network problems found; re-run with EO_AUTOFIX=1 or fix manually: sudo $check --fix"
    echo "[preflight] FAIL: $EO_PREFLIGHT_HINT"
    return 1
  fi

  if ! sudo -n true 2>/dev/null; then
    EO_PREFLIGHT_STATUS=broken
    EO_PREFLIGHT_HINT="automatic repair needs passwordless sudo. Run: sudo $check --fix"
    echo "[preflight] FAIL: $EO_PREFLIGHT_HINT"
    return 1
  fi

  echo "[preflight] attempting automatic repair"
  if ! sudo -n "$check" --fix 2>&1 | grep -E '\[(ok|info)\]' | sed 's/^/[preflight]   /'; then
    :
  fi
  if "$check" >/dev/null 2>&1; then
    echo "[preflight] repair succeeded"
    EO_PREFLIGHT_STATUS=fixed
    return 0
  fi

  EO_PREFLIGHT_STATUS=broken
  EO_PREFLIGHT_HINT="automatic repair did not fix everything; inspect /tmp/eo-preflight.log and run: sudo $check --fix"
  echo "[preflight] FAIL: $EO_PREFLIGHT_HINT"
  return 1
}

# Abort helper: keep the message in one place.
preflight_abort_if_broken() {
  if [[ "${EO_PREFLIGHT_STATUS:-ok}" == "broken" ]]; then
    echo
    echo "ABORTING: ${EO_PREFLIGHT_HINT}"
    echo "Every guest-side symptom (no NIC, no DNS, apt doing nothing) is a"
    echo "consequence of the host problems listed above."
    exit 1
  fi
}
