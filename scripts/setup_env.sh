#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# setup_env.sh — Edge-Cloud Orchestrator Environment Bootstrap
# ═══════════════════════════════════════════════════════════════════════════════
#
# One-command environment setup for any device joining the cluster.
# Verifies toolchain, diagnoses network topology, auto-configures firewall &
# mDNS, creates storage directories, and validates the Rust + Python workspace.
#
# Usage:
#   ./scripts/setup_env.sh                        # Full interactive setup
#   ./scripts/setup_env.sh --network-only         # Only run network checks + fixes
#   ./scripts/setup_env.sh --check-only           # Dry-run, no mutations
#   ./scripts/setup_env.sh --ci                   # Non-interactive (skip prompts)
#
# What this script auto-fixes (when run without --check-only):
#   - Missing avahi-daemon on Linux       (sudo apt install avahi-daemon)
#   - Firewall blocking mDNS / libp2p     (sudo ufw allow / macOS socketfilterfw)
#   - Stale IPC socket at /tmp/eo_control.sock
#   - Missing eo-agent Python virtualenv
#
# State file written: .eo_network — consumed by run_node_*.sh to avoid re-probing.
#
# Idempotent — safe to run multiple times.

set -euo pipefail

# ── Colour helpers ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
ask()   { echo -e "${CYAN}[ASK]${NC}   $*"; }

# ── Parse flags ──────────────────────────────────────────────────────────────
CI_MODE=false; CHECK_ONLY=false; NETWORK_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --ci)            CI_MODE=true ;;
        --check-only)    CHECK_ONLY=true ;;
        --network-only)  NETWORK_ONLY=true ;;
        *) echo "Unknown argument: $arg" >&2; exit 2 ;;
    esac
done

# ── Platform detection ───────────────────────────────────────────────────────
IS_MACOS=false; IS_LINUX=false
case "$(uname -s)" in
    Darwin) IS_MACOS=true ;;
    Linux)  IS_LINUX=true ;;
    *)      error "Unsupported platform: $(uname -s)"; exit 1 ;;
esac

# ── Paths ────────────────────────────────────────────────────────────────────
EO_HOME="${EO_HOME:-$HOME/.eo_storage}"
EO_IPC_SOCKET="${EO_IPC_SOCKET:-/tmp/eo_control.sock}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
NETWORK_STATE_FILE="$REPO_DIR/.eo_network"
HAVE_SUDO=false
sudo -n true 2>/dev/null && HAVE_SUDO=true || true

# ── Helpers ──────────────────────────────────────────────────────────────────
have() { command -v "$1" >/dev/null 2>&1; }

sudo_maybe() {
    # Run with sudo if we have passwordless sudo, otherwise run bare (will fail loudly if priv needed)
    if $HAVE_SUDO; then sudo "$@"; else "$@"; fi
}

ensure_sudo() {
    if ! $HAVE_SUDO; then
        ask "This step needs root privileges. Grant sudo access:"
        sudo -v
        HAVE_SUDO=true
    fi
}

# ── Step 1: Dependency check ─────────────────────────────────────────────────
run_dependency_check() {
    info "Step 1/7: System dependencies..."

    MISSING=""
    check_dep() {
        if have "$1"; then ok "$1: $(command -v "$1")"
        else error "$1 NOT FOUND  ${2:+— $2}"; MISSING="$MISSING $1"; fi
    }

    check_dep "cargo"       "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    check_dep "rustc"       "rustup update stable"
    check_dep "python3"     "brew install python@3.14  /  sudo apt install python3.14"
    check_dep "git"         "brew install git  /  sudo apt install git"
    check_dep "openssl"     "brew install openssl  /  sudo apt install libssl-dev"
    check_dep "pkg-config"  "brew install pkg-config  /  sudo apt install pkg-config"

    # Rust >= 1.77
    if have rustc; then
        RUST_VER=$(rustc --version | cut -d' ' -f2)
        MAJOR=$(echo "$RUST_VER" | cut -d'.' -f1)
        MINOR=$(echo "$RUST_VER" | cut -d'.' -f2)
        if [ "$MAJOR" -gt 1 ] || { [ "$MAJOR" -eq 1 ] && [ "$MINOR" -ge 77 ]; }; then
            ok "rustc $RUST_VER (>= 1.77)"
        else
            error "rustc $RUST_VER too old — need >= 1.77"; MISSING="$MISSING rustc-version"
        fi
    fi

    # Python >= 3.11
    if have python3; then
        PY_MAJOR=$(python3 -c 'import sys; print(sys.version_info.major)')
        PY_MINOR=$(python3 -c 'import sys; print(sys.version_info.minor)')
        if [ "$PY_MAJOR" -gt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -ge 11 ]; }; then
            ok "python3 $PY_MAJOR.$PY_MINOR (>= 3.11)"
        else
            error "python3 $PY_MAJOR.$PY_MINOR too old — need >= 3.11"; MISSING="$MISSING python3-version"
        fi
    fi

    if [ -n "$MISSING" ]; then
        echo ""; error "Missing dependencies:$MISSING"; echo ""
        echo "  Install them and re-run this script."
        exit 1
    fi
}

# ── Step 2: Network diagnostic (the key step) ────────────────────────────────
run_network_diag() {
    info "Step 2/7: Network topology diagnostic..."
    > "$NETWORK_STATE_FILE"  # fresh state file

    # 2a. Detect primary interface + IP
    PRIMARY_IP=""; PRIMARY_IFACE=""; SUBNET_PREFIX=""
    if $IS_MACOS; then
        # Prefer en0 (built-in Wi-Fi/Ethernet), fallback to en1
        for iface in en0 en1 en2 en3; do
            PRIMARY_IP=$(ifconfig "$iface" 2>/dev/null | grep 'inet ' | awk '{print $2}' | grep -v '^127\.' | head -1)
            if [ -n "$PRIMARY_IP" ]; then PRIMARY_IFACE="$iface"; break; fi
        done
        if [ -n "$PRIMARY_IP" ]; then
            SUBNET_MASK_HEX=$(ifconfig "$PRIMARY_IFACE" 2>/dev/null | grep 'inet ' | awk '{print $4}' | cut -dx -f2)
            SUBNET_PREFIX=$(python3 -c "print(bin(int('$SUBNET_MASK_HEX',16)).count('1'))" 2>/dev/null || echo "24")
        fi
    else
        PRIMARY_IFACE=$(ip -4 route show default 2>/dev/null | awk '{print $5}' | head -1)
        PRIMARY_IP=$(ip -4 addr show "$PRIMARY_IFACE" 2>/dev/null | grep inet | awk '{print $2}' | cut -d'/' -f1 | head -1)
        SUBNET_PREFIX=$(ip -4 addr show "$PRIMARY_IFACE" 2>/dev/null | grep inet | awk '{print $2}' | cut -d'/' -f2 | head -1)
    fi

    if [ -n "$PRIMARY_IP" ]; then
        ok "Interface: $PRIMARY_IFACE  IP: $PRIMARY_IP/$SUBNET_PREFIX"
        echo "PRIMARY_IP=$PRIMARY_IP" >> "$NETWORK_STATE_FILE"
        echo "PRIMARY_IFACE=$PRIMARY_IFACE" >> "$NETWORK_STATE_FILE"
        echo "SUBNET_PREFIX=${SUBNET_PREFIX:-24}" >> "$NETWORK_STATE_FILE"
    else
        error "No routable IP found — check network cable / Wi-Fi"; exit 1
    fi

    # 2b. Is this IP a link-local (169.254.x.x) or public?
    case "$PRIMARY_IP" in
        169.254.*) warn "Link-local IP — DHCP failed or no DHCP server. mDNS will NOT cross routers." ;;
        10.*|172.1[6-9].*|172.2[0-9].*|172.3[0-1].*|192.168.*)
            ok "Private subnet — good for LAN mDNS discovery" ;;
        *) warn "Public IP detected (or unusual range) — ensure devices share the same subnet" ;;
    esac

    # 2c. Gateway reachability
    if $IS_MACOS; then
        GATEWAY=$(route -n get default 2>/dev/null | grep gateway | awk '{print $2}')
    else
        GATEWAY=$(ip route show default 2>/dev/null | awk '{print $3}' | head -1)
    fi
    if [ -n "$GATEWAY" ] && ping -c 1 -W 2 "$GATEWAY" >/dev/null 2>&1; then
        ok "Gateway reachable: $GATEWAY"
        echo "GATEWAY=$GATEWAY" >> "$NETWORK_STATE_FILE"
    else
        warn "Gateway unreachable — DHCP may be broken or VLAN isolated"
    fi

    # 2d. Multicast capability (critical for mDNS)
    if $IS_MACOS; then
        if ifconfig "$PRIMARY_IFACE" 2>/dev/null | grep -q MULTICAST; then
            ok "Multicast: ENABLED on $PRIMARY_IFACE"
        else
            error "Multicast: DISABLED on $PRIMARY_IFACE — mDNS WILL NOT WORK"
            warn "Fix: sudo ifconfig $PRIMARY_IFACE multicast"
        fi
    else
        if ip link show "$PRIMARY_IFACE" 2>/dev/null | grep -q MULTICAST; then
            ok "Multicast: ENABLED on $PRIMARY_IFACE"
        else
            error "Multicast: DISABLED on $PRIMARY_IFACE"; warn "Fix: sudo ip link set $PRIMARY_IFACE multicast on"
        fi
    fi

    # 2e. mDNS service check + auto-install
    info "Checking mDNS service (UDP 5353)..."
    if $IS_MACOS; then
        # macOS: Bonjour/mDNSResponder is always running
        if pgrep -x mDNSResponder >/dev/null 2>&1; then
            ok "mDNSResponder running (Bonjour)"
        else
            error "mDNSResponder NOT running — mDNS is broken on this Mac"
            warn "Fix: sudo launchctl load -w /System/Library/LaunchDaemons/com.apple.mDNSResponder.plist"
        fi
    else
        if systemctl is-active --quiet avahi-daemon 2>/dev/null; then
            ok "avahi-daemon: running"
        elif have systemd-resolve && systemd-resolve --status 2>/dev/null | grep -q "MulticastDNS.*yes"; then
            ok "systemd-resolved mDNS: enabled"
        else
            warn "No mDNS service running (avahi-daemon / systemd-resolved)"
            if $CHECK_ONLY; then
                ask "Run: sudo apt install avahi-daemon -y && sudo systemctl enable --now avahi-daemon"
            else
                ask "Installing avahi-daemon (needs sudo)..."
                ensure_sudo
                sudo apt update -qq && sudo apt install avahi-daemon -y -qq && sudo systemctl enable --now avahi-daemon
                ok "avahi-daemon installed & started"
            fi
        fi
    fi

    # 2f. Listen for mDNS traffic — can we actually receive?
    info "Probing mDNS broadcast reachability (5s listen)..."
    if $IS_MACOS; then
        # dns-sd -B to browse for _http._tcp services on the local link
        FOUND=$(timeout 6 dns-sd -B _http._tcp local 2>/dev/null | grep -c "Add" || echo "0")
    else
        FOUND=$(timeout 6 avahi-browse -a -t -k 2>/dev/null | wc -l || echo "0")
    fi
    FOUND=$(echo "$FOUND" | tr -d ' ')
    if [ "${FOUND:-0}" -gt 0 ]; then
        ok "mDNS broadcast reachable — ${FOUND} other service(s) visible"
        echo "MDNS_WORKING=true" >> "$NETWORK_STATE_FILE"
    else
        warn "No mDNS responses heard — possible causes:"
        warn "  1. AP/client isolation on Wi-Fi (common in campus/enterprise networks)"
        warn "  2. VLAN isolation — two wall ports on different subnets"
        warn "  3. Firewall blocking UDP 5353"
        echo "MDNS_WORKING=false" >> "$NETWORK_STATE_FILE"
    fi

    # 2g. AP isolation test — try to ping our own IP broadcast
    # (If this fails, the network interface is isolated)
    if $IS_MACOS; then
        ping -c 1 -W 1 "${PRIMARY_IP%.*}.255" >/dev/null 2>&1 && ok "Subnet broadcast reachable" || warn "Subnet broadcast blocked — likely AP/client isolation"
    fi

    # 2h. Detect VMware
    if $IS_LINUX; then
        if grep -qi vmware /sys/class/dmi/id/product_name 2>/dev/null || \
           grep -qi vmware /sys/class/dmi/id/sys_vendor 2>/dev/null || \
           systemd-detect-virt 2>/dev/null | grep -qi vmware; then
            warn "Running inside VMware VM"
            echo "IS_VM=true" >> "$NETWORK_STATE_FILE"

            # Check VMware network adapter mode
            # Bridged: the default route interface has a non-NAT IP on the same LAN
            # NAT: usually 192.168.xxx.0/24 where xxx is VMware's internal range
            case "$PRIMARY_IP" in
                192.168.78.*|192.168.163.*|192.168.40.*|192.168.94.*|172.16.*)
                    warn "  IP ($PRIMARY_IP) looks like VMware NAT — NOT Bridged!"
                    warn "  Fix: VMware Settings → Network Adapter → Bridged → Replicate physical state"
                    ;;
                *)  ok "  IP ($PRIMARY_IP) appears to be on the physical LAN — likely Bridged ✓" ;;
            esac
        else
            echo "IS_VM=false" >> "$NETWORK_STATE_FILE"
        fi
    fi

    # 2i. Detect if firewall is on
    if $IS_MACOS; then
        if sudo_maybe /usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate 2>/dev/null | grep -q "enabled"; then
            warn "macOS Application Firewall is ON — will add node binary after build"
            echo "FIREWALL_ON=true" >> "$NETWORK_STATE_FILE"
        else
            echo "FIREWALL_ON=false" >> "$NETWORK_STATE_FILE"
        fi
    else
        if have ufw && sudo_maybe ufw status 2>/dev/null | grep -q "active"; then
            warn "UFW firewall is ON — will add rules after checks"
            echo "FIREWALL_ON=true" >> "$NETWORK_STATE_FILE"
        else
            echo "FIREWALL_ON=false" >> "$NETWORK_STATE_FILE"
        fi
    fi

    # 2j. DHCP lease check (warn if short)
    if $IS_MACOS; then
        LEASE=$(ipconfig getpacket "$PRIMARY_IFACE" 2>/dev/null | grep lease_time | awk '{print $3}' | sed 's/0x//' || echo "")
        if [ -n "$LEASE" ] && [ "$LEASE" != "ffffffff" ]; then
            LEASE_SEC=$((16#$LEASE))
            if [ "$LEASE_SEC" -lt 3600 ]; then
                warn "DHCP lease: ${LEASE_SEC}s (< 1 hour) — IP may change, disrupting connections"
                warn "  Consider DHCP reservation (MAC binding) for stable IP"
            else
                ok "DHCP lease: $((LEASE_SEC / 3600))h — stable"
            fi
        fi
    fi
}

# ── Step 3: Firewall auto-config ─────────────────────────────────────────────
run_firewall_config() {
    if $CHECK_ONLY; then
        info "Step 3/7: (check-only) Skipping firewall config"
        return
    fi

    FIREWALL_ON=$(grep -c "FIREWALL_ON=true" "$NETWORK_STATE_FILE" 2>/dev/null || echo "0")
    if [ "$FIREWALL_ON" -eq 0 ]; then
        info "Step 3/7: Firewall not active — skipping"
        return
    fi

    info "Step 3/7: Configuring firewall for mDNS + libp2p..."

    if $IS_MACOS; then
        ask "To auto-configure macOS firewall, sudo is needed..."
        ensure_sudo
        # Allow the node binary (will be built later, but try now)
        NODE_BIN="$REPO_DIR/target/release/node"
        if [ -f "$NODE_BIN" ]; then
            sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$NODE_BIN" 2>/dev/null || true
            sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$NODE_BIN" 2>/dev/null || true
            ok "macOS firewall: node binary allowed"
        else
            info "Node binary not built yet — will configure firewall in run_node_mac.sh"
        fi
        # Allow Python for the HTTP relay
        PYTHON_BIN="$(command -v python3)"
        sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$PYTHON_BIN" 2>/dev/null || true
        sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$PYTHON_BIN" 2>/dev/null || true
        ok "macOS firewall: python3 allowed (for HTTP relay)"
    else
        ask "To auto-configure UFW, sudo is needed..."
        ensure_sudo
        sudo ufw allow 5353/udp comment 'eo-mDNS-discovery' 2>/dev/null || true
        sudo ufw allow 42000:43000/tcp comment 'eo-libp2p-swarm' 2>/dev/null || true
        ok "UFW: UDP 5353 + TCP 42000-43000 opened for mDNS + libp2p"
    fi
}

# ── Step 4: Storage directories ──────────────────────────────────────────────
run_storage_setup() {
    if $CHECK_ONLY; then
        info "Step 4/7: (check-only) Skipping storage creation"; return
    fi
    info "Step 4/7: Creating storage tree at $EO_HOME..."
    mkdir -p "$EO_HOME/objects" "$EO_HOME/wal" "$EO_HOME/raft"
    chmod 755 "$EO_HOME"
    ok "$EO_HOME/objects/  (CAS blob store)"
    ok "$EO_HOME/wal/      (LSM WAL)"
    ok "$EO_HOME/raft/     (Raft persistent state)"
}

# ── Step 5: IPC socket dir ───────────────────────────────────────────────────
run_ipc_setup() {
    if $CHECK_ONLY; then
        info "Step 5/7: (check-only) Skipping IPC setup"; return
    fi
    info "Step 5/7: IPC socket at $EO_IPC_SOCKET..."
    IPC_DIR=$(dirname "$EO_IPC_SOCKET")
    mkdir -p "$IPC_DIR"
    [ -S "$EO_IPC_SOCKET" ] && rm -f "$EO_IPC_SOCKET" && ok "Removed stale socket"
    ok "IPC dir: $IPC_DIR  (socket created by node on startup, chmod 660)"
}

# ── Step 6: Python venv ──────────────────────────────────────────────────────
run_python_setup() {
    info "Step 6/7: Python eo-agent environment..."
    EO_AGENT_DIR="$REPO_DIR/eo-agent"
    if [ ! -d "$EO_AGENT_DIR" ]; then
        error "eo-agent directory not found at $EO_AGENT_DIR"; exit 1
    fi

    VENV_DIR="$EO_AGENT_DIR/.venv"
    if [ -d "$VENV_DIR" ] && [ -f "$VENV_DIR/bin/python" ]; then
        VENV_PY=$("$VENV_DIR/bin/python" --version 2>&1)
        ok "Virtualenv: $VENV_PY"
    else
        if $CHECK_ONLY; then
            warn "Virtualenv missing — run without --check-only to create"
        else
            warn "Creating virtualenv..."
            python3 -m venv "$VENV_DIR"
            source "$VENV_DIR/bin/activate"
            pip install "$EO_AGENT_DIR[dev]" -q
            deactivate
            ok "eo-agent installed into venv"
        fi
    fi

    # Quick import test
    if "$VENV_DIR/bin/python" -c "import eo_agent" 2>/dev/null; then
        ok "eo_agent module importable"
    else
        warn "eo_agent not importable — run: $VENV_DIR/bin/pip install $EO_AGENT_DIR[dev]"
        info "Note: Python 3.14+ requires non-editable install (pip install, not pip install -e)"
        info "  because .pth files starting with '_' are skipped for security."
    fi
}

# ── Step 7: Rust workspace ───────────────────────────────────────────────────
run_rust_check() {
    info "Step 7/7: Rust workspace..."
    cd "$REPO_DIR"
    if cargo metadata --no-deps --format-version 1 >/dev/null 2>&1; then
        ok "Cargo workspace valid"
    else
        error "Invalid Cargo.toml"; exit 1
    fi
    if $CHECK_ONLY; then
        info "(check-only) cargo check --workspace..."
        cargo check --workspace 2>&1 | tail -3
    else
        info "cargo check --workspace (one-time)..."
        cargo check --workspace 2>&1 | tail -3
        ok "Workspace compiles"
    fi
}

# ── Summary ──────────────────────────────────────────────────────────────────
print_summary() {
    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Edge-Cloud Orchestrator — Environment Ready${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  Platform:     $(uname -s) ($(uname -m))"
    echo "  IP:           ${PRIMARY_IP:-unknown}  (iface: ${PRIMARY_IFACE:-unknown})"
    echo "  GateWay:      ${GATEWAY:-unknown}"
    echo "  mDNS:         ${MDNS_WORKING:-unchecked}"
    echo "  Storage:      $EO_HOME"
    echo "  IPC socket:   $EO_IPC_SOCKET"
    echo ""
    MDNS_WORKING=$(grep "MDNS_WORKING=" "$NETWORK_STATE_FILE" 2>/dev/null | cut -d= -f2 || echo "unknown")
    if [ "$MDNS_WORKING" = "false" ]; then
        echo -e "  ${YELLOW}⚠ mDNS was NOT detected working on this network.${NC}"
        echo "  You may need to manually configure bootstrap_peers in the YAML config."
        echo "  See RUNBOOK.md §2 for troubleshooting."
        echo ""
    fi
    echo "  Next:"
    echo "    ./scripts/run_node_mac.sh        # On the ControlPlane (macOS)"
    echo "    ./scripts/run_node_linux.sh      # On each Execution node (Linux)"
    echo ""
}

# ── Main dispatcher ──────────────────────────────────────────────────────────
if $NETWORK_ONLY; then
    run_network_diag
    run_firewall_config
    print_summary
    exit 0
fi

run_dependency_check
run_network_diag
run_firewall_config

if $NETWORK_ONLY; then
    print_summary
    exit 0
fi

run_storage_setup
run_ipc_setup
run_python_setup
run_rust_check
print_summary
