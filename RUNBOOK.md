# Edge-Cloud Orchestrator — Multi-Device Production Runbook

**Version:** 1.0.0
**Date:** 2026-06-11
**Target Cluster:** MacBook Air (ControlPlane) + VMware Ubuntu (Execution Node) + iPhone (iOS Client)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Network Architecture & mDNS Discovery](#2-network-architecture--mdns-discovery)
3. [Storage Directory Structure](#3-storage-directory-structure)
4. [IPC Socket Management](#4-ipc-socket-management)
5. [Dependency Matrix](#5-dependency-matrix)
6. [Deployment Sequence](#6-deployment-sequence)
7. [macOS — ControlPlane, Inference Hub, Storage Peer](#7-macos--controlplane-inference-hub-storage-peer)
8. [Ubuntu Linux (VMware) — Execution Node, Storage Peer](#8-ubuntu-linux-vmware--execution-node-storage-peer)
9. [iOS (iPhone) — Remote Trigger & CLI Controller](#9-ios-iphone--remote-trigger--cli-controller)
10. [Cluster Scaling — Adding a 3rd (Nth) Device](#10-cluster-scaling--adding-a-3rd-nth-device)
11. [Operational Monitoring](#11-operational-monitoring)
12. [Troubleshooting](#12-troubleshooting)
13. [Security Considerations](#13-security-considerations)
14. [Reference — All Scripts & Configs](#14-reference--all-scripts--configs)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        LOCAL SUBNET (192.168.1.0/24)                  │
│                                                                       │
│  ┌─────────────────────────┐    mDNS/UDP:5353    ┌──────────────────┐ │
│  │   MacBook Air (macOS)   │◄──────────────────►│ VMware Ubuntu VM  │ │
│  │                         │                     │ (Linux)          │ │
│  │  Roles:                 │   libp2p gossip     │                  │ │
│  │  • ControlPlane         │◄──────────────────►│ Roles:           │ │
│  │  • Inference Hub        │                     │ • Execution      │ │
│  │  • Storage Peer         │   JSON-RPC / UDS    │ • Storage Peer   │ │
│  │                         │◄──────────────────►│                  │ │
│  │  ┌─────────────────┐   │                     │ ┌──────────────┐ │ │
│  │  │ Rust node        │   │                     │ │ Rust node    │ │ │
│  │  │ (P2P + Raft +    │   │                     │ │ (P2P + CAS + │ │ │
│  │  │  IPC server)     │   │                     │ │  Sandbox)    │ │ │
│  │  └────────┬────────┘   │                     │ └──────────────┘ │ │
│  │           │ UDS         │                     │                  │ │
│  │  ┌────────▼────────┐   │                     └──────────────────┘ │
│  │  │ Python eo-agent  │   │                                          │
│  │  │ (LangGraph ReAct) │   │  ┌──────────────────────────────────┐  │
│  │  └─────────────────┘   │  │   iPhone (iOS)                     │  │
│  └─────────────────────────┘  │                                    │  │
│                               │  Roles:                            │  │
│                               │  • Event Trigger                   │  │
│                               │  • Remote CLI (Shortcuts/Pythonista)│  │
│                               └───────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### Component Map

| Device | OS | Role(s) | Binary/Runtime | Key Process |
|--------|----|---------|---------------|-------------|
| MacBook Air | macOS 15 | ControlPlane, Inference, Storage | Rust `node` + Python `eo-agent` | Raft leader, agent REPL |
| VMware VM | Ubuntu 24.04 | Execution, Storage | Rust `node` | Sandbox runner, CAS peer |
| iPhone | iOS 20 | Event Trigger, CLI | Shortcuts / Pythonista | HTTP POST → ControlPlane |

### Data Flow

1. User types natural language into `eo-agent` REPL on macOS (or sends via iPhone Shortcut)
2. `eo-agent` (LangGraph) invokes `get_cluster_topology` tool → JSON-RPC over UDS → Rust node
3. LangGraph Planner node reads topology, produces structured execution plan
4. LangGraph Coder node generates code (Python/Wasm), Deployment node calls `submit_to_cas_and_raft` → Rust node stores blob in CAS, proposes task to Raft
5. Raft consensus replicates the task; orchestration engine assigns it to the Linux Execution node
6. Linux node fetches blob from CAS, runs sandbox, stores `ExecutionResult` back in CAS
7. `eo-agent` Evaluator node calls `fetch_execution_result`, self-heals if exit_code ≠ 0 (up to 3 retries)
8. Final answer returned to user

---

## 2. Network Architecture & mDNS Discovery

### 2.1 Subnet Topology

All devices MUST be on the same logical Ethernet segment for mDNS multicast discovery to work. The local subnet is assumed to be `192.168.1.0/24` (adjust for your network).

### 2.2 VMware Bridged Mode Configuration

The Ubuntu VM must use **Bridged Networking** (not NAT, not host-only) so it appears as a distinct physical device on the LAN.

**Step-by-step (VMware Workstation / Fusion):**

1. Shut down the Ubuntu VM
2. Open **Virtual Machine Settings** → **Network Adapter**
3. Set **Network Connection** to **Bridged: Connected directly to the physical network**
4. Under **Advanced**, ensure **Replicate physical network connection state** is checked
5. Start the VM
6. Verify the VM received a LAN IP on the same subnet as the Mac:

```bash
# On macOS
ifconfig en0 | grep 'inet '
# Output: inet 192.168.1.42 netmask 0xffffff00 broadcast 192.168.1.255

# On Ubuntu VM
ip -4 addr show | grep inet
# Output: inet 192.168.1.67/24 brd 192.168.1.255 scope global ...
```

If the VM shows a `172.16.x.x` or `10.x.x.x` address, bridged mode is not working — re-check the VMware settings.

### 2.3 mDNS Verification

```bash
# macOS — mDNSResponder is built-in, always running
dns-sd -B _http._tcp local
# Should list Bonjour-advertised services

# Ubuntu — install avahi-daemon if not present
sudo apt install avahi-daemon -y
sudo systemctl enable --now avahi-daemon

# Cross-device ping test
# From Mac:
ping 192.168.1.67   # Ubuntu VM IP

# From Ubuntu VM:
ping 192.168.1.42   # Mac IP
```

### 2.4 Firewall Rules

**macOS:** The Rust `node` binary binds a random TCP port for libp2p. Allow it:

```bash
# Check if firewall is on
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate

# If on, allow the node binary
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /path/to/target/release/node
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp /path/to/target/release/node
```

**Linux (UFW):** Allow mDNS and the libp2p port range:

```bash
sudo ufw allow 5353/udp comment 'mDNS discovery'
sudo ufw allow 42000:43000/tcp comment 'libp2p swarm ports'
sudo ufw enable
```

### 2.5 libp2p Peer Discovery Flow

```
1. Node A starts → generates Ed25519 keypair → PeerId derived from public key
2. Node A binds TCP listener on random port → mDNS announces "_ipfs-discovery._udp" service
3. Node B starts → mDNS browser sees Node A's announcement
4. Node B opens TCP connection to Node A → libp2p Identify protocol exchanges agent version + protocols
5. Node B sends DescriptorRequest → Node A responds with NodeDescriptor (roles, capabilities, OS)
6. Node A receives Node B's descriptor → logs "mDNS: discovered peer <PeerId>"
7. Both nodes now know each other's full descriptor → Raft can absorb new peers
```

---

## 3. Storage Directory Structure

All nodes use a standardized directory layout under `$EO_HOME` (default `~/.eo_storage`).

```
~/.eo_storage/
├── objects/          # CAS (Content-Addressed Storage) — Git-model blob store
│   ├── ab/           # Sharded by first two hex chars of SHA-256 hash
│   │   └── abcdef1234567890...   # Raw blob (code, results, descriptors)
│   ├── cd/
│   └── ...
├── wal/              # LSM-tree Write-Ahead Log
│   ├── 000001.log    # Immutable WAL segments (rotated at 64MB)
│   ├── 000002.log
│   └── CURRENT       # Points to the active WAL segment
└── raft/             # Raft persistent state (if this node is a Raft peer)
    ├── current_term
    └── voted_for
```

### Setup (handled by `scripts/setup_env.sh`)

```bash
# Manual equivalent:
mkdir -p ~/.eo_storage/{objects,wal,raft}
chmod 755 ~/.eo_storage
```

**Cross-device note:** These are local directories — not shared storage. Each device maintains its own CAS store. Blobs are replicated across Storage peers via the P2P blob sync protocol.

---

## 4. IPC Socket Management

### 4.1 Socket Path Convention

| Platform | Default Path | Rationale |
|----------|-------------|-----------|
| macOS | `/tmp/eo_control.sock` | `/tmp` is world-writable, survives reboots on macOS |
| Linux | `/tmp/eo_control.sock` | Standard FHS temp location |
| Override | `EO_IPC_SOCKET` env var | For containerized or multi-node-on-one-machine setups |

The Rust node creates the socket on startup (`bootstrap.rs:123`). The Python `eo-agent` connects to it (`main.py:37`).

### 4.2 Permission Model

The UDS is created by the Rust node (running as the current user). For the Python eo-agent to connect, it needs read/write access:

```bash
# The run_node_mac.sh script does this automatically after socket creation:
chmod 666 /tmp/eo_control.sock
```

**Security note:** `chmod 666` makes the socket world-readable and world-writable. This is acceptable for a development/workstation setup on a trusted local network. For production deployments with multi-tenant hosts, use a dedicated group:

```bash
# Production alternative:
sudo chgrp eo-group /tmp/eo_control.sock
chmod 660 /tmp/eo_control.sock
```

### 4.3 Protocol

- **Wire format:** Newline-delimited JSON-RPC 2.0, one request-response per connection
- **Encoding:** UTF-8
- **Methods:** `get_cluster_topology`, `submit_to_cas_and_raft`, `fetch_execution_result`
- **Reference:** [crates/ipc/src/handler.rs](crates/ipc/src/handler.rs)

```json
→ {"jsonrpc":"2.0","method":"get_cluster_topology","params":{},"id":1}
← {"jsonrpc":"2.0","result":{"nodes":[],"tasks_completed":0},"id":1}
```

### 4.4 Troubleshooting Permissions

If `eo-agent` fails with `ConnectionRefusedError` or `PermissionError`:

```bash
# 1. Is the socket alive?
ls -la /tmp/eo_control.sock
# Expected: srw-rw-rw- 1 macbook staff 0 ...

# 2. Check the node is running
cat /tmp/eo_node_mac.pid  # or /tmp/eo_node_linux.pid
kill -0 $(cat /tmp/eo_node_mac.pid) && echo "Node alive"

# 3. Manually set permissions
chmod 666 /tmp/eo_control.sock

# 4. Connect manually to test
echo '{"jsonrpc":"2.0","method":"get_cluster_topology","params":{},"id":1}' | nc -U /tmp/eo_control.sock
```

---

## 5. Dependency Matrix

| Dependency | macOS (Apple Silicon) | Ubuntu (x86_64/arm64) | Purpose |
|-----------|----------------------|----------------------|---------|
| Rust 1.77+ | `brew install rustup-init` | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` | Compiles all crates |
| Python 3.11+ | `brew install python@3.14` | `sudo apt install python3.14` | eo-agent runtime |
| OpenSSL | `brew install openssl` | `sudo apt install libssl-dev` | TLS for libp2p |
| pkg-config | `brew install pkg-config` | `sudo apt install pkg-config` | Cargo native deps |
| Docker (opt.) | Docker Desktop | `sudo apt install docker.io` | Container runtime |
| Wasmtime | (bundled via Cargo) | (bundled via Cargo) | Wasm sandbox |
| avahi-daemon | (built-in Bonjour) | `sudo apt install avahi-daemon` | mDNS on Linux |

Run `./scripts/setup_env.sh` on each device — it checks all of the above automatically.

---

## 6. Deployment Sequence

### Recommended Order

```
Step 1: macOS — Run setup_env.sh (installs deps, creates dirs)
Step 2: macOS — Run run_node_mac.sh (starts Rust node + eo-agent REPL)
Step 3: Ubuntu VM — Configure VMware Bridged Mode
Step 4: Ubuntu VM — Run setup_env.sh
Step 5: Ubuntu VM — Run run_node_linux.sh
Step 6: macOS — Verify peer discovery in logs
Step 7: iPhone — Configure Shortcut or Pythonista
Step 8: Test — Submit a task from iPhone, see it execute on Linux
```

### Quick Start (all-in-one)

```bash
# Clone on both Mac and Linux
git clone https://github.com/edge-orchestrator/edge-orchestrator.git
cd edge-orchestrator

# macOS
./scripts/setup_env.sh && ./scripts/run_node_mac.sh

# Linux
./scripts/setup_env.sh && ./scripts/run_node_linux.sh
```

---

## 7. macOS — ControlPlane, Inference Hub, Storage Peer

### 7.1 Configuration

The Mac node uses [configs/mac-node.yaml](configs/mac-node.yaml). Before starting, review and adjust:

```yaml
# configs/mac-node.yaml
node_id: ""                    # Auto-generated UUID v4
node_type: "Heavy"             # Mac has ample resources
listen_addresses:
  - "/ip4/0.0.0.0/tcp/0"      # Random port, all interfaces
bootstrap_peers: []            # mDNS-only; empty = auto-discovery
capabilities:
  storage: true                # Serves CAS blobs
  gpu_acceleration: false      # Set to true if using Apple MPS/ANE for inference
  runtimes:
    - "Wasm"                   # Wasmtime — always available
  max_memory_mb: 16384         # Adjust to your Mac's RAM
  cpu_cores: 4                 # Adjust for your Mac
roles:
  - "Execution"                # Can also execute (light tasks)
  - "Storage"                  # Persistent blob storage
  - "Coordinator"              # Runs scheduling logic
```

### 7.2 Recommended Role Layout

For the MacBook Air as the primary ControlPlane, add `Coordinator` and `Inference` roles:

```yaml
roles:
  - "Coordinator"
  - "Inference"
  - "Storage"
  - "Execution"
```

### 7.3 Launch

```bash
# Full stack (node + agent REPL)
./scripts/run_node_mac.sh

# Node only (agent in another terminal)
./scripts/run_node_mac.sh --node-only

# Agent only (connect to already-running node)
./scripts/run_node_mac.sh --agent-only

# Demo mode (no Rust node, all mock responses)
./scripts/run_node_mac.sh --mock
```

### 7.4 Monitoring During Operation

```bash
# Tail node logs (timestamped in logs/)
tail -f logs/node_mac_*.log

# Key log lines to watch:
#   "mDNS: discovered peer <PeerId>"         ← Linux peer found
#   "Received descriptor from <PeerId>"      ← Linux capabilities received
#   "IPC server listening on /tmp/eo_control.sock"
#   "Raft consensus initialized"

# Agent logs
tail -f logs/agent_mac_*.log

# Swarm status via eo-agent REPL
eo> topology
```

### 7.5 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `EO_HOME` | `~/.eo_storage` | Storage root |
| `EO_IPC_SOCKET` | `/tmp/eo_control.sock` | UDS path |
| `DEEPSEEK_API_KEY` | (required) | LLM API key |
| `EO_LLM_MODEL` | `gpt-4o` | Model override |
| `EO_MOCK_MODE` | `false` | Mock mode toggle |
| `RUST_LOG` | `info` | Tracing level |

---

## 8. Ubuntu Linux (VMware) — Execution Node, Storage Peer

### 8.1 VMware Pre-flight Checklist

- [ ] Network Adapter: **Bridged** (not NAT, not host-only)
- [ ] Ubuntu has a LAN IP in `192.168.1.0/24`
- [ ] Can ping the Mac from Ubuntu and vice versa
- [ ] `avahi-daemon` installed and running
- [ ] `EO_HOME` directories created: `~/[.eo_storage/objects/](.eo_storage/objects/)`, `~/[.eo_storage/wal/](.eo_storage/wal/)`

### 8.2 Configuration

The Linux node uses [configs/linux-node.yaml](configs/linux-node.yaml):

```yaml
# configs/linux-node.yaml
node_id: ""
node_type: "Heavy"
listen_addresses:
  - "/ip4/0.0.0.0/tcp/0"
bootstrap_peers: []
capabilities:
  storage: true
  gpu_acceleration: false
  runtimes:
    - "Wasm"
    - "NativePosix"      # Linux namespaces
    - "Container"         # Docker/OCI
  max_memory_mb: 32768    # Allocate generously for a VM
  cpu_cores: 8
roles:
  - "Execution"
  - "Storage"
```

### 8.3 Launch

```bash
# Standard launch
./scripts/run_node_linux.sh

# With explicit bootstrap peer (if mDNS isn't working)
./scripts/run_node_linux.sh /ip4/192.168.1.42/tcp/42069/p2p/12D3KooW...
```

### 8.4 Verifying Sandbox Capabilities

```bash
# Check Wasmtime is bundled (no separate install needed — compiled into the binary)
ldd target/release/node | grep wasmtime && echo "Wasmtime linked"

# Check Docker is available for container runtime
docker run --rm hello-world

# Verify Linux namespaces are available (required for NativePosix)
ls -la /proc/self/ns/
```

### 8.5 Resource Allocation

For a VMware Ubuntu VM, allocate at minimum:

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| vCPUs | 4 | 8 |
| RAM | 8 GB | 16 GB |
| Disk | 20 GB | 40 GB |
| Network | Bridged | Bridged |

The `configs/linux-node.yaml` should reflect the actual VM allocation in `max_memory_mb` and `cpu_cores`.

---

## 9. iOS (iPhone) — Remote Trigger & CLI Controller

Two deployment methodologies are supported. Choose the one that matches your comfort level and iOS development tooling.

### 9.1 Method A — iOS Shortcuts (Zero-Code)

Use the Shortcuts app to send natural language commands to the cluster via SSH or HTTP.

#### 9.1.1 HTTP POST Shortcut

The macOS node's `eo-agent` can be wrapped in a lightweight HTTP listener (Flask/FastAPI) that relays POST body text to the agent. Assuming you deploy a small relay server:

**Relay snippet (run on macOS, e.g., as a launchd service):**

```python
# relay.py — run on macOS: python3 relay.py
import subprocess, json
from http.server import HTTPServer, BaseHTTPRequestHandler

class Relay(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers['Content-Length'])
        body = self.rfile.read(length).decode('utf-8')
        # Invoke eo-agent
        result = subprocess.run(
            ['eo-agent', body],
            capture_output=True, text=True, timeout=120
        )
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({
            'answer': result.stdout,
            'error': result.stderr,
        }).encode())

HTTPServer(('0.0.0.0', 9090), Relay).serve_forever()
```

**iOS Shortcut Setup:**

1. Open **Shortcuts** app → **+** → **Add Action**
2. Search for **"Get Contents of URL"**
3. Set:
   - **URL:** `http://192.168.1.42:9090`
   - **Method:** POST
   - **Request Body:** `{"prompt": "Deploy a hello-world Wasm module to the nearest executor"}`
4. Add action **"Show Result"** to display the agent's response
5. Name the Shortcut: *"EO — Deploy Hello"*
6. Add to Home Screen for one-tap triggering

#### 9.1.2 SSH Shortcut

1. Open **Shortcuts** → **+** → **Add Action**
2. Search for **"Run Script Over SSH"**
3. Set:
   - **Host:** `192.168.1.42`
   - **User:** your macOS username
   - **Password/Key:** authenticate
   - **Script:**
     ```bash
     cd ~/edge-orchestrator
     source eo-agent/.venv/bin/activate
     eo-agent "$1"
     ```
   - **Input:** `"Show all nodes in the cluster"` (becomes `$1`)
4. Save as *"EO — Cluster Status"*

### 9.2 Method B — Pythonista 3 / Pyto (Lightweight App)

For a native iOS Python experience with full programmatic control.

#### 9.2.1 Install & Configure

1. Install **[Pythonista 3](https://apps.apple.com/app/pythonista-3/id1085978097)** or **[Pyto](https://apps.apple.com/app/pyto-python-3/id1436650069)** from the App Store
2. Create a new script: `eo_client.py`

```python
"""
eo_client.py — Edge-Cloud Orchestrator iOS Client Stub
Run in Pythonista 3 or Pyto on iPhone/iPad.

Connects to the macOS ControlPlane via HTTP (the relay server from §9.1.1)
or directly via SSH. Provides a lightweight console for submitting tasks.
"""

import requests
import json

CONTROL_PLANE = "http://192.168.1.42:9090"  # Adjust to your Mac's IP

def send_prompt(prompt: str) -> dict:
    """Send a natural-language prompt to the control plane."""
    resp = requests.post(
        CONTROL_PLANE,
        data=json.dumps({"prompt": prompt}),
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()

def main():
    print("Edge-Cloud Orchestrator — iOS Client")
    print(f"Control Plane: {CONTROL_PLANE}")
    print("Type your task or 'quit' to exit.\n")

    while True:
        try:
            cmd = input("eo> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not cmd:
            continue
        if cmd.lower() in ("quit", "exit", "q"):
            print("Goodbye.")
            break

        try:
            result = send_prompt(cmd)
            print(f"\n{result.get('answer', 'No answer')}\n")
            if result.get('error'):
                print(f"[stderr]: {result['error']}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
```

#### 9.2.2 Integration with Cluster State

To make the iOS client aware of the full cluster topology (not just the control plane), add a `topology` command:

```python
def get_topology() -> dict:
    resp = requests.post(
        CONTROL_PLANE,
        data=json.dumps({"prompt": "Show me the full cluster topology"}),
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    return resp.json()

# In the REPL loop:
if cmd.lower() == "topology":
    topo = get_topology()
    print(json.dumps(topo, indent=2))
    continue
```

---

## 10. Cluster Scaling — Adding a 3rd (Nth) Device

This section covers the Standard Operating Procedure for onboarding a new device — a Windows PC, a cloud VM, a Raspberry Pi, or any other machine — into the running cluster without restarting the control plane.

### 10.1 The `node_extension.yaml` File

The declarative on-ramp. See [scripts/node_extension.yaml](scripts/node_extension.yaml) for the full annotated template.

**Minimal working example (Raspberry Pi):**

```yaml
node_id: ""
node_type: "Light"
listen_addresses:
  - "/ip4/0.0.0.0/tcp/0"
bootstrap_peers: []
capabilities:
  storage: false
  gpu_acceleration: false
  runtimes:
    - "Wasm"
  max_memory_mb: 2048
  cpu_cores: 4
roles:
  - "Execution"
```

### 10.2 Onboarding SOP

#### Step 1: Prepare the new device

```bash
# Clone the repo
git clone https://github.com/edge-orchestrator/edge-orchestrator.git
cd edge-orchestrator

# Copy and customize the extension template
cp scripts/node_extension.yaml configs/new-node.yaml
# Edit configs/new-node.yaml to match your device's capabilities
```

#### Step 2: Verify network co-presence

```bash
# Run setup (checks deps + network)
./scripts/setup_env.sh

# Confirm the new device sees the control plane
ping 192.168.1.42   # Replace with your Mac's IP
```

#### Step 3: Launch the new node

```bash
# For Linux/cloud-VM/new-device:
./scripts/run_node_linux.sh

# The node will:
#  1. Generate an Ed25519 keypair → new PeerId
#  2. Bind a TCP port for libp2p
#  3. Announce via mDNS
#  4. The control plane discovers it automatically
```

#### Step 4: Verify discovery on the control plane

```bash
# On the macOS control plane:
tail -f logs/node_mac_*.log
```

Expected log lines within ~10 seconds:

```
[INFO] mDNS: discovered peer 12D3KooWAbCdEfGhIjKlMnOpQrStUvWxYz1234
[INFO] Received descriptor from 12D3KooW...: node_type=Light, capabilities=Capabilities { ... }
```

#### Step 5: Confirm via eo-agent

Inside the `eo-agent` REPL on macOS:

```
eo> Show me the full cluster topology with all peers and their capabilities
eo> Deploy a fibonacci calculator Wasm module, pinned to the new node
```

### 10.3 Raft Dynamic Membership

The Raft consensus layer absorbs new PeerIds without restart:

1. New node connects via libp2p → descriptor exchanged
2. `JsonRpcHandler.get_cluster_topology()` returns the updated node list (reads from cluster state held in the Raft state machine — see [crates/ipc/src/handler.rs:136](crates/ipc/src/handler.rs#L136))
3. Future `Proposal::SubmitTask` can target the new node via `RoutingStrategy::Pinned(new_node_id)`

**Note:** Full dynamic Raft member addition (adding a voting node to the Raft group) requires `Proposal::AddNode` which is not yet implemented in the 0.1.0 release. The new node joins as an **executing/storage peer** but not a Raft voter. Raft membership changes are on the roadmap.

### 10.4 Cloud Instance (AWS/GCP/Azure) Onboarding

When the new device is a cloud VM (not on the local subnet), mDNS won't work. Use explicit bootstrap peers:

```yaml
# configs/cloud-node.yaml
bootstrap_peers:
  - "/ip4/<YOUR_MAC_PUBLIC_IP>/tcp/42069/p2p/<YOUR_MAC_PEER_ID>"
```

To find the control plane's full multiaddr:

```bash
# On the Mac, grep the node log:
grep "Listening on" logs/node_mac_*.log
# Example output:
# Listening on /ip4/192.168.1.42/tcp/42069
# Listening on /ip4/192.168.1.42/tcp/42069/p2p/12D3KooWRh8j5fABCDEFGHIJKLMNOPQRSTUVWXYZ

# Use the FULL /p2p/<PeerId> version as the bootstrap peer on the cloud node.
```

For the cloud node to reach your Mac behind NAT, you'll need port forwarding (TCP 42069 → Mac's local IP) or a VPN.

---

## 11. Operational Monitoring

### 11.1 Log Files

All logs are written to the `logs/` directory (git-ignored) with timestamps:

```
logs/
├── node_mac_20260611_143022.log
├── node_linux_20260611_143025.log
├── agent_mac_20260611_143030.log
└── ...
```

### 11.2 Key Metrics

| Metric | Source | What to Watch |
|--------|--------|---------------|
| Peer count | `eo> topology` | Should match expected cluster size |
| Task queue depth | eo-agent `get_cluster_topology` | `tasks_pending`, `tasks_completed` |
| CAS blob count | `find ~/.eo_storage/objects -type f \| wc -l` | Grows with task submissions |
| Disk usage | `du -sh ~/.eo_storage` | Watch for bloat |
| Retry rate | `grep "retry_count" logs/agent_mac_*.log` | >1 = self-healing active |
| Raft term changes | `grep "term" logs/node_mac_*.log` | Should be stable |

### 11.3 Health Check Commands

```bash
# Is the Rust node alive?
kill -0 $(cat /tmp/eo_node_mac.pid) 2>/dev/null && echo "Mac node: OK" || echo "Mac node: DOWN"

# Is the IPC socket responsive?
echo '{"jsonrpc":"2.0","method":"get_cluster_topology","params":{},"id":1}' | nc -U /tmp/eo_control.sock

# Is the eo-agent REPL available?
ps aux | grep eo-agent | grep -v grep

# Are peers visible?
grep "mDNS: discovered peer" logs/node_mac_*.log | tail -5
```

---

## 12. Troubleshooting

### 12.1 Common Issues & Resolutions

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Linux node not discovered by Mac | VMware in NAT mode, not Bridged | See [§2.2](#22-vmware-bridged-mode-configuration) |
| Linux node not discovered (Bridged confirmed) | avahi-daemon not running | `sudo systemctl restart avahi-daemon` |
| `ConnectionRefusedError` from Python | Stale socket or node not running | `rm -f /tmp/eo_control.sock && ./scripts/run_node_mac.sh` |
| `PermissionError` from Python | Socket permissions too restrictive | `chmod 666 /tmp/eo_control.sock` |
| Node binary panics on start | Port conflict or missing config | `tail -50 logs/node_mac_*.log` |
| `DEEPSEEK_API_KEY not set` | LLM env var missing | `export DEEPSEEK_API_KEY="sk-..."` |
| eo-agent produces no output | LLM model not available | Try `--mock` mode or check API key quota |
| Raft node times out | `_cas_storage` unused var (expected for single-node) | This is normal — single-node Raft skips log replication |
| iOS Shortcut fails | Mac firewall blocking port 9090 | `sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add python3` |
| "Code too large" error | >64KB code blob submitted | Split into smaller modules or increase limit in [handler.rs:170](crates/ipc/src/handler.rs#L170) |

### 12.2 Diagnostic Collection

When reporting issues, collect:

```bash
# System info
uname -a && echo "---"

# Git state
git log --oneline -3 && echo "---"

# Cargo version
cargo --version && rustc --version && echo "---"

# Python version
python3 -V && echo "---"

# Network
ifconfig en0 2>/dev/null || ip addr show && echo "---"

# Last 50 lines of each log
for f in logs/*.log; do
    echo "=== $f ==="
    tail -50 "$f"
done

# Cluster state (from eo-agent mock mode)
cd eo-agent && source .venv/bin/activate && eo-agent "topology" --mock
```

---

## 13. Security Considerations

### 13.1 Current State (0.1.0 — Development)

- **UDS socket:** `chmod 666` — world-writable on the local machine. Only local users can access it.
- **libp2p:** Ed25519 keypairs generated fresh each boot (no key persistence yet). PeerId changes across restarts.
- **No authentication:** JSON-RPC has no auth tokens. Any process on the local machine can call IPC methods.
- **No encryption at rest:** CAS blobs stored in plaintext on disk.

### 13.2 Hardening Roadmap

| Area | Current (0.1.0) | Planned (0.2.0+) |
|------|----------------|-----------------|
| IPC | `chmod 666` world-writable | Group-gated `chmod 660`, Unix peer credentials |
| Identity | Ephemeral keypair — new PeerId each boot | `--key-file` flag for persistent Ed25519 key |
| IPC Auth | None | HMAC-signed requests with pre-shared key |
| libp2p | Plain TCP | Noise handshake + QUIC transport |
| At-rest | Plaintext CAS blobs | AES-256-GCM encryption with per-blob keys |
| Network | mDNS (subnet-wide broadcast) | mDNS + optional manual peering for cross-subnet |

### 13.3 Firewall Checklist

```bash
# macOS
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --listapps

# Linux
sudo ufw status verbose
```

---

## 14. Reference — All Scripts & Configs

| File | Purpose |
|------|---------|
| [scripts/setup_env.sh](scripts/setup_env.sh) | Unified dependency checker + directory bootstrapper |
| [scripts/run_node_mac.sh](scripts/run_node_mac.sh) | macOS ControlPlane + eo-agent REPL launcher |
| [scripts/run_node_linux.sh](scripts/run_node_linux.sh) | Linux Execution/Storage node launcher |
| [scripts/node_extension.yaml](scripts/node_extension.yaml) | New device onboarding template (3rd+ device) |
| [configs/mac-node.yaml](configs/mac-node.yaml) | macOS node capabilities & roles |
| [configs/linux-node.yaml](configs/linux-node.yaml) | Linux node capabilities & roles |
| [crates/node/src/bootstrap.rs](crates/node/src/bootstrap.rs) | Rust node startup sequence (7 steps) |
| [crates/node/src/cli.rs](crates/node/src/cli.rs) | CLI flags reference |
| [crates/ipc/src/handler.rs](crates/ipc/src/handler.rs) | JSON-RPC method implementations |
| [eo-agent/src/eo_agent/main.py](eo-agent/src/eo_agent/main.py) | eo-agent CLI entrypoint |

### Environment Variable Quick Reference

| Variable | Scope | Default | Description |
|----------|-------|---------|-------------|
| `EO_HOME` | Both | `~/.eo_storage` | Storage root |
| `EO_IPC_SOCKET` | Both | `/tmp/eo_control.sock` | UDS socket path |
| `EO_MOCK_MODE` | Python | `false` | Mock IPC toggle |
| `EO_LLM_MODEL` | Python | `gpt-4o` | LLM model selection |
| `DEEPSEEK_API_KEY` | Python | (none) | LLM API key |
| `RUST_LOG` | Rust | `info` | Log verbosity |
| `OPENAI_API_KEY` | Python | (none) | Alternative LLM key |
| `ANTHROPIC_API_KEY` | Python | (none) | Alternative LLM key |

### Build & Test Commands

```bash
# Rust — build
cargo build --release --workspace

# Rust — tests (51 tests, 8 crates)
cargo test --workspace

# Python — tests (14 tests)
cd eo-agent && source .venv/bin/activate && python -m pytest tests/ -v

# Python — run in mock mode
cd eo-agent && source .venv/bin/activate && eo-agent "topology" --mock

# Format & lint
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
```

---

*Runbook maintained by the Edge-Cloud Orchestrator team. Updated for v0.1.0 — June 2026.*
