# Edge-Cloud Orchestrator (v3)

A lightweight distributed edge orchestration platform in Rust.

Three highlights:
1. Real distributed consensus — Raft over libp2p: node registration, role
   assignment and kill-leader re-election.
2. Verifiable project execution — a project workspace (sources + build/run
   commands) is packed, routed to a capable node and executed inside a KVM
   virtual machine (qlean), with results and timings returned to the caller.
3. Content-addressed storage — Git-model CAS (`blob`/`tree`/`commit`) with CAS
   hashes used for code/result addressing.

## Architecture

- Control plane: tikv/raft-rs RawNode, static 3-node cluster over libp2p.
  The replicated log currently carries node registration and role changes;
  task scheduling through the log is the next milestone
  (`runtime_loop::spawn_runtime` is explicit scaffolding for it).
- Data plane: Git-model CAS store, plus a blob-exchange protocol over libp2p.
- Execution: `ProjectSandbox` trait; qlean (QEMU/KVM) is the only backend.
  A node advertises `project_sandbox: true` only if it can really run one.
- Client: `eo-agent` (scan workspace -> plan build/run -> submit -> analyse).
- Single binary node + JSON-RPC over a Unix domain socket.

## Quick start

```bash
cargo build --workspace
cargo test --workspace

# host preflight (bridge, /dev/kvm, bridge-helper, upstream reachability);
# --fix repairs what it can. Also run automatically by the scripts below.
./scripts/host_network_check.sh

# end-to-end: 3 nodes + one project task (cold then warm VM reuse)
./scripts/verify_project_e2e.sh

# same, driven by the eo-agent heuristic planner
./scripts/verify_agent.sh

# layer-by-layer triage: env -> mesh -> VM boot -> toolchain -> build
./scripts/diagnose_cluster.sh
```

Measured on a Linux + KVM host: image prep 31–33 s (first time only), VM cold
boot 14.2–14.8 s, snapshot upload 25–49 ms, and a **warm** task (VM reused,
toolchain present) completes in ~60 ms. A cold first task that must install gcc
takes 48–96 s depending on mirror bandwidth.

## Crates

| Crate | Purpose |
|---|---|
| core | types, traits, errors |
| p2p | libp2p network (request-response protocols, discovery) |
| sandbox | qlean KVM project sandbox (Linux + KVM only) |
| storage | Git-model CAS |
| node | binary; raft/orchestration/ipc internal |
| agent | eo-agent CLI (workspace -> plan -> submit -> analyse) |

## Docs

- `docs/v3-modules/` — per-module records. Start with
  `21-阶段总结与下一步分析.md` (current state) and
  `22-wasm-lane移除记录.md` (why the Wasm lane was removed).
- `docs/v3-architecture.md`, `plan-v2/` — earlier design records; they still
  describe the removed Wasm lane, see the note in 22.

## Dev

- cargo clippy --workspace --all-targets (clean)
- cargo fmt --all -- --check (clean)
