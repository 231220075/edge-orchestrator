# Edge-Cloud Orchestrator (v3)

A lightweight distributed edge orchestration platform in Rust.

Three highlights:
1. Real distributed consensus — Raft over libp2p: node registration, role
   assignment and kill-leader re-election.
2. Verifiable project execution — a project workspace (sources + build/run
   commands) is packed, routed to a capable node and executed inside a KVM
   virtual machine (qlean), with results and timings returned to the caller.
3. Content-addressed storage — Git-model CAS (`blob`/`tree`/`commit`). The
   workspace itself travels through it: the task message carries only a hash and
   the executor pulls the bytes on demand, so project size is not a protocol
   limit anymore (`docs/v3-modules/23-快照CAS分发.md`).

## Architecture

- Control plane: tikv/raft-rs RawNode, static 3-node cluster over libp2p.
  The replicated log currently carries node registration and role changes;
  task scheduling through the log is the next milestone
  (`runtime_loop::spawn_runtime` is explicit scaffolding for it).
- Data plane: Git-model CAS store + blob-exchange protocol over libp2p
  (pull-only: a node asks a peer for a missing blob and caches it).
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

Measured on a Linux + KVM host (see `docs/v3-modules/26-项目完成度报告.md` for the
full evidence table):

| path | cost |
|---|---|
| image preparation (first time only) | 31 s |
| VM cold boot | 15 s |
| snapshot upload (local CAS to guest) | 24 ms |
| warm task, toolchain already present | **31–62 ms** |
| cold task that installs gcc (fast mirror, deb-src off) | 18 s |
| 4 MB workspace, pulled from the submitting node's CAS | verified in stage F |

`diagnose_cluster.sh` runs seven stages (env → mesh → VM → guest network → mirrors
→ toolchain → CAS distribution → isolation) and each one prints its verdict.

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

- `docs/v3-modules/26-项目完成度报告.md` — **start here**: what is verified, what
  is not, and what is next.
- `docs/v3-modules/21-阶段总结与下一步分析.md` (how we got here),
  `22-wasm-lane移除记录.md` (why the Wasm lane was removed),
  `23-快照CAS分发.md`, `24-per-task隔离.md`, `25-工具链模板镜像.md`.
- `docs/v3-architecture.md`, `plan-v2/` — earlier design records; they still
  describe the removed Wasm lane, see the note in 22.

## Dev

- cargo clippy --workspace --all-targets (clean)
- cargo fmt --all -- --check (clean)
