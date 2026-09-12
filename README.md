# Edge-Cloud Orchestrator (v3)

A lightweight distributed edge orchestration platform in Rust.

Three highlights:
1. Real distributed consensus - Raft over libp2p, kill-leader re-election.
2. Verifiable sandbox - Wasmtime StoreLimits + epoch interruption.
3. Content-addressed storage - Git-model CAS dedup.

## Architecture

- Control plane: tikv/raft-rs RawNode, static 3-node cluster over libp2p.
- Data plane: Git-model CAS store.
- Execution: Sandbox trait; Wasmtime (real limits) + container (honest stub).
- Single binary node + JSON-RPC over UDS.

## Quick start

```bash
cargo build --workspace
cargo test --workspace
./scripts/integration_3node_test.sh
```

## Crates

| Crate | Purpose |
|---|---|
| core | types, traits, errors |
| p2p | libp2p network |
| sandbox | Wasmtime sandbox |
| storage | Git-model CAS |
| node | binary; raft/orchestration/ipc internal |

## Docs

- docs/v3-modules/ per-module records
- plan-v2/06-v3重构总方案.md final plan

## Dev

- cargo clippy --workspace --all-targets -- -D warnings (clean)
- cargo fmt --all -- --check (clean)
