# Edge-Cloud Orchestrator

A production-quality distributed orchestration platform built in Rust.

## Architecture

- **Control Plane**: Raft consensus (tikv/raft-rs) maintaining cluster state
- **P2P Mesh**: libp2p with mDNS discovery, Noise encryption, Yamux multiplexing
- **Sandbox Layer**: Polymorphic execution environment (Wasmtime + Linux containers)
- **Storage**: Git-model content-addressed storage with P2P distribution

## Quick Start

```bash
# Build everything
cargo build --workspace

# Run tests
cargo test --workspace

# Start a node
cargo run -p node -- --config configs/node.yaml
```

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `core` | Shared types, traits, error definitions |
| `p2p` | libp2p network layer (mDNS, TCP, Noise) |
| `raft` | Raft consensus integration |
| `storage` | Git-model content-addressed storage |
| `sandbox` | Polymorphic sandbox (Wasmtime + containers) |
| `orchestration` | Role engine, scheduler, topology |
| `node` | Binary: the node process |

## Development

- Rust stable (see `rust-toolchain.toml`)
- `cargo build --workspace` — zero warnings
- `cargo test --workspace` — all tests pass
- `cargo clippy --workspace -- -D warnings` — clean
- `cargo fmt --all -- --check` — formatted

## License

MIT OR Apache-2.0
