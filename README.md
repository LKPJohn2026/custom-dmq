# custom-dmq

Kafka-inspired distributed message queue in Rust. Design notes: [docs/architecture.md](docs/architecture.md).

**Status:** push-based consumer groups with binary wire protocol.

## Quick start

```bash
cargo run -- server
cargo run -- consumer 7779 1 1    # port, topic_id, group_id
cargo run -- producer 7778 1 --simulate
```

## Development

```bash
cargo test
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## CI

Pull requests run Rustfmt, Clippy, unit tests, integration tests, and a release build via GitHub Actions.
