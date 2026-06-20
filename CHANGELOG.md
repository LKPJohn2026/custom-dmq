# Changelog

All notable changes to custom-dmq are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/).

## [2.0.0] - 2026-06-20

### Added

- Cargo workspace: `dmq-protocol`, `dmq-storage`, `dmq-core`, `dmq-broker`, `dmq-cli`
- Standalone binaries: `dmq-broker`, `dmq-produce`, `dmq-consume`, `dmq-admin`
- Pinned toolchain in `rust-toolchain.toml`
- CI: `cargo audit`, `cargo deny`, Docker e2e integration workflow, release workflow
- Documentation pack: ADRs, operational runbook, updated architecture guide
- Helm chart, Grafana dashboard JSON, docker-compose demo producer/consumer
- Property tests for partition log invariants; criterion throughput benchmark

### Changed

- `custom-dmq` remains the umbrella CLI; subcommands unchanged
- Version bumped to 2.0.0 across workspace crates

## [1.6.0] - 2026-06-20

### Added

- Protocol v2 framing with correlation ids and handshake
- Bearer-token authentication and topic-level ACLs
- Optional TLS for client and inter-broker connections
- LZ4 fetch compression, fetch `max_wait_ms`, latency histograms
- Dial-back disabled by default (`DMQ_LEGACY_DIALBACK`)

## [1.5.0] - 2026-06-20

### Added

- Dynamic cluster metadata with persisted leadership epochs
- Automatic leader failover via broker heartbeats
- Unified log-only produce path in cluster mode
- Consumer group coordinator with range assignment and rebalance

## [1.4.0] - 2026-06-20

### Added

- Configurable fsync policy, idempotent produce, health/readiness probes
- Docker Compose 3-broker stack and Kubernetes manifests

## [1.3.0] - 2026-06-20

### Added

- Static TOML cluster config, leader/follower replication, `GET_CLUSTER`

## [1.2.0] - 2026-06-20

### Added

- Admin API, Prometheus metrics, payload/fetch limits, durable partition logs

## [1.1.0] - 2026-06-20

### Added

- Topic-partition append log, pull-based `produce`/`fetch`/`commit` CLI

## [1.0.0] - 2026-06-20

### Added

- Local-first baseline: dial-back networking, mmap queues, push delivery
