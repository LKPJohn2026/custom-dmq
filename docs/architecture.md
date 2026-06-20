# Architecture — custom-dmq v2.0.0

## Overview

custom-dmq is a Kafka-shaped distributed message queue: one append-only log per topic-partition, pull-based fetch with explicit offset commits, multi-broker replication with automatic leader failover, and a consumer group coordinator.

```text
┌─────────────┐     PRODUCE/FETCH/COMMIT      ┌──────────────────────────────┐
│ dmq-produce │ ────────────────────────────▶ │  dmq-broker (leader)         │
│ dmq-consume │ ◀──────────────────────────── │  partition logs + coordinator │
└─────────────┘                               │  replication → followers     │
                                              └──────────────────────────────┘
```

## Workspace layout

| Crate | Role |
|-------|------|
| `dmq-protocol` | Wire messages and v2 framing |
| `dmq-storage` | Partition logs, mmap legacy queues, metadata, idempotency |
| `dmq-core` | Topics, groups, cluster config, auth, ACL, metrics |
| `dmq-broker` | Broker runtime, replication, coordinator, TLS client |
| `dmq-cli` | Binaries: `custom-dmq`, `dmq-broker`, `dmq-produce`, `dmq-consume`, `dmq-admin` |

## Storage model

- **Canonical path:** `log_data_{topic}_{partition}.dat` — append-only segment per topic-partition.
- **Offsets:** durable per `(group_id, topic_id, partition_id)` under `DMQ_DATA_DIR`.
- **Legacy (gated):** mmap per-group queues when `DMQ_LEGACY_PUSH=1`.

## Clustering

- Static TOML bootstrap (`DMQ_CLUSTER_CONFIG`) seeds topology.
- Broker 1 runs embedded controller; `BROKER_HEARTBEAT` tracks liveness.
- Leader failure → ISR follower promotion with epoch fencing.
- `DMQ_ACKS=leader|all` controls produce durability.

## Protocol

- **v1:** length-prefixed frames (backward compatible).
- **v2:** handshake, `correlation_id`, optional TLS + bearer token.
- See [`protocol.md`](protocol.md) for the full frame catalog.

## Consumer groups

- `JOIN_GROUP` / `GROUP_HEARTBEAT` with range partition assignment.
- Rebalance on member join/leave.
- Fetch at committed offset; `COMMIT` advances position.

## Operations

| Surface | Default |
|---------|---------|
| Broker TCP | `:7777` |
| Metrics/health | `:9080` (`/metrics`, `/health`, `/ready`) |
| Auth | `DMQ_AUTH_TOKEN` (optional) |
| TLS | `DMQ_TLS_CERT` + `DMQ_TLS_KEY` (optional) |

## Deployment

- **Docker Compose:** 3-broker cluster (`docker compose up`).
- **Kubernetes:** StatefulSet manifests in `deploy/k8s/`.
- **Helm:** `deploy/helm/custom-dmq/`.

## ADRs

- [001: Broker-centric API](adr/001-broker-centric-api.md)
- [002: Append-only log storage](adr/002-append-only-log-storage.md)
- [003: Dial-back retirement](adr/003-dial-back-retirement.md)

## CI/CD

- `ci.yml` — fmt, clippy, test, audit, deny, release build
- `integration.yml` — docker-compose e2e
- `release.yml` — tag → binaries + GHCR image

## Running locally

```bash
cargo run -p dmq-cli --bin dmq-broker
cargo run -p dmq-cli --bin dmq-produce -- 1 --simulate
cargo run -p dmq-cli --bin dmq-consume -- 1 1
```

Or the umbrella CLI:

```bash
cargo run -p dmq-cli --bin custom-dmq -- server
```
