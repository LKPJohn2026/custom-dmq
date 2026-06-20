# custom-dmq

> A Kafka-inspired distributed message queue in Rust (Tokio): **MySQL-first writes** → **Debezium**
> → **Kafka** → **cache invalidation**.
>
> *(No — just kidding.)* This repo is a small educational **distributed message queue**
> that borrows Kafka’s mental model (topics, consumer groups) while keeping the system
> simple enough to run on one machine.

Design notes: [`docs/architecture.md`](docs/architecture.md).

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                                local host                                │
│                                                                          │
│  producer (bind :P)                                                      │
│     │  P_REG(topic_id, port=P)                                           │
│     ▼                                                                    │
│  broker (TCP :7777) ────── dials back ──────▶ producer (TCP :P)         │
│     │                                                          │         │
│     │                           PCM(payload)                   │         │
│     │◀─────────────────────────────────────────────────────────┘        │
│     │  route: topic staging → per-group partitions                       │
│     ▼                                                                    │
│  consumer (bind :C)                                                      │
│     │  C_REG(topic_id, group_id, port=C)                                 │
│     ▼                                                                    │
│  broker (TCP :7777) ────── dials back ──────▶ consumer (TCP :C)         │
│                                              │                           │
│                                              │  R_PCM (ready)            │
│                                              ▼                           │
│                                           broker sends PCM               │
└──────────────────────────────────────────────────────────────────────────┘
```

### Key capabilities

| Area | What it does |
|------|--------------|
| **Broker** | Accepts registration requests on `DMQ_BROKER_PORT` (default `7777`), manages topics / groups / partitions. |
| **Binary protocol** | Length-prefixed frames: `ECHO`, `P_REG`, `C_REG`, `PCM`, `R_*` (see `src/message.rs`). |
| **Dial-back pattern** | Producer/consumer bind a port, register, then accept an outbound connection from the broker. |
| **Consumer groups** | Groups exist per-topic; each consumer registration gets a partition index. |
| **Partitioned storage** | Per-group partitions: messages route to the shortest partition per group for simple parallelism. |
| **Persistence (mmap)** | Queue contents + metadata are stored under `DMQ_DATA_DIR` using mmap files; survives broker restart. |
| **Tests** | Unit tests for queue/metadata invariants + integration tests for TCP delivery + persistence recovery. |
| **Pull-based API (Phase 1)** | `produce`/`fetch` commands use `PRODUCE` + `FETCH` + `COMMIT` on the broker port, backed by an append-only log. |
| **Multi-broker cluster (Phase 3)** | Static TOML cluster config, leader/follower replication, `GET_CLUSTER` metadata, and leader-aware client routing. |

### Consistency model

This system is **eventually consistent** with respect to consumers:

- Producers write to the broker over the dial-back connection.
- Consumers receive messages when they send `R_PCM` (ready) on their dial-back connection.
- If a consumer is slow or disconnected, its partition buffers until it catches up (bounded by queue capacity).

---

## Tech stack

| Layer | Choice |
|------|--------|
| Runtime | Rust 2021 + Tokio |
| Persistence | `memmap2` + small metadata files |
| CI | GitHub Actions: fmt → clippy → test → release build |

---

## Prerequisites

- Rust toolchain (stable)

---

## Quick start

### Start the broker

```bash
cargo run -- server
```

### Start a consumer (group 1, topic 1)

```bash
cargo run -- consumer 7779 1 1    # port, topic_id, group_id
```

### Start a producer (topic 1)

```bash
cargo run -- producer 7778 1 --simulate
```

### Or use the pull-based path (no dial-back)

```bash
cargo run -- produce 1 --simulate
cargo run -- fetch 1 1
```

### Multi-broker cluster (3 brokers, RF=3)

Use `config/cluster.example.toml` and start one broker per node:

```bash
DMQ_BROKER_ID=1 DMQ_BROKER_PORT=7777 DMQ_DATA_DIR=dmq-data-1 \
  DMQ_CLUSTER_CONFIG=config/cluster.example.toml cargo run -- server

DMQ_BROKER_ID=2 DMQ_BROKER_PORT=7778 DMQ_DATA_DIR=dmq-data-2 \
  DMQ_CLUSTER_CONFIG=config/cluster.example.toml cargo run -- server

DMQ_BROKER_ID=3 DMQ_BROKER_PORT=7779 DMQ_DATA_DIR=dmq-data-3 \
  DMQ_CLUSTER_CONFIG=config/cluster.example.toml cargo run -- server
```

Produce and fetch route to the partition leader automatically when `DMQ_CLUSTER_CONFIG` is set:

```bash
DMQ_CLUSTER_CONFIG=config/cluster.example.toml cargo run -- produce 1 --simulate
DMQ_CLUSTER_CONFIG=config/cluster.example.toml cargo run -- admin cluster
```

Set `DMQ_ACKS=all` to require `min_insync_replicas` followers to ack before produce succeeds.

---

## Configuration

All config is via env vars:

| Variable | Default | Purpose |
|---------|---------|---------|
| `DMQ_BROKER_PORT` | `7777` | Broker registration port |
| `DMQ_BROKER_ID` | `1` | Broker identity in a cluster |
| `DMQ_DATA_DIR` | `dmq-data` | Persistence directory for mmap + metadata |
| `DMQ_CLUSTER_CONFIG` | _(unset)_ | Path to static cluster TOML (brokers + assignments) |
| `DMQ_ACKS` | `leader` | Produce ack policy: `leader` or `all` |
| `DMQ_METRICS_PORT` | `9080` | Prometheus `/metrics` HTTP port |
| `DMQ_MAX_PAYLOAD_BYTES` | `255` | Max produce payload size |
| `DMQ_MAX_FETCH_BYTES` | `65536` | Max fetch response size |

---

## Running the tests

```bash
cargo test
```

### Development commands

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

---

## Continuous integration

GitHub Actions workflow: [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

- Rustfmt (`cargo fmt --check`)
- Clippy (`cargo clippy -D warnings`)
- Tests (`cargo test`)
- Release build (`cargo build --release`)

---

## Project structure

```text
custom-dmq/
├── src/
│   ├── broker.rs                 broker state + routing + delivery loop
│   ├── message.rs                binary wire protocol
│   ├── topic.rs                  topic staging queue + group registry
│   ├── cgroup.rs                 consumer groups + partition assignment
│   ├── partition.rs              per-group partition backed by mmap queue
│   ├── mmap_queue.rs             mmap ring buffer + per-queue metadata
│   └── metadata.rs               broker/topic/group metadata tables
├── tests/
│   ├── push_integration.rs       TCP dial-back + ready-initiated delivery
│   ├── partition_integration.rs  partition assignment + routing
│   └── persistence_integration.rs restart recovery
└── docs/
    └── architecture.md           deeper design notes
```

---

## Extending

- Add retention/backpressure knobs (queue capacity, eviction policy).
- Add admin API for listing topics / groups.
- Replace dial-back with a single long-lived client→broker socket (Kafka-like).

