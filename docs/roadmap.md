# Roadmap: v1.0.0 → v2.0.0

This document describes the starting baseline (`v1.0.0`), the production target (`v2.0.0`), and the phased plan to get there. Each phase builds on the previous one; later phases assume earlier semantics are stable.

---

## v1.0.0 — Local-first baseline

`v1.0.0` is a **single-broker, localhost-oriented message queue** that borrows Kafka vocabulary (topics, consumer groups, partitions) while optimizing for simplicity on one machine.

### What it includes

| Area | Behavior |
|------|----------|
| **Process model** | One broker process (`custom-dmq server`) on TCP port 7777 |
| **Wire protocol** | Length-prefixed binary frames: `ECHO`, `P_REG`, `C_REG`, `PCM`, `R_*` |
| **Client connectivity** | **Dial-back**: producer/consumer bind a local port, register with the broker, then accept an outbound connection from the broker |
| **Producer flow** | Register → broker dials back → send `PCM` payloads on persistent connection |
| **Consumer flow** | Register with group id → broker dials back → consumer sends `R_PCM` (ready) → broker pushes one `PCM` message |
| **Topics** | Identified by `u16` topic id; auto-created on first producer registration |
| **Consumer groups** | Per-topic groups; each group owns its own partition queues |
| **Partitions** | Belong to the **consumer group**, not the topic. A second consumer in a group adds a new partition rather than rebalancing existing ones |
| **Produce routing** | Messages fan out: each registered group receives a copy routed to its shortest partition |
| **Staging queue** | Messages produced before any group exists buffer in a topic staging queue, then drain when a group registers |
| **Storage** | mmap ring buffers (`underArr_*`, `underSize_*`) plus small metadata files under `DMQ_DATA_DIR` |
| **Delivery semantics** | **Destructive pop** — messages are removed from the queue when delivered; no replay |
| **Offsets** | Implicit in queue head/tail; not exposed as a durable consumer commit API |
| **Persistence** | Survives broker restart via mmap files and metadata recovery |
| **CLI** | `server`, `producer`, `consumer` subcommands |
| **Tests & CI** | Unit tests, push-path integration tests, GitHub Actions (fmt → clippy → test → release build) |

### What v1.0.0 is not

- Not Kafka-compatible (no `rdkafka`, no standard CLI tools)
- Not a shared topic-level commit log — each group has independent queues
- Not cluster-ready — single process, no replication, no leader election
- Not operable at scale — no admin API, metrics, or health endpoints

`v1.0.0` is the **preserved teaching baseline**: dial-back networking, mmap queues, push delivery, and per-group fan-out. All production work starts from here without breaking this path until a later phase explicitly retires it.

---

## v2.0.0 — Production target

`v2.0.0` is a **Kafka-shaped distributed message queue** that remains runnable locally but meets production expectations for semantics, clustering, and operations.

### Definition of done

| Capability | Target |
|------------|--------|
| **Storage** | One append-only log per topic-partition; all consumer groups read the same log at independent offsets |
| **Delivery** | Pull-based fetch with explicit offset commit; replay and lag are first-class |
| **Networking** | Clients connect to the broker; dial-back retired for the data plane |
| **Clustering** | 3+ brokers, leader/follower replication, dynamic metadata, automatic leader failover |
| **Consumer groups** | Coordinator with partition assignment and rebalance on join/leave |
| **Correctness** | Idempotent produce, at-least-once end-to-end, documented delivery semantics |
| **Operations** | Admin API, Prometheus metrics, health/readiness probes, structured logging |
| **Deployment** | Docker Compose one-command stack, Kubernetes manifests, CI that builds and tests containers |
| **Security** | TLS on client and inter-broker connections; basic authentication |

### Non-goals for v2.0.0

These are explicitly out of scope; document awareness without blocking the release:

- Full Kafka protocol compatibility
- KRaft/ZooKeeper-equivalent external metadata quorum
- Exactly-once / transactional messaging
- Schema registry, compaction, tiered storage
- Multi-tenant ACLs and quota enforcement at Kafka scale

---

## Phased plan

```text
v1.0.0  local-first baseline (dial-back, mmap, push)
   │
   ▼ Phase 0 — Scaffold
   ▼ Phase 1 — Kafka-shaped single broker
   ▼ Phase 2 — Operationally usable single broker
   ▼ Phase 3 — Multi-broker cluster (minimum viable)
   ▼ Phase 4 — Production polish
   ▼ Phase 5 — Dynamic cluster + unified storage
   ▼ Phase 6 — Security, performance, protocol hardening
   ▼ Phase 7 — Engineering maturity
v2.0.0  production-ready distributed log
```

---

## Phase 0 — Scaffold

**Target release:** foundation for v1.1.0  
**Duration:** 1–2 days

### Goal

Prepare the codebase for deep rewrites without thrashing. No user-visible behavior change.

### Work

- Introduce clear module boundaries: `storage`, `protocol`, `broker`, `admin`
- Add a `Storage` trait so the broker can swap in-memory and persistent backends
- Add `docs/roadmap.md` (this file) with goals and non-goals
- Keep `v1.0.0` dial-back and mmap paths working unchanged

### Outcome

Architecture is ready for Phase 1 semantic changes. CI remains green throughout.

---

## Phase 1 — Kafka-shaped single broker

**Target release:** v1.1.0  
**Duration:** 1–2 weeks

### Goal

Stop being a per-group queue fan-out system. Become a **topic-partition append log** on a single broker.

### Work

**Storage model**

- Introduce topic-level partitions: one `PartitionLog` per `(topic_id, partition_id)`
- Replace destructive `pop_front` on the pull path with:
  - `append(payload) → offset`
  - `fetch(offset, max_bytes) → batch`
- Add retention policy (max records / size) at the log layer

**Consumer offsets**

- Per `(group, topic, partition)` committed offsets
- `CommitOffset(group, topic, partition, offset)` wire message
- Persist offsets to disk under `DMQ_DATA_DIR`

**Networking & protocol**

- Add pull-based messages: `PRODUCE`, `FETCH`, `COMMIT` (+ responses)
- Add CLI commands: `produce`, `fetch` (client connects to broker — no dial-back)
- Keep dial-back push path as compatibility mode for now

**Protocol foundations (start, finish in Phase 6)**

- Plan for `protocol_version` and `correlation_id` on every frame

### Outcome

Replay, lag, and offset-based consumption work on a single broker. Two consumer groups on the same topic read the same partition log at different offsets.

### Key decision

From this phase onward, the **topic-partition log is the canonical write path**. Legacy mmap fan-out remains but must not receive new features.

---

## Phase 2 — Operationally usable single broker

**Target release:** v1.2.0  
**Duration:** 1–2 weeks

### Goal

Run the single broker continuously with admin tooling, observability, and hardening tests.

### Work

**Admin API**

- `CREATE_TOPIC`, `DESCRIBE_TOPIC`, `LIST_TOPICS`, `GET_LAG`
- CLI: `custom-dmq admin create|describe|list|lag`

**Observability**

- Prometheus metrics endpoint (`/metrics`): produce/fetch bytes, request counts, errors
- Structured logs with request type and peer address

**Backpressure & limits**

- Bounded produce payload size (`DMQ_MAX_PAYLOAD_BYTES`)
- Bounded fetch response size (`DMQ_MAX_FETCH_BYTES`)
- Fetch batching via `max_bytes` on the existing fetch API

**Persistence hardening**

- Durable partition logs on disk (`log_data_{topic}_{partition}.dat`)
- Crash recovery test: append → restart → fetch verifies records
- Topic config persistence (partition count, retention)

### Outcome

A single broker you can run with basic SLO signals: metrics, admin visibility, payload limits, and proven restart recovery on the pull path.

---

## Phase 3 — Multi-broker cluster (minimum viable)

**Target release:** v1.3.0  
**Duration:** 2–4 weeks

### Goal

Survive a broker failure without data loss. Demo a real HA story: 3 brokers, replication factor 3.

### Work

**Static metadata / control plane (v1.3 scope)**

- TOML cluster config: broker registry, partition assignments, leaders, replicas
- `DMQ_BROKER_ID`, `DMQ_CLUSTER_CONFIG` env vars
- `GET_CLUSTER` wire message for clients to discover topology

**Replication**

- Leader accepts produces; fans out `REPLICATE` frames to followers
- Followers apply records idempotently by offset
- `DMQ_ACKS=leader|all` with `min_insync_replicas`

**Client routing**

- Producers and fetchers resolve the partition leader from cluster config
- Non-leaders respond with `R_NOT_LEADER(leader_broker_id)`

**Static partition assignment**

- Partition leaders and replica sets defined in config file
- No automatic reassignment yet — manual config edit for topology changes

### Outcome

3-broker local cluster with leader/follower replication. Tolerates one follower failure. Leader failure requires manual intervention (addressed in Phase 5).

---

## Phase 4 — Production polish

**Target release:** v1.4.0  
**Duration:** 1–2 weeks

### Goal

Resume-grade operability: durability tuning, retry-safe producers, deployment artifacts.

### Work

**Durability**

- Configurable fsync policy: `DMQ_FSYNC=always|never|every:N`

**Correctness**

- Idempotent producer: `IDEMPOTENT_PRODUCE` with `(producer_id, sequence)` dedup
- Persist idempotency state across restart
- CLI: `produce --idempotent` with `DMQ_PRODUCER_ID`

**Operations**

- HTTP ops port: `/metrics`, `/health`, `/ready`
- Structured request logging on the broker TCP handler

**Deployment**

- `Dockerfile` and `docker-compose.yml` for 3-broker local cluster
- Kubernetes manifests: StatefulSet, headless Service, ConfigMap, PVC per broker

### Outcome

Deployable demo stack via Docker or K8s. Producers survive retries without duplicates. Ops probes ready for orchestrators.

---

## Phase 5 — Dynamic cluster + unified storage

**Target release:** v1.5.0  
**Duration:** 2–4 weeks

### Goal

Replace static config with a live control plane. Retire the dual storage model. Close the biggest gap between v1.4 and v2.0.

### Work

**Dynamic metadata / control plane**

- Embedded Raft (or dedicated controller service) for cluster metadata
- Track live broker membership, partition leaders, replica sets, leader epochs
- Replace static TOML as the source of truth (TOML remains valid for bootstrap seed)

**Automatic leader failover**

- Detect leader failure via heartbeats
- Promote in-sync follower; fence stale leader
- Integration tests: kill leader mid-produce → cluster recovers without manual config edit

**Unified storage**

- Make topic-partition log the **only** write path
- Retire or gate legacy mmap per-group fan-out behind a `--legacy-push` flag
- `produce_pcm` routes through `append_log` only; remove duplicate copies per group

**Consumer group coordinator**

- `JoinGroup`, `Heartbeat`, partition assignment (range assigner)
- Rebalance when consumers join or leave a group
- Assign topic partitions (not per-group queues) to group members

### Outcome

True Kafka-shaped semantics end-to-end: one log, many groups, dynamic HA, automatic failover, coordinated consumption.

---

## Phase 6 — Security, performance, protocol hardening

**Target release:** v1.6.0  
**Duration:** 2–3 weeks

### Goal

Harden the wire protocol and runtime for untrusted networks and higher throughput.

### Work

**Protocol**

- `protocol_version` on handshake; reject unknown versions with clear errors
- `correlation_id` on every request/response pair
- Retire dial-back for the data plane; clients always connect to broker (or load balancer)

**Security**

- TLS on client-facing and inter-broker TCP connections
- Basic authentication (token or mTLS client certs)
- Optional ACLs: produce/fetch/admin per topic

**Performance**

- Payload compression (lz4 or zstd) on produce/fetch batches
- Fetch `max_wait_ms` for efficient long-polling
- Request latency histograms in Prometheus metrics
- Evaluate zero-copy reads from log segments

**Fetch improvements**

- Followers serve read-only fetch (eventual consistency) or redirect to leader
- Document consistency guarantees per ack mode

### Outcome

Broker suitable for deployment outside localhost. Protocol is versioned and observable. Throughput and latency are measurable and tunable.

---

## Phase 7 — Engineering maturity

**Target release:** v2.0.0  
**Duration:** 2–3 weeks

### Goal

Ship a credible systems project: reproducible releases, full CI/CD, documentation pack, and operational runbooks.

### Work

**Repository structure**

- Cargo workspace: `dmq-core`, `dmq-protocol`, `dmq-storage`, `dmq-broker`, `dmq-cli`
- Separate binaries: `dmq-broker`, `dmq-produce`, `dmq-consume`, `dmq-admin`
- Commit `Cargo.lock`; pin toolchain in `rust-toolchain.toml`

**CI/CD**

- `ci.yml`: fmt, clippy, test, `cargo audit`, `cargo deny`
- `integration.yml`: docker-compose e2e (produce → fetch → commit → restart)
- `release.yml`: tag → cross-build binaries + push container to GHCR
- Chaos test in CI: kill broker during produce, verify recovery

**Documentation**

- Update `docs/architecture.md` for v2 semantics
- Add `docs/protocol.md` (frame catalog, versioning)
- Add `docs/adr/` (broker-centric API, storage format, dial-back retirement)
- Add `docs/runbook.md` (lag spike, disk full, broker down, failover)
- Add `CHANGELOG.md` with semver history

**Deployment polish**

- Helm chart (or Kustomize overlays) on top of existing K8s manifests
- Rolling update validation with readiness probes
- Optional Grafana dashboard JSON committed to repo
- `docker compose up` includes sample producer + consumer for one-command demo

**Testing**

- Property tests (`proptest`) for log monotonicity and offset invariants
- Multi-broker failover integration suite
- Load benchmark (`criterion`) with throughput numbers in README

### Outcome

`v2.0.0` — a deliberately scoped distributed log with modern engineering practices. Resume narrative: append-only segmented log, consumer groups with offset commits, leader replication, K8s deployment, and a full CI/CD pipeline.

---

## Version milestones

| Version | Phase | Summary |
|---------|-------|---------|
| **v1.0.0** | — | Local-first baseline: dial-back, mmap, push delivery |
| **v1.1.0** | Phase 1 | Topic-partition log, fetch/commit, pull CLI |
| **v1.2.0** | Phase 2 | Admin API, metrics, limits, log persistence |
| **v1.3.0** | Phase 3 | 3-broker static cluster, replication |
| **v1.4.0** | Phase 4 | Fsync, idempotent produce, Docker/K8s |
| **v1.5.0** | Phase 5 | Dynamic metadata, failover, unified log, rebalance |
| **v1.6.0** | Phase 6 | TLS, protocol versioning, compression, performance |
| **v2.0.0** | Phase 7 | Workspace split, full CI/CD, docs, Helm, v2 release |

---

## Architectural through-line

Every phase after v1.0.0 moves toward four properties that define a Kafka-shaped broker:

1. **Topic-level partitions** — one append log per `(topic, partition)`
2. **Pull-based fetch** — consumers read at offset, commit explicitly
3. **Committed offsets** — durable per `(group, topic, partition)`
4. **Append-only logs** — non-destructive storage with retention

If a feature does not advance one of these four, it belongs in a later phase or in the non-goals list.

---

## Reference

- Design notes: [`architecture.md`](architecture.md)
- Cluster example config: [`../config/cluster.example.toml`](../config/cluster.example.toml)
- Kubernetes manifests: [`../deploy/k8s/`](../deploy/k8s/)
