# Architecture — custom-dmq

## Components

```text
custom-dmq server          → broker (TCP :7777)
custom-dmq producer P T    → binds :P, registers topic T, accepts dial-back
custom-dmq consumer P T G  → binds :P, registers group G on topic T, receives push
```

## Wire protocol

Binary frames: `[length][type][payload]`

| Type | Value | Direction |
|------|-------|-----------|
| ECHO | 1 | client → broker |
| P_REG | 2 | producer → broker |
| C_REG | 3 | consumer → broker |
| PCM | 4 | producer → broker (dial-back) |
| R_* | 101–104 | responses |

## Push delivery flow

```mermaid
sequenceDiagram
    participant C as Consumer
    participant B as Broker
    participant P as Producer

    C->>B: C_REG (port, topic, group)
    B->>C: R_C_REG + dial-back
    P->>B: P_REG + dial-back
    P->>B: PCM payload
    B->>B: append to topic log
    B->>C: PCM push
    C->>B: R_PCM ack
    B->>B: advance group offset
```

## Recent changes (vs prior text-protocol broker)

| Area | Before | Now |
|------|--------|-----|
| Wire format | Newline text (`REGISTER_PRODUCER`, `CONSUME`) | Binary `P_REG` / `C_REG` / `PCM` frames |
| Consumer API | Pull via `CONSUME` on broker port | Push on dial-back connection |
| Topic key | String name | `u16` topic id |
| Groups | Flat `HashMap` on broker | `ConsumerGroup` list per topic |
| Entry point | Single binary with embedded producer | `server` / `producer` / `consumer` CLI |
| Offsets | Per-group string keys | Per-group `u64` into shared topic log |

## Design notes

**Push vs pull.** Consumers no longer poll the broker. A background task per group reads at the group offset and writes PCM to a ready consumer; the offset advances after `R_PCM`.

**Non-destructive log.** `read_at(offset)` leaves records in place so independent groups can read the same topic at different positions. Retention is bounded by the ring buffer capacity.

**Dial-back registration.** Producers and consumers bind a local port, register with the broker, then accept an outbound connection from the broker for PCM traffic.

## Module map

| Module | Role |
|--------|------|
| `message.rs` | Binary encode/decode |
| `topic.rs` | Ring-buffer log + topic consumer groups |
| `cgroup.rs` | Group offset, consumer handles |
| `broker.rs` | Topic registry, produce, push delivery loop |
| `producer.rs` | Producer client |
| `consumer_client.rs` | Push consumer client |

## Running locally

```bash
cargo run -- server
cargo run -- consumer 7779 1 1
cargo run -- producer 7778 1 --simulate
```

## CI

GitHub Actions (`.github/workflows/ci.yml`): `fmt` → `clippy` → `cargo test` → release build.
