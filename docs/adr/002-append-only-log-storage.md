# ADR 002: Append-only partition log storage

## Status

Accepted (v1.1.0, unified v1.5.0)

## Context

v1.0.0 stored messages in per-group mmap ring queues with destructive pop. Multiple consumer groups on the same topic received independent copies, preventing replay and shared retention.

## Decision

Each `(topic_id, partition_id)` has one **append-only log** (`log_data_{topic}_{partition}.dat`). All consumer groups read the same log at independent committed offsets.

## Consequences

- Replay, lag, and offset commits are first-class.
- Legacy mmap fan-out is retired in cluster mode; `DMQ_LEGACY_PUSH` gates the old path.
- Storage layout is segment-oriented and suitable for retention policies.
