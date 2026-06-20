# Operational Runbook

Quick reference for common production incidents on custom-dmq v2.0.0 clusters.

## Health checks

| Endpoint | Port | Meaning |
|----------|------|---------|
| `/health` | `DMQ_METRICS_PORT` (9080) | Process is alive |
| `/ready` | `DMQ_METRICS_PORT` | Broker finished recovery and can serve |
| `/metrics` | `DMQ_METRICS_PORT` | Prometheus scrape target |

```bash
curl -sf localhost:9080/ready
```

## Lag spike

**Symptoms:** `GET_LAG` or `/metrics` shows growing consumer lag; fetch latency histograms increase.

**Steps:**

1. Confirm consumers are running and sending `GROUP_HEARTBEAT`.
2. Check `dmq_fetch_requests_total` vs `dmq_produce_requests_total` on the leader.
3. Scale consumers or add partitions via admin `create` (requires cluster controller).
4. If a single partition is hot, verify range assignment is balanced across members.

## Disk full

**Symptoms:** produce errors, `ENOSPC` in broker logs, `/ready` returns 503.

**Steps:**

1. Check `DMQ_DATA_DIR` usage: `du -sh $DMQ_DATA_DIR`.
2. Lower retention via topic config or purge old log segments manually (stop broker first).
3. Expand the volume (K8s PVC resize or host disk).
4. Restart broker and verify `/ready`.

## Broker down

**Symptoms:** clients get connection refused or `R_NOT_LEADER`.

**Steps:**

1. Check process / pod status: `docker compose ps` or `kubectl get pods`.
2. Inspect logs for panic or TLS misconfiguration.
3. In cluster mode, verify another broker promoted as leader (`GET_CLUSTER`).
4. Restart the failed broker; it rejoins via `BROKER_HEARTBEAT`.

## Leader failover

**Symptoms:** produces fail with `R_NOT_LEADER` during a broker outage.

**Expected behavior:** in-sync follower promotes within `DMQ_HEARTBEAT_TIMEOUT_MS` (default 5s).

**Steps:**

1. Confirm the old leader is actually down (not a network partition).
2. Wait for epoch increment in cluster state file under broker-1 data dir.
3. Clients should retry against updated leader from `GET_CLUSTER`.
4. If failover stalls, check broker-1 controller logs and replica ISR status.

## TLS / auth failures

**Symptoms:** handshake rejected, `401` equivalent on wire.

**Steps:**

1. Verify `DMQ_TLS_CERT` / `DMQ_TLS_KEY` paths inside the container.
2. Confirm client sends matching `DMQ_AUTH_TOKEN` in handshake.
3. Check ACL file (`DMQ_ACL`) allows the principal for the topic operation.
