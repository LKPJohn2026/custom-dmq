# ADR 001: Broker-centric client API

## Status

Accepted (v1.1.0, reinforced v2.0.0)

## Context

v1.0.0 used a dial-back pattern: clients bound local ports and the broker initiated outbound connections. This simplified local demos but broke behind NAT and load balancers.

## Decision

Clients connect **to** the broker for all data-plane operations (`PRODUCE`, `FETCH`, `COMMIT`). Dial-back is gated behind `DMQ_LEGACY_DIALBACK` and disabled by default from v1.6.0.

## Consequences

- Production deployments can use standard TCP load balancers and K8s Services.
- Legacy push consumers remain available for teaching but receive no new features.
- CLI binaries (`dmq-produce`, `dmq-consume`) always use broker-centric connections.
