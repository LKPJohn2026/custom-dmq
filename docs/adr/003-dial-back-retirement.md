# ADR 003: Dial-back retirement

## Status

Accepted (v1.6.0)

## Context

Dial-back required clients to expose inbound ports, complicating firewall rules, container networking, and cloud deployment.

## Decision

Disable dial-back by default. Enable only with `DMQ_LEGACY_DIALBACK=1` for v1.0.0 compatibility demos.

## Consequences

- New deployments use pull-based fetch exclusively.
- Push integration tests remain behind the legacy flag.
- Documentation and docker-compose demos use broker-centric clients.
