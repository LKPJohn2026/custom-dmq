# Wire protocol (v1.6)

custom-dmq uses length-prefixed binary frames on the broker TCP port (default `7777`).

## Frame formats

### v1 (legacy)

```text
[length: u8][message_type: u8][payload...]
```

`length` includes the type byte. Maximum frame size is 255 bytes. No correlation id (implicit `0`).

### v2

```text
[0xFF][0x02][length: u16 BE][version: u8=2][correlation_id: u32 BE][message_type: u8][payload...]
```

Responses echo the request `correlation_id`. Maximum frame size is 65535 bytes.

## Handshake

v2 clients should send `HANDSHAKE` (18) as the first frame on a connection:

| Field | Type | Description |
|-------|------|-------------|
| protocol_version | u16 BE | Requested version (max supported: 2) |
| token_len | u16 BE | Auth token length |
| token | bytes | Bearer token when `DMQ_AUTH_TOKEN` is set |

Response `R_HANDSHAKE` (118): `[code: u8][negotiated_version: u16 BE]` — code `0` means success.

On failure the broker may respond with `R_ERROR` (119): `[code: u8][message UTF-8...]`.

## Message catalog

| Type | ID | Description |
|------|-----|-------------|
| PRODUCE | 7 | Append to partition log |
| FETCH | 5 | Pull records (`max_wait_ms` in bytes 16–19 for long poll) |
| COMMIT | 6 | Commit consumer offset |
| HANDSHAKE | 18 | Protocol + auth negotiation |
| GET_CLUSTER | 13 | Cluster metadata (v2 responses include leader epochs) |
| JOIN_GROUP | 16 | Consumer group join + partition assignment |
| GROUP_HEARTBEAT | 17 | Keep group membership |

Dial-back registration (`P_REG` / `C_REG`) is disabled by default in v1.6. Enable with `DMQ_LEGACY_DIALBACK=1`.

## Fetch batch encoding

`RFetch` payload: `[codec: u8][batch...]` where codec `0` = none, `1` = lz4 (when `DMQ_COMPRESSION=1`).

Batch body (after codec byte when codec=0, or decompressed):

```text
[count: u16 BE]
  repeat count:
    [offset: u64 BE][len: u16 BE][payload...]
```

## Fetch consistency

| `DMQ_FETCH_CONSISTENCY` | Behavior |
|-------------------------|----------|
| `follower` (default) | Any in-sync replica may serve fetch (eventual consistency) |
| `leader` | Non-leaders respond with `R_NOT_LEADER` |

## Security

| Variable | Purpose |
|----------|---------|
| `DMQ_AUTH_TOKEN` | Required bearer token in handshake when set |
| `DMQ_CLIENT_TOKEN` | Token sent by CLI clients |
| `DMQ_TLS_CERT` / `DMQ_TLS_KEY` | Enable TLS on broker port |
| `DMQ_TLS_CA` | CA for client TLS connections |
| `DMQ_ACL` | Semicolon rules: `principal:operation:topic_id` |
| `DMQ_ACL_DENY_BY_DEFAULT` | Deny when no rule matches |

## Produce ack consistency

| `DMQ_ACKS` | Guarantee |
|------------|-----------|
| `leader` | Ack after leader append (default) |
| `all` | Ack only when `min_insync_replicas` followers replicate |
