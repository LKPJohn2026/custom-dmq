//! Request size limits for produce and fetch backpressure.

use std::io;

pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 255;
pub const DEFAULT_MAX_FETCH_BYTES: u32 = 64 * 1024;

pub fn max_payload_bytes() -> usize {
    std::env::var("DMQ_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_PAYLOAD_BYTES)
}

pub fn max_fetch_bytes() -> u32 {
    std::env::var("DMQ_MAX_FETCH_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_FETCH_BYTES)
}

pub fn validate_produce_payload(len: usize) -> io::Result<()> {
    let max = max_payload_bytes();
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("payload {len} bytes exceeds limit {max}"),
        ));
    }
    Ok(())
}

pub fn clamp_fetch_bytes(requested: u32) -> u32 {
    requested.min(max_fetch_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_fetch_respects_ceiling() {
        assert_eq!(clamp_fetch_bytes(1_000_000), max_fetch_bytes());
    }

    #[test]
    fn validate_rejects_oversized_payload() {
        let err = validate_produce_payload(max_payload_bytes() + 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
