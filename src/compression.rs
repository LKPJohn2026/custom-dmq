//! Payload compression for fetch batches (lz4).

use std::io;

pub const CODEC_NONE: u8 = 0;
pub const CODEC_LZ4: u8 = 1;

pub fn compression_enabled() -> bool {
    match std::env::var("DMQ_COMPRESSION") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("lz4"),
        Err(_) => false,
    }
}

pub fn preferred_codec() -> u8 {
    if compression_enabled() {
        CODEC_LZ4
    } else {
        CODEC_NONE
    }
}

pub fn compress(data: &[u8]) -> io::Result<Vec<u8>> {
    Ok(lz4_flex::compress(data))
}

pub fn decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    lz4_flex::decompress(data, data.len() * 8)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn wrap_batch(codec: u8, payload: &[u8]) -> io::Result<Vec<u8>> {
    if codec == CODEC_NONE {
        let mut out = vec![CODEC_NONE];
        out.extend_from_slice(payload);
        return Ok(out);
    }
    if codec == CODEC_LZ4 {
        let compressed = compress(payload)?;
        if compressed.len() >= payload.len() {
            let mut out = vec![CODEC_NONE];
            out.extend_from_slice(payload);
            return Ok(out);
        }
        let mut out = Vec::with_capacity(1 + compressed.len());
        out.push(CODEC_LZ4);
        out.extend_from_slice(&compressed);
        return Ok(out);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unknown compression codec {codec}"),
    ))
}

pub fn unwrap_batch(bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty compressed batch",
        ));
    }
    match bytes[0] {
        CODEC_NONE => Ok(bytes[1..].to_vec()),
        CODEC_LZ4 => decompress(&bytes[1..]),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown compression codec {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lz4_roundtrip() {
        let data = b"hello world hello world hello world";
        let wrapped = wrap_batch(CODEC_LZ4, data).unwrap();
        assert_eq!(wrapped[0], CODEC_LZ4);
        assert_eq!(unwrap_batch(&wrapped).unwrap(), data);
    }
}
