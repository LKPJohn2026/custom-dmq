//! Encoding for fetch responses (record batches).
//!
//! This intentionally uses a simple binary format independent of the message framing:
//!
//! ```text
//! [count:u16]
//!   repeated count times:
//!     [offset:u64][len:u16][bytes...]
//! ```
//!
//! Offsets are included so consumers can commit the next offset precisely.

use dmq_storage::partition_log::Record;
use std::io;

pub fn encode_records(records: &[Record]) -> Vec<u8> {
    let mut out = Vec::new();
    let count = u16::try_from(records.len()).unwrap_or(u16::MAX);
    out.extend_from_slice(&count.to_be_bytes());
    for rec in records.iter().take(count as usize) {
        out.extend_from_slice(&rec.offset.to_be_bytes());
        let len = u16::try_from(rec.payload.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&rec.payload[..len as usize]);
    }
    out
}

pub fn decode_records(bytes: &[u8]) -> io::Result<Vec<Record>> {
    if bytes.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "fetch batch too short",
        ));
    }
    let count = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let mut idx = 2usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if idx + 8 + 2 > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated record header",
            ));
        }
        let offset = u64::from_be_bytes([
            bytes[idx],
            bytes[idx + 1],
            bytes[idx + 2],
            bytes[idx + 3],
            bytes[idx + 4],
            bytes[idx + 5],
            bytes[idx + 6],
            bytes[idx + 7],
        ]);
        idx += 8;
        let len = u16::from_be_bytes([bytes[idx], bytes[idx + 1]]) as usize;
        idx += 2;
        if idx + len > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated record payload",
            ));
        }
        let payload = bytes[idx..idx + len].to_vec();
        idx += len;
        out.push(Record { offset, payload });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let records = vec![
            Record {
                offset: 1,
                payload: b"a".to_vec(),
            },
            Record {
                offset: 2,
                payload: b"bb".to_vec(),
            },
        ];
        let bytes = encode_records(&records);
        let decoded = decode_records(&bytes).unwrap();
        assert_eq!(decoded, records);
    }
}
