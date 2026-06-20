//! Wire framing: v1 (legacy) and v2 (correlation id + u16 length).

use crate::message::{self, Message};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_V1: u16 = 1;
pub const PROTOCOL_V2: u16 = 2;
pub const MAX_PROTOCOL_VERSION: u16 = PROTOCOL_V2;

/// v2 body layout after the u16 length prefix:
/// `[version: u8=2][correlation_id: u32 BE][msg_type: u8][payload...]`
pub const V2_HEADER_LEN: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub correlation_id: u32,
    pub message: Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    V1,
    V2,
}

impl Frame {
    pub fn v1(message: Message) -> Self {
        Self {
            correlation_id: 0,
            message,
        }
    }

    pub fn v2(correlation_id: u32, message: Message) -> Self {
        Self {
            correlation_id,
            message,
        }
    }
}

pub fn legacy_dialback_enabled() -> bool {
    match std::env::var("DMQ_LEGACY_DIALBACK") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

pub fn require_handshake() -> bool {
    match std::env::var("DMQ_REQUIRE_HANDSHAKE") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

pub fn negotiated_version_from_env() -> u16 {
    std::env::var("DMQ_PROTOCOL_VERSION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(PROTOCOL_V2)
        .clamp(PROTOCOL_V1, MAX_PROTOCOL_VERSION)
}

pub fn validate_protocol_version(requested: u16) -> Result<u16, u8> {
    if requested == 0 || requested > MAX_PROTOCOL_VERSION {
        Err(1)
    } else {
        Ok(requested.min(MAX_PROTOCOL_VERSION))
    }
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<(Frame, WireFormat)> {
    let first = reader.read_u8().await?;
    if first == 0xFF {
        let second = reader.read_u8().await?;
        if second != 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported extended frame marker {second}"),
            ));
        }
        let len = reader.read_u16().await? as usize;
        let mut body = vec![0u8; len];
        reader.read_exact(&mut body).await?;
        parse_v2_body(&body)
    } else {
        let length = first as usize;
        if length == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty v1 frame"));
        }
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).await?;
        let message = message::parse_frame(&body)?;
        Ok((Frame::v1(message), WireFormat::V1))
    }
}

fn parse_v2_body(body: &[u8]) -> io::Result<(Frame, WireFormat)> {
    if body.len() < V2_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "v2 frame too short",
        ));
    }
    let version = body[0];
    if version != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported frame version {version}"),
        ));
    }
    let correlation_id = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
    let message = message::parse_frame(&body[5..])?;
    Ok((Frame::v2(correlation_id, message), WireFormat::V2))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    format: WireFormat,
) -> io::Result<()> {
    match format {
        WireFormat::V1 => message::write_message(writer, &frame.message).await,
        WireFormat::V2 => {
            let inner = message::encode_message(&frame.message)?;
            let body_len = V2_HEADER_LEN - 1 + inner.len();
            let mut wire = Vec::with_capacity(4 + body_len);
            wire.push(0xFF);
            wire.push(2);
            wire.extend_from_slice(&(body_len as u16).to_be_bytes());
            wire.push(2);
            wire.extend_from_slice(&frame.correlation_id.to_be_bytes());
            wire.extend_from_slice(&inner);
            writer.write_all(&wire).await?;
            writer.flush().await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{HandshakeRequest, Message};

    async fn roundtrip(frame: Frame, format: WireFormat) -> Frame {
        let (mut client, mut server) = tokio::io::duplex(4096);
        write_frame(&mut client, &frame, format).await.unwrap();
        let (decoded, _) = read_frame(&mut server).await.unwrap();
        decoded
    }

    #[tokio::test]
    async fn v1_frame_roundtrip() {
        let frame = Frame::v1(Message::Echo("hi".into()));
        let decoded = roundtrip(frame.clone(), WireFormat::V1).await;
        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn v2_frame_preserves_correlation_id() {
        let frame = Frame::v2(
            42,
            Message::Handshake(HandshakeRequest {
                protocol_version: 2,
                auth_token: b"secret".to_vec(),
            }),
        );
        let decoded = roundtrip(frame.clone(), WireFormat::V2).await;
        assert_eq!(decoded.correlation_id, 42);
        assert_eq!(decoded.message, frame.message);
    }

    #[test]
    fn validate_rejects_unknown_version() {
        assert!(validate_protocol_version(99).is_err());
        assert_eq!(validate_protocol_version(2).unwrap(), 2);
    }
}
