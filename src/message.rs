//! Binary wire protocol for broker registration and PCM transport.
//!
//! Replaces the previous newline text commands (`REGISTER_PRODUCER`, `CONSUME`, etc.).
//! Frame layout: `[length: u8][type: u8][payload...]`
//!
//! Registration uses one-shot broker connections; producers and consumers then
//! exchange PCM and response frames on dial-back TCP streams.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const ECHO: u8 = 1;
pub const P_REG: u8 = 2;
pub const C_REG: u8 = 3;
pub const PCM: u8 = 4;

pub const R_ECHO: u8 = 101;
pub const R_P_REG: u8 = 102;
pub const R_C_REG: u8 = 103;
pub const R_PCM: u8 = 104;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerRegister {
    pub port: u16,
    pub topic_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRegister {
    pub port: u16,
    pub topic_id: u16,
    pub group_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Echo(String),
    ProducerRegister(ProducerRegister),
    ConsumerRegister(ConsumerRegister),
    Pcm(Vec<u8>),
    REcho(String),
    RProducerRegister(u8),
    RConsumerRegister(u8),
    RPcm(u8),
}

impl ProducerRegister {
    pub fn encode(&self) -> [u8; 4] {
        let mut data = [0u8; 4];
        data[0] = (self.port >> 8) as u8;
        data[1] = (self.port & 0xff) as u8;
        data[2] = (self.topic_id >> 8) as u8;
        data[3] = (self.topic_id & 0xff) as u8;
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "P_REG payload too short",
            ));
        }
        Ok(ProducerRegister {
            port: u16::from_be_bytes([payload[0], payload[1]]),
            topic_id: u16::from_be_bytes([payload[2], payload[3]]),
        })
    }
}

impl ConsumerRegister {
    pub fn encode(&self) -> [u8; 6] {
        let mut data = [0u8; 6];
        data[0] = (self.port >> 8) as u8;
        data[1] = (self.port & 0xff) as u8;
        data[2] = (self.topic_id >> 8) as u8;
        data[3] = (self.topic_id & 0xff) as u8;
        data[4] = (self.group_id >> 8) as u8;
        data[5] = (self.group_id & 0xff) as u8;
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 6 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "C_REG payload too short",
            ));
        }
        Ok(ConsumerRegister {
            port: u16::from_be_bytes([payload[0], payload[1]]),
            topic_id: u16::from_be_bytes([payload[2], payload[3]]),
            group_id: u16::from_be_bytes([payload[4], payload[5]]),
        })
    }
}

pub fn parse_frame(body: &[u8]) -> io::Result<Message> {
    if body.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty frame"));
    }
    let msg_type = body[0];
    let payload = &body[1..];
    match msg_type {
        ECHO => Ok(Message::Echo(String::from_utf8_lossy(payload).into_owned())),
        P_REG => Ok(Message::ProducerRegister(ProducerRegister::decode(
            payload,
        )?)),
        C_REG => Ok(Message::ConsumerRegister(ConsumerRegister::decode(
            payload,
        )?)),
        PCM => Ok(Message::Pcm(payload.to_vec())),
        R_ECHO => Ok(Message::REcho(
            String::from_utf8_lossy(payload).into_owned(),
        )),
        R_P_REG => {
            let byte = payload.first().copied().unwrap_or(0);
            Ok(Message::RProducerRegister(byte))
        }
        R_C_REG => {
            let byte = payload.first().copied().unwrap_or(0);
            Ok(Message::RConsumerRegister(byte))
        }
        R_PCM => {
            let byte = payload.first().copied().unwrap_or(0);
            Ok(Message::RPcm(byte))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown message type {other}"),
        )),
    }
}

pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Message> {
    let length = reader.read_u8().await?;
    let mut body = vec![0u8; length as usize];
    reader.read_exact(&mut body).await?;
    parse_frame(&body)
}

fn frame_bytes(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(2 + payload.len());
    frame.push((payload.len() + 1) as u8);
    frame.push(msg_type);
    frame.extend_from_slice(payload);
    frame
}

pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Message,
) -> io::Result<()> {
    let frame = match message {
        Message::Echo(s) => frame_bytes(ECHO, s.as_bytes()),
        Message::ProducerRegister(reg) => frame_bytes(P_REG, &reg.encode()),
        Message::ConsumerRegister(reg) => frame_bytes(C_REG, &reg.encode()),
        Message::Pcm(bytes) => frame_bytes(PCM, bytes),
        Message::REcho(s) => frame_bytes(R_ECHO, s.as_bytes()),
        Message::RProducerRegister(b) => frame_bytes(R_P_REG, &[*b]),
        Message::RConsumerRegister(b) => frame_bytes(R_C_REG, &[*b]),
        Message::RPcm(b) => frame_bytes(R_PCM, &[*b]),
    };
    writer.write_all(&frame).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_register_roundtrip() {
        let reg = ProducerRegister {
            port: 7778,
            topic_id: 1,
        };
        let decoded = ProducerRegister::decode(&reg.encode()).unwrap();
        assert_eq!(reg, decoded);
    }

    #[test]
    fn consumer_register_roundtrip() {
        let reg = ConsumerRegister {
            port: 7779,
            topic_id: 1,
            group_id: 2,
        };
        let decoded = ConsumerRegister::decode(&reg.encode()).unwrap();
        assert_eq!(reg, decoded);
    }

    #[test]
    fn parse_echo_and_pcm() {
        let echo_body = [ECHO, b'h', b'i'];
        assert_eq!(
            parse_frame(&echo_body).unwrap(),
            Message::Echo("hi".to_string())
        );

        let pcm_body = [PCM, b'a', b'b', b'c'];
        assert_eq!(
            parse_frame(&pcm_body).unwrap(),
            Message::Pcm(b"abc".to_vec())
        );
    }
}
