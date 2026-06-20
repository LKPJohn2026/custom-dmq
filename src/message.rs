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
pub const FETCH: u8 = 5;
pub const COMMIT: u8 = 6;
pub const PRODUCE: u8 = 7;
pub const CREATE_TOPIC: u8 = 8;
pub const DESCRIBE_TOPIC: u8 = 9;
pub const LIST_TOPICS: u8 = 10;
pub const GET_LAG: u8 = 11;
pub const REPLICATE: u8 = 12;
pub const GET_CLUSTER: u8 = 13;

pub const R_ECHO: u8 = 101;
pub const R_P_REG: u8 = 102;
pub const R_C_REG: u8 = 103;
pub const R_PCM: u8 = 104;
pub const R_FETCH: u8 = 105;
pub const R_COMMIT: u8 = 106;
pub const R_PRODUCE: u8 = 107;
pub const R_CREATE_TOPIC: u8 = 108;
pub const R_DESCRIBE_TOPIC: u8 = 109;
pub const R_LIST_TOPICS: u8 = 110;
pub const R_GET_LAG: u8 = 111;
pub const R_REPLICATE: u8 = 112;
pub const R_GET_CLUSTER: u8 = 113;
pub const R_NOT_LEADER: u8 = 114;

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
pub struct FetchRequest {
    pub topic_id: u16,
    pub partition_id: u16,
    pub offset: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetRequest {
    pub group_id: u16,
    pub topic_id: u16,
    pub partition_id: u16,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    pub topic_id: u16,
    pub partition_id: u16,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicRequest {
    pub topic_id: u16,
    pub partition_count: u16,
    pub max_records: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicRequest {
    pub topic_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLagRequest {
    pub group_id: u16,
    pub topic_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateRequest {
    pub topic_id: u16,
    pub partition_id: u16,
    pub offset: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Echo(String),
    ProducerRegister(ProducerRegister),
    ConsumerRegister(ConsumerRegister),
    Pcm(Vec<u8>),
    Fetch(FetchRequest),
    CommitOffset(CommitOffsetRequest),
    Produce(ProduceRequest),
    CreateTopic(CreateTopicRequest),
    DescribeTopic(DescribeTopicRequest),
    ListTopics,
    GetLag(GetLagRequest),
    Replicate(ReplicateRequest),
    GetCluster,
    REcho(String),
    RProducerRegister(u8),
    RConsumerRegister(u8),
    RPcm(u8),
    RFetch(Vec<u8>),
    RCommitOffset(u8),
    RProduce(u64),
    RCreateTopic(u8),
    RDescribeTopic(Vec<u8>),
    RListTopics(Vec<u8>),
    RGetLag(Vec<u8>),
    RReplicate(u8),
    RGetCluster(Vec<u8>),
    RNotLeader(u16),
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

impl FetchRequest {
    pub fn encode(&self) -> [u8; 16] {
        let mut data = [0u8; 16];
        data[0..2].copy_from_slice(&self.topic_id.to_be_bytes());
        data[2..4].copy_from_slice(&self.partition_id.to_be_bytes());
        data[4..12].copy_from_slice(&self.offset.to_be_bytes());
        data[12..16].copy_from_slice(&self.max_bytes.to_be_bytes());
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FETCH payload too short",
            ));
        }
        Ok(FetchRequest {
            topic_id: u16::from_be_bytes([payload[0], payload[1]]),
            partition_id: u16::from_be_bytes([payload[2], payload[3]]),
            offset: u64::from_be_bytes([
                payload[4],
                payload[5],
                payload[6],
                payload[7],
                payload[8],
                payload[9],
                payload[10],
                payload[11],
            ]),
            max_bytes: u32::from_be_bytes([payload[12], payload[13], payload[14], payload[15]]),
        })
    }
}

impl CommitOffsetRequest {
    pub fn encode(&self) -> [u8; 14] {
        let mut data = [0u8; 14];
        data[0..2].copy_from_slice(&self.group_id.to_be_bytes());
        data[2..4].copy_from_slice(&self.topic_id.to_be_bytes());
        data[4..6].copy_from_slice(&self.partition_id.to_be_bytes());
        data[6..14].copy_from_slice(&self.offset.to_be_bytes());
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 14 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "COMMIT payload too short",
            ));
        }
        Ok(CommitOffsetRequest {
            group_id: u16::from_be_bytes([payload[0], payload[1]]),
            topic_id: u16::from_be_bytes([payload[2], payload[3]]),
            partition_id: u16::from_be_bytes([payload[4], payload[5]]),
            offset: u64::from_be_bytes([
                payload[6],
                payload[7],
                payload[8],
                payload[9],
                payload[10],
                payload[11],
                payload[12],
                payload[13],
            ]),
        })
    }
}

impl ProduceRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.payload.len());
        out.extend_from_slice(&self.topic_id.to_be_bytes());
        out.extend_from_slice(&self.partition_id.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "PRODUCE payload too short",
            ));
        }
        Ok(ProduceRequest {
            topic_id: u16::from_be_bytes([payload[0], payload[1]]),
            partition_id: u16::from_be_bytes([payload[2], payload[3]]),
            payload: payload[4..].to_vec(),
        })
    }
}

impl CreateTopicRequest {
    pub fn encode(&self) -> [u8; 8] {
        let mut data = [0u8; 8];
        data[0..2].copy_from_slice(&self.topic_id.to_be_bytes());
        data[2..4].copy_from_slice(&self.partition_count.to_be_bytes());
        data[4..8].copy_from_slice(&self.max_records.to_be_bytes());
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CREATE_TOPIC payload too short",
            ));
        }
        Ok(CreateTopicRequest {
            topic_id: u16::from_be_bytes([payload[0], payload[1]]),
            partition_count: u16::from_be_bytes([payload[2], payload[3]]),
            max_records: u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]),
        })
    }
}

impl DescribeTopicRequest {
    pub fn encode(&self) -> [u8; 2] {
        self.topic_id.to_be_bytes()
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DESCRIBE_TOPIC payload too short",
            ));
        }
        Ok(DescribeTopicRequest {
            topic_id: u16::from_be_bytes([payload[0], payload[1]]),
        })
    }
}

impl GetLagRequest {
    pub fn encode(&self) -> [u8; 4] {
        let mut data = [0u8; 4];
        data[0..2].copy_from_slice(&self.group_id.to_be_bytes());
        data[2..4].copy_from_slice(&self.topic_id.to_be_bytes());
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "GET_LAG payload too short",
            ));
        }
        Ok(GetLagRequest {
            group_id: u16::from_be_bytes([payload[0], payload[1]]),
            topic_id: u16::from_be_bytes([payload[2], payload[3]]),
        })
    }
}

impl ReplicateRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(14 + self.payload.len());
        out.extend_from_slice(&self.topic_id.to_be_bytes());
        out.extend_from_slice(&self.partition_id.to_be_bytes());
        out.extend_from_slice(&self.offset.to_be_bytes());
        let len = u16::try_from(self.payload.len()).expect("payload fits u16");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 14 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "REPLICATE payload too short",
            ));
        }
        let len = u16::from_be_bytes([payload[12], payload[13]]) as usize;
        if payload.len() < 14 + len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "REPLICATE payload truncated",
            ));
        }
        Ok(ReplicateRequest {
            topic_id: u16::from_be_bytes([payload[0], payload[1]]),
            partition_id: u16::from_be_bytes([payload[2], payload[3]]),
            offset: u64::from_be_bytes([
                payload[4], payload[5], payload[6], payload[7], payload[8], payload[9],
                payload[10], payload[11],
            ]),
            payload: payload[14..14 + len].to_vec(),
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
        FETCH => Ok(Message::Fetch(FetchRequest::decode(payload)?)),
        COMMIT => Ok(Message::CommitOffset(CommitOffsetRequest::decode(payload)?)),
        PRODUCE => Ok(Message::Produce(ProduceRequest::decode(payload)?)),
        CREATE_TOPIC => Ok(Message::CreateTopic(CreateTopicRequest::decode(payload)?)),
        DESCRIBE_TOPIC => Ok(Message::DescribeTopic(DescribeTopicRequest::decode(
            payload,
        )?)),
        LIST_TOPICS => Ok(Message::ListTopics),
        GET_LAG => Ok(Message::GetLag(GetLagRequest::decode(payload)?)),
        REPLICATE => Ok(Message::Replicate(ReplicateRequest::decode(payload)?)),
        GET_CLUSTER => Ok(Message::GetCluster),
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
        R_FETCH => Ok(Message::RFetch(payload.to_vec())),
        R_COMMIT => {
            let byte = payload.first().copied().unwrap_or(0);
            Ok(Message::RCommitOffset(byte))
        }
        R_PRODUCE => {
            if payload.len() < 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "R_PRODUCE payload too short",
                ));
            }
            Ok(Message::RProduce(u64::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ])))
        }
        R_CREATE_TOPIC => {
            let byte = payload.first().copied().unwrap_or(0);
            Ok(Message::RCreateTopic(byte))
        }
        R_DESCRIBE_TOPIC => Ok(Message::RDescribeTopic(payload.to_vec())),
        R_LIST_TOPICS => Ok(Message::RListTopics(payload.to_vec())),
        R_GET_LAG => Ok(Message::RGetLag(payload.to_vec())),
        R_REPLICATE => {
            let byte = payload.first().copied().unwrap_or(0);
            Ok(Message::RReplicate(byte))
        }
        R_GET_CLUSTER => Ok(Message::RGetCluster(payload.to_vec())),
        R_NOT_LEADER => {
            if payload.len() < 2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "R_NOT_LEADER payload too short",
                ));
            }
            Ok(Message::RNotLeader(u16::from_be_bytes([payload[0], payload[1]])))
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
        Message::Fetch(req) => frame_bytes(FETCH, &req.encode()),
        Message::CommitOffset(req) => frame_bytes(COMMIT, &req.encode()),
        Message::Produce(req) => frame_bytes(PRODUCE, &req.encode()),
        Message::CreateTopic(req) => frame_bytes(CREATE_TOPIC, &req.encode()),
        Message::DescribeTopic(req) => frame_bytes(DESCRIBE_TOPIC, &req.encode()),
        Message::ListTopics => frame_bytes(LIST_TOPICS, &[]),
        Message::GetLag(req) => frame_bytes(GET_LAG, &req.encode()),
        Message::Replicate(req) => frame_bytes(REPLICATE, &req.encode()),
        Message::GetCluster => frame_bytes(GET_CLUSTER, &[]),
        Message::REcho(s) => frame_bytes(R_ECHO, s.as_bytes()),
        Message::RProducerRegister(b) => frame_bytes(R_P_REG, &[*b]),
        Message::RConsumerRegister(b) => frame_bytes(R_C_REG, &[*b]),
        Message::RPcm(b) => frame_bytes(R_PCM, &[*b]),
        Message::RFetch(bytes) => frame_bytes(R_FETCH, bytes),
        Message::RCommitOffset(b) => frame_bytes(R_COMMIT, &[*b]),
        Message::RProduce(offset) => frame_bytes(R_PRODUCE, &offset.to_be_bytes()),
        Message::RCreateTopic(b) => frame_bytes(R_CREATE_TOPIC, &[*b]),
        Message::RDescribeTopic(bytes) => frame_bytes(R_DESCRIBE_TOPIC, bytes),
        Message::RListTopics(bytes) => frame_bytes(R_LIST_TOPICS, bytes),
        Message::RGetLag(bytes) => frame_bytes(R_GET_LAG, bytes),
        Message::RReplicate(b) => frame_bytes(R_REPLICATE, &[*b]),
        Message::RGetCluster(bytes) => frame_bytes(R_GET_CLUSTER, bytes),
        Message::RNotLeader(id) => frame_bytes(R_NOT_LEADER, &id.to_be_bytes()),
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

    #[test]
    fn fetch_request_roundtrip() {
        let req = FetchRequest {
            topic_id: 7,
            partition_id: 2,
            offset: 42,
            max_bytes: 4096,
        };
        let decoded = FetchRequest::decode(&req.encode()).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn commit_offset_roundtrip() {
        let req = CommitOffsetRequest {
            group_id: 1,
            topic_id: 7,
            partition_id: 2,
            offset: 42,
        };
        let decoded = CommitOffsetRequest::decode(&req.encode()).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn produce_request_roundtrip() {
        let req = ProduceRequest {
            topic_id: 1,
            partition_id: 0,
            payload: b"hello".to_vec(),
        };
        let decoded = ProduceRequest::decode(&req.encode()).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn create_topic_roundtrip() {
        let req = CreateTopicRequest {
            topic_id: 5,
            partition_count: 3,
            max_records: 100,
        };
        let decoded = CreateTopicRequest::decode(&req.encode()).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn replicate_request_roundtrip() {
        let req = ReplicateRequest {
            topic_id: 1,
            partition_id: 0,
            offset: 42,
            payload: b"data".to_vec(),
        };
        let decoded = ReplicateRequest::decode(&req.encode()).unwrap();
        assert_eq!(req, decoded);
    }
}
