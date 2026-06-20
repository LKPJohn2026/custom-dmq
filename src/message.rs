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
pub const IDEMPOTENT_PRODUCE: u8 = 14;
pub const BROKER_HEARTBEAT: u8 = 15;
pub const JOIN_GROUP: u8 = 16;
pub const GROUP_HEARTBEAT: u8 = 17;
pub const HANDSHAKE: u8 = 18;

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
pub const R_BROKER_HEARTBEAT: u8 = 115;
pub const R_JOIN_GROUP: u8 = 116;
pub const R_GROUP_HEARTBEAT: u8 = 117;
pub const R_HANDSHAKE: u8 = 118;
pub const R_ERROR: u8 = 119;

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
    pub max_wait_ms: u32,
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
pub struct IdempotentProduceRequest {
    pub topic_id: u16,
    pub partition_id: u16,
    pub producer_id: u64,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHeartbeatRequest {
    pub broker_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupRequest {
    pub group_id: u16,
    pub topic_id: u16,
    pub member_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupHeartbeatRequest {
    pub group_id: u16,
    pub member_id: u64,
    pub generation: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeRequest {
    pub protocol_version: u16,
    pub auth_token: Vec<u8>,
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
    IdempotentProduce(IdempotentProduceRequest),
    BrokerHeartbeat(BrokerHeartbeatRequest),
    JoinGroup(JoinGroupRequest),
    GroupHeartbeat(GroupHeartbeatRequest),
    Handshake(HandshakeRequest),
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
    RBrokerHeartbeat(u8),
    RJoinGroup(Vec<u8>),
    RGroupHeartbeat(u8, u8),
    RHandshake(u8, u16),
    RError(u8, String),
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

impl BrokerHeartbeatRequest {
    pub fn encode(&self) -> [u8; 2] {
        self.broker_id.to_be_bytes()
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BROKER_HEARTBEAT payload too short",
            ));
        }
        Ok(BrokerHeartbeatRequest {
            broker_id: u16::from_be_bytes([payload[0], payload[1]]),
        })
    }
}

impl JoinGroupRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12);
        out.extend_from_slice(&self.group_id.to_be_bytes());
        out.extend_from_slice(&self.topic_id.to_be_bytes());
        out.extend_from_slice(&self.member_id.to_be_bytes());
        out
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JOIN_GROUP payload too short",
            ));
        }
        Ok(JoinGroupRequest {
            group_id: u16::from_be_bytes([payload[0], payload[1]]),
            topic_id: u16::from_be_bytes([payload[2], payload[3]]),
            member_id: u64::from_be_bytes([
                payload[4],
                payload[5],
                payload[6],
                payload[7],
                payload[8],
                payload[9],
                payload[10],
                payload[11],
            ]),
        })
    }
}

impl GroupHeartbeatRequest {
    pub fn encode(&self) -> [u8; 14] {
        let mut data = [0u8; 14];
        data[0..2].copy_from_slice(&self.group_id.to_be_bytes());
        data[2..10].copy_from_slice(&self.member_id.to_be_bytes());
        data[10..14].copy_from_slice(&self.generation.to_be_bytes());
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 14 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "GROUP_HEARTBEAT payload too short",
            ));
        }
        Ok(GroupHeartbeatRequest {
            group_id: u16::from_be_bytes([payload[0], payload[1]]),
            member_id: u64::from_be_bytes([
                payload[2], payload[3], payload[4], payload[5], payload[6], payload[7], payload[8],
                payload[9],
            ]),
            generation: u32::from_be_bytes([payload[10], payload[11], payload[12], payload[13]]),
        })
    }
}

pub fn encode_join_group_response(
    code: u8,
    member_id: u64,
    generation: u32,
    partitions: &[u16],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(15 + partitions.len() * 2);
    out.push(code);
    out.extend_from_slice(&member_id.to_be_bytes());
    out.extend_from_slice(&generation.to_be_bytes());
    out.extend_from_slice(&(partitions.len() as u16).to_be_bytes());
    for p in partitions {
        out.extend_from_slice(&p.to_be_bytes());
    }
    out
}

impl HandshakeRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.auth_token.len());
        out.extend_from_slice(&self.protocol_version.to_be_bytes());
        out.extend_from_slice(&(self.auth_token.len() as u16).to_be_bytes());
        out.extend_from_slice(&self.auth_token);
        out
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HANDSHAKE payload too short",
            ));
        }
        let protocol_version = u16::from_be_bytes([payload[0], payload[1]]);
        let token_len = u16::from_be_bytes([payload[2], payload[3]]) as usize;
        if payload.len() < 4 + token_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HANDSHAKE token truncated",
            ));
        }
        Ok(HandshakeRequest {
            protocol_version,
            auth_token: payload[4..4 + token_len].to_vec(),
        })
    }
}

impl FetchRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut data = vec![0u8; 20];
        data[0..2].copy_from_slice(&self.topic_id.to_be_bytes());
        data[2..4].copy_from_slice(&self.partition_id.to_be_bytes());
        data[4..12].copy_from_slice(&self.offset.to_be_bytes());
        data[12..16].copy_from_slice(&self.max_bytes.to_be_bytes());
        data[16..20].copy_from_slice(&self.max_wait_ms.to_be_bytes());
        data
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FETCH payload too short",
            ));
        }
        let max_wait_ms = if payload.len() >= 20 {
            u32::from_be_bytes([payload[16], payload[17], payload[18], payload[19]])
        } else {
            0
        };
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
            max_wait_ms,
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
                payload[4],
                payload[5],
                payload[6],
                payload[7],
                payload[8],
                payload[9],
                payload[10],
                payload[11],
            ]),
            payload: payload[14..14 + len].to_vec(),
        })
    }
}

impl IdempotentProduceRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(20 + self.payload.len());
        out.extend_from_slice(&self.topic_id.to_be_bytes());
        out.extend_from_slice(&self.partition_id.to_be_bytes());
        out.extend_from_slice(&self.producer_id.to_be_bytes());
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IDEMPOTENT_PRODUCE payload too short",
            ));
        }
        Ok(IdempotentProduceRequest {
            topic_id: u16::from_be_bytes([payload[0], payload[1]]),
            partition_id: u16::from_be_bytes([payload[2], payload[3]]),
            producer_id: u64::from_be_bytes([
                payload[4],
                payload[5],
                payload[6],
                payload[7],
                payload[8],
                payload[9],
                payload[10],
                payload[11],
            ]),
            sequence: u64::from_be_bytes([
                payload[12],
                payload[13],
                payload[14],
                payload[15],
                payload[16],
                payload[17],
                payload[18],
                payload[19],
            ]),
            payload: payload[20..].to_vec(),
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
        IDEMPOTENT_PRODUCE => Ok(Message::IdempotentProduce(
            IdempotentProduceRequest::decode(payload)?,
        )),
        BROKER_HEARTBEAT => Ok(Message::BrokerHeartbeat(BrokerHeartbeatRequest::decode(
            payload,
        )?)),
        JOIN_GROUP => Ok(Message::JoinGroup(JoinGroupRequest::decode(payload)?)),
        GROUP_HEARTBEAT => Ok(Message::GroupHeartbeat(GroupHeartbeatRequest::decode(
            payload,
        )?)),
        HANDSHAKE => Ok(Message::Handshake(HandshakeRequest::decode(payload)?)),
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
            Ok(Message::RNotLeader(u16::from_be_bytes([
                payload[0], payload[1],
            ])))
        }
        R_BROKER_HEARTBEAT => {
            let byte = payload.first().copied().unwrap_or(0);
            Ok(Message::RBrokerHeartbeat(byte))
        }
        R_JOIN_GROUP => Ok(Message::RJoinGroup(payload.to_vec())),
        R_GROUP_HEARTBEAT => {
            let code = payload.first().copied().unwrap_or(0);
            let flag = payload.get(1).copied().unwrap_or(0);
            Ok(Message::RGroupHeartbeat(code, flag))
        }
        R_HANDSHAKE => {
            if payload.len() < 3 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "R_HANDSHAKE payload too short",
                ));
            }
            Ok(Message::RHandshake(
                payload[0],
                u16::from_be_bytes([payload[1], payload[2]]),
            ))
        }
        R_ERROR => {
            let code = payload.first().copied().unwrap_or(0);
            let msg = String::from_utf8_lossy(&payload[1..]).into_owned();
            Ok(Message::RError(code, msg))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown message type {other}"),
        )),
    }
}

pub fn encode_message(message: &Message) -> io::Result<Vec<u8>> {
    let payload = match message {
        Message::Echo(s) => s.as_bytes().to_vec(),
        Message::ProducerRegister(reg) => reg.encode().to_vec(),
        Message::ConsumerRegister(reg) => reg.encode().to_vec(),
        Message::Pcm(bytes) => bytes.clone(),
        Message::Fetch(req) => req.encode(),
        Message::CommitOffset(req) => req.encode().to_vec(),
        Message::Produce(req) => req.encode(),
        Message::CreateTopic(req) => req.encode().to_vec(),
        Message::DescribeTopic(req) => req.encode().to_vec(),
        Message::ListTopics => Vec::new(),
        Message::GetLag(req) => req.encode().to_vec(),
        Message::Replicate(req) => req.encode(),
        Message::GetCluster => Vec::new(),
        Message::IdempotentProduce(req) => req.encode(),
        Message::BrokerHeartbeat(req) => req.encode().to_vec(),
        Message::JoinGroup(req) => req.encode(),
        Message::GroupHeartbeat(req) => req.encode().to_vec(),
        Message::Handshake(req) => req.encode(),
        Message::REcho(s) => s.as_bytes().to_vec(),
        Message::RProducerRegister(b) => vec![*b],
        Message::RConsumerRegister(b) => vec![*b],
        Message::RPcm(b) => vec![*b],
        Message::RFetch(bytes) => bytes.clone(),
        Message::RCommitOffset(b) => vec![*b],
        Message::RProduce(offset) => offset.to_be_bytes().to_vec(),
        Message::RCreateTopic(b) => vec![*b],
        Message::RDescribeTopic(bytes) => bytes.clone(),
        Message::RListTopics(bytes) => bytes.clone(),
        Message::RGetLag(bytes) => bytes.clone(),
        Message::RReplicate(b) => vec![*b],
        Message::RGetCluster(bytes) => bytes.clone(),
        Message::RNotLeader(id) => id.to_be_bytes().to_vec(),
        Message::RBrokerHeartbeat(code) => vec![*code],
        Message::RJoinGroup(bytes) => bytes.clone(),
        Message::RGroupHeartbeat(code, flag) => vec![*code, *flag],
        Message::RHandshake(code, version) => {
            let mut out = vec![*code];
            out.extend_from_slice(&version.to_be_bytes());
            out
        }
        Message::RError(code, msg) => {
            let mut out = vec![*code];
            out.extend_from_slice(msg.as_bytes());
            out
        }
    };
    let msg_type = message_type_byte(message)?;
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(msg_type);
    out.extend_from_slice(&payload);
    Ok(out)
}

fn message_type_byte(message: &Message) -> io::Result<u8> {
    Ok(match message {
        Message::Echo(_) => ECHO,
        Message::ProducerRegister(_) => P_REG,
        Message::ConsumerRegister(_) => C_REG,
        Message::Pcm(_) => PCM,
        Message::Fetch(_) => FETCH,
        Message::CommitOffset(_) => COMMIT,
        Message::Produce(_) => PRODUCE,
        Message::CreateTopic(_) => CREATE_TOPIC,
        Message::DescribeTopic(_) => DESCRIBE_TOPIC,
        Message::ListTopics => LIST_TOPICS,
        Message::GetLag(_) => GET_LAG,
        Message::Replicate(_) => REPLICATE,
        Message::GetCluster => GET_CLUSTER,
        Message::IdempotentProduce(_) => IDEMPOTENT_PRODUCE,
        Message::BrokerHeartbeat(_) => BROKER_HEARTBEAT,
        Message::JoinGroup(_) => JOIN_GROUP,
        Message::GroupHeartbeat(_) => GROUP_HEARTBEAT,
        Message::Handshake(_) => HANDSHAKE,
        Message::REcho(_) => R_ECHO,
        Message::RProducerRegister(_) => R_P_REG,
        Message::RConsumerRegister(_) => R_C_REG,
        Message::RPcm(_) => R_PCM,
        Message::RFetch(_) => R_FETCH,
        Message::RCommitOffset(_) => R_COMMIT,
        Message::RProduce(_) => R_PRODUCE,
        Message::RCreateTopic(_) => R_CREATE_TOPIC,
        Message::RDescribeTopic(_) => R_DESCRIBE_TOPIC,
        Message::RListTopics(_) => R_LIST_TOPICS,
        Message::RGetLag(_) => R_GET_LAG,
        Message::RReplicate(_) => R_REPLICATE,
        Message::RGetCluster(_) => R_GET_CLUSTER,
        Message::RNotLeader(_) => R_NOT_LEADER,
        Message::RBrokerHeartbeat(_) => R_BROKER_HEARTBEAT,
        Message::RJoinGroup(_) => R_JOIN_GROUP,
        Message::RGroupHeartbeat(_, _) => R_GROUP_HEARTBEAT,
        Message::RHandshake(_, _) => R_HANDSHAKE,
        Message::RError(_, _) => R_ERROR,
    })
}

pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Message> {
    let length = reader.read_u8().await?;
    let mut body = vec![0u8; length as usize];
    reader.read_exact(&mut body).await?;
    parse_frame(&body)
}

pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Message,
) -> io::Result<()> {
    let frame = encode_message(message)?;
    let mut wire = Vec::with_capacity(1 + frame.len());
    wire.push(frame.len() as u8);
    wire.extend_from_slice(&frame);
    writer.write_all(&wire).await?;
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
            max_wait_ms: 0,
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

    #[test]
    fn idempotent_produce_roundtrip() {
        let req = IdempotentProduceRequest {
            topic_id: 1,
            partition_id: 0,
            producer_id: 99,
            sequence: 3,
            payload: b"x".to_vec(),
        };
        let decoded = IdempotentProduceRequest::decode(&req.encode()).unwrap();
        assert_eq!(req, decoded);
    }
}
