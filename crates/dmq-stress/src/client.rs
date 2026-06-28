//! TCP client helpers for produce, fetch, and admin.

use custom_dmq::cluster::ClusterConfig;
use custom_dmq::fetch_batch::decode_records;
use custom_dmq::message::{
    self, CommitOffsetRequest, CreateTopicRequest, FetchRequest, IdempotentProduceRequest, Message,
};
use custom_dmq::partition_log::Record;
use std::io;
use std::time::{Duration, Instant};
use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

pub fn broker_addr(topic_id: u16, partition_id: u16) -> String {
    ClusterConfig::resolve_leader_addr(topic_id, partition_id)
}

pub fn encode_payload(run_id: &str, seq: u64, payload_bytes: usize) -> Vec<u8> {
    let header = format!("{run_id}:{seq:012}:");
    let mut out = vec![0u8; payload_bytes];
    let copy_len = header.len().min(payload_bytes);
    out[..copy_len].copy_from_slice(&header.as_bytes()[..copy_len]);
    for byte in out.iter_mut().skip(copy_len) {
        *byte = b'x';
    }
    out
}

pub fn parse_payload(payload: &[u8]) -> io::Result<(String, u64)> {
    let text =
        std::str::from_utf8(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let (run_part, rest) = text
        .split_once(':')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing run_id"))?;
    let (seq_part, _) = rest
        .split_once(':')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing seq"))?;
    let seq = seq_part
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid seq"))?;
    Ok((run_part.to_string(), seq))
}

pub async fn create_topic(topic_id: u16, partition_count: u16, max_records: u32) -> io::Result<()> {
    let addr = broker_addr(topic_id, 0);
    let mut stream = TcpStream::connect(&addr).await?;
    let req = CreateTopicRequest {
        topic_id,
        partition_count,
        max_records,
    };
    message::write_message(&mut stream, &Message::CreateTopic(req)).await?;
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await?;
    match resp {
        Message::RCreateTopic(0) | Message::RCreateTopic(1) => Ok(()),
        Message::RCreateTopic(code) => Err(io::Error::other(format!(
            "create topic failed with code {code}"
        ))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected create topic response: {other:?}"),
        )),
    }
}

pub struct ProduceSession {
    writer: OwnedWriteHalf,
    reader: BufReader<OwnedReadHalf>,
    topic_id: u16,
    partition_id: u16,
    producer_id: u64,
}

impl ProduceSession {
    pub async fn connect(topic_id: u16, partition_id: u16, producer_id: u64) -> io::Result<Self> {
        let addr = broker_addr(topic_id, partition_id);
        let stream = TcpStream::connect(&addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            writer,
            reader: BufReader::new(reader),
            topic_id,
            partition_id,
            producer_id,
        })
    }

    pub async fn produce(
        &mut self,
        sequence: u64,
        payload: Vec<u8>,
    ) -> io::Result<(u64, Duration)> {
        let started = Instant::now();
        let req = IdempotentProduceRequest {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            producer_id: self.producer_id,
            sequence,
            payload,
        };
        message::write_message(&mut self.writer, &Message::IdempotentProduce(req)).await?;
        let resp = message::read_message(&mut self.reader).await?;
        let latency = started.elapsed();
        match resp {
            Message::RProduce(offset) => Ok((offset, latency)),
            Message::RNotLeader(leader) => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("not leader; retry broker {leader}"),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected produce response: {other:?}"),
            )),
        }
    }
}

pub struct FetchSession {
    writer: OwnedWriteHalf,
    reader: BufReader<OwnedReadHalf>,
    topic_id: u16,
    partition_id: u16,
}

impl FetchSession {
    pub async fn connect(topic_id: u16, partition_id: u16) -> io::Result<Self> {
        let addr = broker_addr(topic_id, partition_id);
        let stream = TcpStream::connect(&addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            writer,
            reader: BufReader::new(reader),
            topic_id,
            partition_id,
        })
    }

    async fn fetch_batch(&mut self, offset: u64, max_bytes: u32) -> io::Result<Vec<Record>> {
        // v1 wire frames are length-prefixed with u8; paginate with a small
        // max_bytes so each RFetch response stays under the 255-byte limit.
        let safe_max = max_bytes.min(64);
        let req = FetchRequest {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            offset,
            max_bytes: safe_max,
            max_wait_ms: 100,
        };
        message::write_message(&mut self.writer, &Message::Fetch(req)).await?;
        let resp = message::read_message(&mut self.reader).await?;
        let Message::RFetch(bytes) = resp else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected fetch response: {resp:?}"),
            ));
        };
        let batch = match bytes.first() {
            Some(b) if *b == custom_dmq::compression::CODEC_LZ4 => {
                custom_dmq::compression::unwrap_batch(&bytes)?
            }
            _ => bytes,
        };
        decode_records(&batch)
    }

    pub async fn fetch_all(&mut self, max_bytes: u32) -> io::Result<Vec<Record>> {
        let mut offset = 0u64;
        let mut all = Vec::new();
        loop {
            let records = self.fetch_batch(offset, max_bytes).await?;
            if records.is_empty() {
                break;
            }
            for rec in records {
                offset = rec.offset + 1;
                all.push(rec);
            }
        }
        Ok(all)
    }

    pub async fn fetch_and_commit(
        &mut self,
        group_id: u16,
        max_bytes: u32,
    ) -> io::Result<(Vec<Record>, u64)> {
        let all = self.fetch_all(max_bytes).await?;
        let offset = all.last().map(|rec| rec.offset + 1).unwrap_or(0);
        if offset > 0 {
            let commit = CommitOffsetRequest {
                group_id,
                topic_id: self.topic_id,
                partition_id: self.partition_id,
                offset,
            };
            message::write_message(&mut self.writer, &Message::CommitOffset(commit)).await?;
            let _ = message::read_message(&mut self.reader).await?;
        }
        Ok((all, offset))
    }
}

pub async fn fetch_all(
    topic_id: u16,
    partition_id: u16,
    max_bytes: u32,
) -> io::Result<Vec<Record>> {
    let mut session = FetchSession::connect(topic_id, partition_id).await?;
    session.fetch_all(max_bytes).await
}

pub async fn fetch_and_commit(
    topic_id: u16,
    partition_id: u16,
    group_id: u16,
    max_bytes: u32,
) -> io::Result<(Vec<Record>, u64)> {
    let mut session = FetchSession::connect(topic_id, partition_id).await?;
    session.fetch_and_commit(group_id, max_bytes).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_roundtrip() {
        let payload = encode_payload("run42", 7, 64);
        let (run_id, seq) = parse_payload(&payload).unwrap();
        assert_eq!(run_id, "run42");
        assert_eq!(seq, 7);
    }
}
