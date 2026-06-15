//! Broker state, partition routing, and consumer delivery.
//!
//! Producers and consumers register over binary frames; the broker dials back
//! on the client port. Messages land in a topic staging queue until a consumer
//! group exists, then route into per-group partitions. Consumers signal
//! readiness with R_PCM and receive the next message from their assigned partition.

use crate::message::{self, ConsumerRegister, Message, ProducerRegister};
use crate::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub const BROKER_PORT: u16 = 7777;

pub fn broker_addr() -> String {
    let port = std::env::var("DMQ_BROKER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(BROKER_PORT as u32) as u16;
    format!("127.0.0.1:{port}")
}

pub fn broker_port() -> u16 {
    std::env::var("DMQ_BROKER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(BROKER_PORT)
}

pub struct Broker {
    topics: HashMap<u16, Topic>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            topics: HashMap::new(),
        }
    }

    fn topic_mut(&mut self, topic_id: u16) -> &mut Topic {
        self.topics
            .entry(topic_id)
            .or_insert_with(|| Topic::new(topic_id))
    }

    pub fn process_echo(&self, text: &str) -> String {
        format!("I have receiver: {text}")
    }

    pub fn register_producer(&mut self, reg: &ProducerRegister) -> u8 {
        self.topic_mut(reg.topic_id);
        0
    }

    pub fn register_consumer(&mut self, reg: &ConsumerRegister) -> u16 {
        let topic = self.topic_mut(reg.topic_id);
        topic.find_or_create_group(reg.group_id);
        topic
            .group_mut(reg.group_id)
            .expect("group just created")
            .assign_partition()
    }

    pub fn produce_pcm(&mut self, topic_id: u16, payload: &[u8]) -> (u8, u64) {
        let topic = self.topic_mut(topic_id);

        if topic.cgroups.is_empty() {
            let offset = topic.append_to_staging(payload);
            return (0, offset);
        }

        let payload = payload.to_vec();
        let group_count = topic.cgroups.len();
        for i in 0..group_count {
            let partition_idx = topic.cgroups[i].smallest_partition_index();
            while let Some(msg) = topic.staging.pop_front() {
                topic.cgroups[i].partitions[partition_idx].append(&msg);
            }
            topic.cgroups[i].partitions[partition_idx].append(&payload);
        }

        (0, 0)
    }

    pub fn topic_ids_with_groups(&self) -> Vec<(u16, Vec<u16>)> {
        self.topics
            .iter()
            .map(|(tid, topic)| (*tid, topic.cgroups.iter().map(|g| g.group_id).collect()))
            .collect()
    }

    /// Pop the next message from a group partition — used by unit tests.
    pub fn consume_from_partition(
        &mut self,
        topic_id: u16,
        group_id: u16,
        partition_idx: u16,
        payload_out: &mut Option<Vec<u8>>,
    ) -> bool {
        let topic = match self.topics.get_mut(&topic_id) {
            Some(t) => t,
            None => return false,
        };
        topic.find_or_create_group(group_id);
        let partition = match topic
            .group_mut(group_id)
            .and_then(|g| g.partitions.get_mut(partition_idx as usize))
        {
            Some(p) => p,
            None => return false,
        };
        match partition.pop_front() {
            None => {
                *payload_out = None;
                false
            }
            Some(bytes) => {
                *payload_out = Some(bytes);
                true
            }
        }
    }
}

/// Per-consumer task: wait for R_PCM, pop from assigned partition, send PCM.
pub async fn run_consumer_ready_and_send<R, W>(
    broker: Arc<Mutex<Broker>>,
    topic_id: u16,
    group_id: u16,
    partition_idx: u16,
    reader: &mut BufReader<R>,
    writer: &mut W,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        match message::read_message(reader).await {
            Ok(Message::RPcm(_)) => {}
            Ok(other) => {
                eprintln!("[broker→consumer] Expected R_PCM, got {other:?}");
                break;
            }
            Err(e) => {
                eprintln!("[broker→consumer] Read error: {e}");
                break;
            }
        }

        let payload = loop {
            let data = {
                let mut guard = broker.lock().await;
                guard
                    .topics
                    .get_mut(&topic_id)
                    .and_then(|t| t.group_mut(group_id))
                    .and_then(|g| g.partitions.get_mut(partition_idx as usize))
                    .and_then(|p| p.pop_front())
            };
            if let Some(p) = data {
                break p;
            }
            sleep(Duration::from_millis(10)).await;
        };

        if message::write_message(writer, &Message::Pcm(payload))
            .await
            .is_err()
        {
            break;
        }

        println!(
            "[broker] Delivered message from topic {topic_id} group {group_id} partition {partition_idx}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{ConsumerRegister, ProducerRegister};

    fn setup() -> Broker {
        let mut broker = Broker::new();
        broker.register_producer(&ProducerRegister {
            port: 7778,
            topic_id: 1,
        });
        broker
    }

    fn setup_with_group(group_id: u16) -> Broker {
        let mut broker = setup();
        broker.register_consumer(&ConsumerRegister {
            port: 7779,
            topic_id: 1,
            group_id,
        });
        broker
    }

    #[test]
    fn test_register_producer_auto_creates_topic() {
        let mut broker = Broker::new();
        let code = broker.register_producer(&ProducerRegister {
            port: 7778,
            topic_id: 1,
        });
        assert_eq!(code, 0);
        assert!(broker.topics.contains_key(&1));
    }

    #[test]
    fn test_produce_to_staging_without_groups() {
        let mut broker = setup();
        let (_, o1) = broker.produce_pcm(1, b"msg-a");
        let (_, o2) = broker.produce_pcm(1, b"msg-b");
        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
    }

    #[test]
    fn test_consume_returns_messages_in_order() {
        let mut broker = setup_with_group(10);
        broker.produce_pcm(1, b"first");
        broker.produce_pcm(1, b"second");

        let mut payload = None;
        assert!(broker.consume_from_partition(1, 10, 0, &mut payload));
        assert_eq!(payload.as_deref(), Some(b"first" as &[u8]));

        payload = None;
        assert!(broker.consume_from_partition(1, 10, 0, &mut payload));
        assert_eq!(payload.as_deref(), Some(b"second" as &[u8]));
    }

    #[test]
    fn test_two_groups_independent() {
        let mut broker = setup_with_group(1);
        broker.register_consumer(&ConsumerRegister {
            port: 7780,
            topic_id: 1,
            group_id: 2,
        });
        broker.produce_pcm(1, b"msg-a");
        broker.produce_pcm(1, b"msg-b");

        let mut a = None;
        let mut b = None;
        assert!(broker.consume_from_partition(1, 1, 0, &mut a));
        assert!(broker.consume_from_partition(1, 2, 0, &mut b));
        assert_eq!(a.as_deref(), Some(b"msg-a" as &[u8]));
        assert_eq!(b.as_deref(), Some(b"msg-a" as &[u8]));

        a = None;
        assert!(broker.consume_from_partition(1, 1, 0, &mut a));
        assert_eq!(a.as_deref(), Some(b"msg-b" as &[u8]));
    }

    #[test]
    fn test_staging_drains_when_group_registers_after_produce() {
        let mut broker = setup();
        broker.produce_pcm(1, b"early");
        broker.register_consumer(&ConsumerRegister {
            port: 7779,
            topic_id: 1,
            group_id: 1,
        });
        broker.produce_pcm(1, b"late");

        let mut payload = None;
        assert!(broker.consume_from_partition(1, 1, 0, &mut payload));
        assert_eq!(payload.as_deref(), Some(b"early" as &[u8]));
        payload = None;
        assert!(broker.consume_from_partition(1, 1, 0, &mut payload));
        assert_eq!(payload.as_deref(), Some(b"late" as &[u8]));
    }
}
