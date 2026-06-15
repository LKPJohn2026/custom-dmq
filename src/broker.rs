//! Broker state and consumer delivery.
//!
//! Producers and consumers register over binary frames; the broker dials back
//! on the client port. Consumers signal readiness with R_PCM; the broker then
//! reads at the group offset and pushes the next PCM frame.

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

    pub fn register_consumer(&mut self, reg: &ConsumerRegister) -> u8 {
        let topic = self.topic_mut(reg.topic_id);
        topic.find_or_create_group(reg.group_id);
        0
    }

    pub fn produce_pcm(&mut self, topic_id: u16, payload: &[u8]) -> (u8, u64) {
        let offset = self.topic_mut(topic_id).append(payload);
        (0, offset)
    }

    pub fn topic_ids_with_groups(&self) -> Vec<(u16, Vec<u16>)> {
        self.topics
            .iter()
            .map(|(tid, topic)| (*tid, topic.cgroups.iter().map(|g| g.group_id).collect()))
            .collect()
    }

    /// Synchronous read at the group offset — used by unit tests only.
    pub fn consume_at_offset(
        &mut self,
        topic_id: u16,
        group_id: u16,
        payload_out: &mut Option<Vec<u8>>,
    ) -> bool {
        let topic = match self.topics.get_mut(&topic_id) {
            Some(t) => t,
            None => return false,
        };
        topic.find_or_create_group(group_id);
        let offset = topic.group(group_id).unwrap().offset;
        let payload = topic.read_at(offset).map(|bytes| bytes.to_vec());
        match payload {
            None => {
                *payload_out = None;
                false
            }
            Some(bytes) => {
                *payload_out = Some(bytes);
                topic.group_mut(group_id).unwrap().offset += 1;
                true
            }
        }
    }
}

/// Per-consumer task: wait for R_PCM ready, deliver next group message, advance offset.
pub async fn run_consumer_ready_and_send<R, W>(
    broker: Arc<Mutex<Broker>>,
    topic_id: u16,
    group_id: u16,
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
                let guard = broker.lock().await;
                guard.topics.get(&topic_id).and_then(|t| {
                    let offset = t.group(group_id)?.offset;
                    t.read_at(offset).map(|p| p.to_vec())
                })
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

        let mut guard = broker.lock().await;
        if let Some(group) = guard
            .topics
            .get_mut(&topic_id)
            .and_then(|t| t.group_mut(group_id))
        {
            group.offset += 1;
            println!(
                "[broker] Delivered offset {} to group {group_id} on topic {topic_id}",
                group.offset - 1
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::ProducerRegister;

    fn setup() -> Broker {
        let mut broker = Broker::new();
        broker.register_producer(&ProducerRegister {
            port: 7778,
            topic_id: 1,
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
    fn test_produce_appends_and_returns_offset() {
        let mut broker = setup();
        let (_, o1) = broker.produce_pcm(1, b"msg-a");
        let (_, o2) = broker.produce_pcm(1, b"msg-b");
        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
    }

    #[test]
    fn test_consume_returns_messages_in_order() {
        let mut broker = setup();
        broker.produce_pcm(1, b"first");
        broker.produce_pcm(1, b"second");

        let mut payload = None;
        assert!(broker.consume_at_offset(1, 10, &mut payload));
        assert_eq!(payload.as_deref(), Some(b"first" as &[u8]));

        payload = None;
        assert!(broker.consume_at_offset(1, 10, &mut payload));
        assert_eq!(payload.as_deref(), Some(b"second" as &[u8]));
    }

    #[test]
    fn test_two_groups_independent() {
        let mut broker = setup();
        broker.produce_pcm(1, b"msg-a");
        broker.produce_pcm(1, b"msg-b");

        let mut a = None;
        let mut b = None;
        assert!(broker.consume_at_offset(1, 1, &mut a));
        assert!(broker.consume_at_offset(1, 2, &mut b));
        assert_eq!(a.as_deref(), Some(b"msg-a" as &[u8]));
        assert_eq!(b.as_deref(), Some(b"msg-a" as &[u8]));

        a = None;
        assert!(broker.consume_at_offset(1, 1, &mut a));
        assert_eq!(a.as_deref(), Some(b"msg-b" as &[u8]));
    }

    #[test]
    fn test_messages_persist_after_consume() {
        let mut broker = setup();
        broker.produce_pcm(1, b"persistent");

        let mut a = None;
        broker.consume_at_offset(1, 1, &mut a);

        let mut b = None;
        assert!(broker.consume_at_offset(1, 2, &mut b));
        assert_eq!(b.as_deref(), Some(b"persistent" as &[u8]));
    }
}
