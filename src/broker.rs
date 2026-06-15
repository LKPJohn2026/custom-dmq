//! Broker state and push-based consumer group delivery.
//!
//! Replaces the prior text-protocol broker and pull-style `CONSUME` handler.
//! Producers and consumers register over binary frames; the broker dials back
//! on the client port and receives PCM on that persistent connection.
//! Each consumer group tracks its own offset into the shared topic log; the
//! group delivery loop pushes the next message to a ready consumer and
//! advances the offset after an R_PCM ack.

use crate::cgroup::{ConsumerHandle, PushRequest};
use crate::message::{ConsumerRegister, ProducerRegister};
use crate::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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

    pub fn register_consumer(&mut self, reg: &ConsumerRegister) -> (u8, bool, u16) {
        let topic = self.topic_mut(reg.topic_id);
        let (is_new_group, _idx) = topic.find_or_create_group(reg.group_id);
        (0, is_new_group, reg.topic_id)
    }

    pub fn produce_pcm(&mut self, topic_id: u16, payload: &[u8]) -> (u8, u64) {
        let offset = self.topic_mut(topic_id).append(payload);
        (0, offset)
    }

    pub fn add_consumer_handle(&mut self, topic_id: u16, group_id: u16, handle: ConsumerHandle) {
        if let Some(group) = self
            .topics
            .get_mut(&topic_id)
            .and_then(|t| t.group_mut(group_id))
        {
            group.add_consumer(handle);
        }
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
        let (_, _) = topic.find_or_create_group(group_id);
        let offset = topic.group_mut(group_id).unwrap().offset;
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

/// Background task: read at the group offset and push PCM to a ready consumer.
pub async fn run_consumer_group_delivery(broker: Arc<Mutex<Broker>>, topic_id: u16, group_id: u16) {
    println!("[broker] Starting push delivery for topic={topic_id} group={group_id}");

    loop {
        let delivery = {
            let mut guard = broker.lock().await;
            let topic = match guard.topics.get_mut(&topic_id) {
                Some(t) => t,
                None => {
                    drop(guard);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
            };
            if topic.group_mut(group_id).is_none() {
                drop(guard);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            let offset = topic.group_mut(group_id).unwrap().offset;
            let payload = topic.read_at(offset).map(|p| p.to_vec());
            let push_tx = topic
                .group_mut(group_id)
                .unwrap()
                .find_ready_consumer()
                .map(|c| c.push_tx.clone());

            (payload, push_tx, offset)
        };

        let (payload, push_tx, offset) = match delivery {
            (Some(p), Some(tx), off) => (p, tx, off),
            _ => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }
        };

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if push_tx
            .send(PushRequest {
                payload,
                ack: ack_tx,
            })
            .await
            .is_err()
        {
            continue;
        }

        if ack_rx.await.is_ok() {
            let mut guard = broker.lock().await;
            if let Some(group) = guard
                .topics
                .get_mut(&topic_id)
                .and_then(|t| t.group_mut(group_id))
            {
                if group.offset == offset {
                    group.offset += 1;
                }
            }
            println!("[broker] Delivered offset {offset} to group {group_id} on topic {topic_id}");
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
