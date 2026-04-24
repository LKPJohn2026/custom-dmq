// ============================================================
// broker.rs — Broker
//
// Key changes from previous version:
//   - create_topic() is now PRIVATE — called internally only
//   - register_producer() auto-creates topic if missing (lazy init)
//     mirrors professor's processProducerRegisterMessage logic exactly
//   - produce() now takes &[u8] (raw bytes) instead of String,
//     matching the ring buffer's byte-oriented API
// ============================================================

use crate::consumer::ConsumerGroup;
use crate::topic::Topic;
use std::collections::HashMap;

pub struct Broker {
    topics: HashMap<String, Topic>,
    groups: HashMap<String, ConsumerGroup>,
    producers: HashMap<String, String>, // producer_id -> topic_name
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            topics: HashMap::new(),
            groups: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    // ----------------------------------------------------------
    // Private — called internally by register_producer if needed.
    // Mirrors professor's lazy topic creation inside
    // processProducerRegisterMessage.
    // ----------------------------------------------------------
    fn create_topic_if_missing(&mut self, topic_name: &str) {
        if !self.topics.contains_key(topic_name) {
            self.topics
                .insert(topic_name.to_string(), Topic::new(topic_name));
            println!("[broker] Topic '{}' auto-created", topic_name);
        }
    }

    // ----------------------------------------------------------
    // REGISTER_PRODUCER <id> <topic> <port>
    // Auto-creates topic if it doesn't exist.
    // ----------------------------------------------------------
    pub fn register_producer(&mut self, producer_id: &str, topic_name: &str) -> String {
        // Lazy topic creation — no separate CREATE_TOPIC needed
        self.create_topic_if_missing(topic_name);
        self.producers
            .insert(producer_id.to_string(), topic_name.to_string());
        format!(
            "OK producer '{}' registered to '{}'\n",
            producer_id, topic_name
        )
    }

    // ----------------------------------------------------------
    // PRODUCE <message>
    // Raw bytes — matches ring buffer's byte-oriented storage.
    // ----------------------------------------------------------
    pub fn produce(&mut self, topic_name: &str, payload: &[u8]) -> String {
        match self.topics.get_mut(topic_name) {
            None => format!("ERR topic '{}' does not exist\n", topic_name),
            Some(topic) => {
                let offset = topic.append(payload);
                format!("OK offset {}\n", offset)
            }
        }
    }

    // ----------------------------------------------------------
    // CONSUME <topic> <group>
    // Returns raw bytes as a UTF-8 string for now.
    // Consumer offset advances only on successful read.
    // ----------------------------------------------------------
    pub fn consume(&mut self, topic_name: &str, group_name: &str) -> String {
        if !self.topics.contains_key(topic_name) {
            return format!("ERR topic '{}' does not exist\n", topic_name);
        }

        let group = self
            .groups
            .entry(group_name.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_name));

        let offset = group.get_offset(topic_name);
        let topic = self.topics.get(topic_name).unwrap();

        match topic.read_at(offset) {
            None => format!("EMPTY no messages at offset {}\n", offset),
            Some(bytes) => {
                let payload = String::from_utf8_lossy(bytes).to_string();
                let response = format!("MSG offset={} payload={}\n", offset, payload);
                self.groups.get_mut(group_name).unwrap().advance(topic_name);
                response
            }
        }
    }
}

// ============================================================
// Tests
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Broker {
        let mut broker = Broker::new();
        // Register a producer — this auto-creates the topic
        broker.register_producer("prod-1", "orders");
        broker
    }

    #[test]
    fn test_register_producer_auto_creates_topic() {
        let mut broker = Broker::new();
        let res = broker.register_producer("prod-1", "orders");
        assert!(res.contains("OK"));
        // Topic should exist now — produce should work
        let res2 = broker.produce("orders", b"test");
        assert!(res2.contains("OK"));
    }

    #[test]
    fn test_register_same_topic_twice_does_not_fail() {
        let mut broker = Broker::new();
        broker.register_producer("prod-1", "orders");
        // Second producer on same topic — topic already exists, should still succeed
        let res = broker.register_producer("prod-2", "orders");
        assert!(res.contains("OK"));
    }

    #[test]
    fn test_produce_appends_and_returns_offset() {
        let mut broker = setup();
        let r1 = broker.produce("orders", b"msg-a");
        let r2 = broker.produce("orders", b"msg-b");
        assert!(r1.contains("offset 0"));
        assert!(r2.contains("offset 1"));
    }

    #[test]
    fn test_produce_to_missing_topic_fails() {
        let mut broker = Broker::new();
        let res = broker.produce("ghost", b"hello");
        assert!(res.contains("ERR"));
    }

    #[test]
    fn test_consume_returns_messages_in_order() {
        let mut broker = setup();
        broker.produce("orders", b"first");
        broker.produce("orders", b"second");

        let r1 = broker.consume("orders", "analytics");
        let r2 = broker.consume("orders", "analytics");
        assert!(r1.contains("first"));
        assert!(r2.contains("second"));
    }

    #[test]
    fn test_consume_empty_returns_empty() {
        let mut broker = setup();
        let res = broker.consume("orders", "analytics");
        assert!(res.contains("EMPTY"));
    }

    #[test]
    fn test_two_groups_independent() {
        let mut broker = setup();
        broker.produce("orders", b"msg-a");
        broker.produce("orders", b"msg-b");

        let a1 = broker.consume("orders", "group-a");
        // group-b starts fresh at offset 0
        let b1 = broker.consume("orders", "group-b");
        assert!(a1.contains("msg-a"));
        assert!(b1.contains("msg-a"));

        let a2 = broker.consume("orders", "group-a");
        assert!(a2.contains("msg-b"));
    }

    #[test]
    fn test_messages_persist_after_consume() {
        let mut broker = setup();
        broker.produce("orders", b"persistent");
        broker.consume("orders", "group-a");
        // group-b can still read the same message
        let res = broker.consume("orders", "group-b");
        assert!(res.contains("persistent"));
    }
}
