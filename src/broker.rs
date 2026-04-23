// broker.rs — owns all topics and consumer groups
// Depends on: topic.rs, consumer.rs

use crate::consumer::ConsumerGroup;
use crate::topic::Topic;
use std::collections::HashMap;

pub struct Broker {
    topics: HashMap<String, Topic>,
    groups: HashMap<String, ConsumerGroup>,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            topics: HashMap::new(),
            groups: HashMap::new(),
        }
    }

    pub fn create_topic(&mut self, name: &str) -> String {
        if self.topics.contains_key(name) {
            return format!("ERR topic '{}' already exists\n", name);
        }
        self.topics.insert(name.to_string(), Topic::new(name));
        format!("OK topic '{}' created\n", name)
    }

    pub fn register_producer(&mut self, topic: &str) -> String {
        if !self.topics.contains_key(topic) {
            return format!("ERR topic '{}' does not exist\n", topic);
        }
        format!("OK producer registered on '{}'\n", topic)
    }

    pub fn produce(&mut self, topic: &str, payload: String) -> String {
        match self.topics.get_mut(topic) {
            None => format!("ERR topic '{}' does not exist\n", topic),
            Some(t) => {
                let offset = t.append(payload);
                format!("OK offset {}\n", offset)
            }
        }
    }

    pub fn consume(&mut self, topic: &str, group: &str) -> String {
        // Ensure topic exists
        if !self.topics.contains_key(topic) {
            return format!("ERR topic '{}' does not exist\n", topic);
        }

        // Get or create consumer group
        let cg = self
            .groups
            .entry(group.to_string())
            .or_insert_with(|| ConsumerGroup::new(group));

        let offset = cg.get_offset(topic);

        // Read from topic at group's current offset
        match self.topics.get(topic).unwrap().read_at(offset) {
            None => format!("EMPTY no messages at offset {}\n", offset),
            Some(msg) => {
                let payload = msg.payload.clone();
                // Advance only AFTER successful read
                self.groups.get_mut(group).unwrap().advance(topic);
                format!("MSG offset={} payload={}\n", offset, payload)
            }
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Broker {
        let mut b = Broker::new();
        b.create_topic("orders");
        b
    }

    #[test]
    fn create_topic_succeeds() {
        let mut b = Broker::new();
        let res = b.create_topic("orders");
        assert!(res.starts_with("OK"));
    }

    #[test]
    fn create_duplicate_topic_fails() {
        let mut b = setup();
        let res = b.create_topic("orders");
        assert!(res.starts_with("ERR"));
    }

    #[test]
    fn produce_to_missing_topic_fails() {
        let mut b = Broker::new();
        let res = b.produce("ghost", "msg".to_string());
        assert!(res.starts_with("ERR"));
    }

    #[test]
    fn produce_returns_sequential_offsets() {
        let mut b = setup();
        let r1 = b.produce("orders", "a".to_string());
        let r2 = b.produce("orders", "b".to_string());
        assert!(r1.contains("offset=0") || r1.contains("offset 0"));
        assert!(r2.contains("offset=1") || r2.contains("offset 1"));
    }

    #[test]
    fn consume_returns_messages_in_order() {
        let mut b = setup();
        b.produce("orders", "first".to_string());
        b.produce("orders", "second".to_string());

        let r1 = b.consume("orders", "payments-service");
        let r2 = b.consume("orders", "payments-service");

        assert!(r1.contains("first"));
        assert!(r2.contains("second"));
    }

    #[test]
    fn consume_empty_topic_returns_empty() {
        let mut b = setup();
        let res = b.consume("orders", "payments-service");
        assert!(res.starts_with("EMPTY"));
    }

    #[test]
    fn two_groups_consume_same_topic_independently() {
        let mut b = setup();
        b.produce("orders", "msg-a".to_string());
        b.produce("orders", "msg-b".to_string());

        // payments reads first message
        let p1 = b.consume("orders", "payments-service");
        assert!(p1.contains("msg-a"));

        // analytics hasn't read yet — still gets msg-a
        let a1 = b.consume("orders", "analytics-service");
        assert!(a1.contains("msg-a"));

        // payments continues to msg-b
        let p2 = b.consume("orders", "payments-service");
        assert!(p2.contains("msg-b"));
    }

    #[test]
    fn register_producer_on_missing_topic_fails() {
        let mut b = Broker::new();
        let res = b.register_producer("ghost");
        assert!(res.starts_with("ERR"));
    }

    #[test]
    fn register_producer_succeeds() {
        let mut b = setup();
        let res = b.register_producer("orders");
        assert!(res.starts_with("OK"));
    }
}
