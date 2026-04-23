// topic.rs — owns Message and Topic
// No dependencies on other local modules

pub struct Message {
    pub offset: u64,
    pub payload: String,
}

pub struct Topic {
    pub name: String,
    log: Vec<Message>, // append-only — messages are NEVER removed (Kafka core idea)
    next_offset: u64,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        Topic {
            name: name.to_string(),
            log: Vec::new(),
            next_offset: 0,
        }
    }

    // Appends a message to the log, returns the assigned offset
    pub fn append(&mut self, payload: String) -> u64 {
        let offset = self.next_offset;
        self.log.push(Message { offset, payload });
        self.next_offset += 1;
        offset
    }

    // Reads the message AT a specific offset (consumer controls this pointer)
    pub fn read_at(&self, offset: u64) -> Option<&Message> {
        self.log.get(offset as usize)
    }

    pub fn len(&self) -> u64 {
        self.next_offset
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_assigns_sequential_offsets() {
        let mut topic = Topic::new("orders");
        let o1 = topic.append("msg-a".to_string());
        let o2 = topic.append("msg-b".to_string());
        let o3 = topic.append("msg-c".to_string());
        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
        assert_eq!(o3, 2);
    }

    #[test]
    fn read_at_returns_correct_payload() {
        let mut topic = Topic::new("orders");
        topic.append("hello".to_string());
        topic.append("world".to_string());

        assert_eq!(topic.read_at(0).unwrap().payload, "hello");
        assert_eq!(topic.read_at(1).unwrap().payload, "world");
    }

    #[test]
    fn read_at_out_of_bounds_returns_none() {
        let topic = Topic::new("empty");
        assert!(topic.read_at(0).is_none());
    }

    #[test]
    fn messages_are_never_removed_after_consume() {
        let mut topic = Topic::new("orders");
        topic.append("first".to_string());

        // Simulate a consumer reading offset 0
        let msg = topic.read_at(0);
        assert!(msg.is_some()); // still there

        // Append more, original is still accessible
        topic.append("second".to_string());
        assert_eq!(topic.read_at(0).unwrap().payload, "first");
        assert_eq!(topic.len(), 2);
    }
}
