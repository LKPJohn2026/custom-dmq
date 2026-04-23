// consumer.rs — owns ConsumerGroup and offset tracking
// No dependencies on other local modules

use std::collections::HashMap;

pub struct ConsumerGroup {
    pub name: String,
    offsets: HashMap<String, u64>, // topic_name -> next offset to read
}

impl ConsumerGroup {
    pub fn new(name: &str) -> Self {
        ConsumerGroup {
            name: name.to_string(),
            offsets: HashMap::new(),
        }
    }

    // Returns the offset this group will read NEXT from a topic
    // Defaults to 0 if this group has never read from that topic
    pub fn get_offset(&self, topic: &str) -> u64 {
        *self.offsets.get(topic).unwrap_or(&0)
    }

    // Advances the offset by 1 after a successful consume
    pub fn advance(&mut self, topic: &str) {
        let current = self.get_offset(topic);
        self.offsets.insert(topic.to_string(), current + 1);
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_group_starts_at_offset_zero() {
        let group = ConsumerGroup::new("payments-service");
        assert_eq!(group.get_offset("orders"), 0);
    }

    #[test]
    fn advance_increments_offset() {
        let mut group = ConsumerGroup::new("payments-service");
        group.advance("orders");
        group.advance("orders");
        assert_eq!(group.get_offset("orders"), 2);
    }

    #[test]
    fn two_groups_track_offsets_independently() {
        let mut g1 = ConsumerGroup::new("payments-service");
        let mut g2 = ConsumerGroup::new("analytics-service");

        g1.advance("orders");
        g1.advance("orders");
        g1.advance("orders");
        g2.advance("orders");

        // g1 is ahead, g2 is behind — same topic, different pointers
        assert_eq!(g1.get_offset("orders"), 3);
        assert_eq!(g2.get_offset("orders"), 1);
    }

    #[test]
    fn offsets_are_tracked_per_topic() {
        let mut group = ConsumerGroup::new("analytics-service");
        group.advance("orders");
        group.advance("orders");
        group.advance("payments");

        assert_eq!(group.get_offset("orders"), 2);
        assert_eq!(group.get_offset("payments"), 1);
        assert_eq!(group.get_offset("inventory"), 0); // never read
    }
}
