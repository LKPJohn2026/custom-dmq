//! Per-topic configuration for partition count and retention.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    pub topic_id: u16,
    pub partition_count: u16,
    pub max_records: u32,
}

impl TopicConfig {
    pub fn new(topic_id: u16, partition_count: u16, max_records: u32) -> Self {
        TopicConfig {
            topic_id,
            partition_count: partition_count.max(1),
            max_records: max_records.max(1),
        }
    }

    pub fn default_for(topic_id: u16) -> Self {
        Self::new(topic_id, 1, 10_000)
    }
}
