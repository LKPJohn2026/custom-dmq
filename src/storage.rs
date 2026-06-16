//! Storage abstraction for broker state.
//!
//! Phase 1 migration path:
//! - Start by wrapping the existing local-first implementation behind a trait.
//! - Then introduce a topic-partition append-only log implementation and swap callers.

use std::io;

pub type TopicId = u16;
pub type GroupId = u16;
pub type PartitionIdx = u16;

pub trait Storage: Send + Sync {
    fn ensure_topic(&mut self, topic_id: TopicId) -> io::Result<()>;

    fn ensure_group(&mut self, topic_id: TopicId, group_id: GroupId) -> io::Result<()>;

    fn assign_partition(
        &mut self,
        topic_id: TopicId,
        group_id: GroupId,
    ) -> io::Result<PartitionIdx>;

    /// Produce a payload into the topic. The concrete implementation decides routing.
    fn produce(&mut self, topic_id: TopicId, payload: &[u8]) -> io::Result<(u8, u64)>;

    /// Consume one message from a group's assigned partition (local-first behavior).
    fn consume_one(
        &mut self,
        topic_id: TopicId,
        group_id: GroupId,
        partition_idx: PartitionIdx,
    ) -> io::Result<Option<Vec<u8>>>;
}
