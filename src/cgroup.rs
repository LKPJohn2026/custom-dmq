//! Consumer group state.
//!
//! Each group belongs to a topic and tracks the next log offset to deliver.
//! Multiple groups on one topic read independently from the same append-only log.

pub struct ConsumerGroup {
    pub group_id: u16,
    /// Next log offset this group will receive.
    pub offset: u64,
}

impl ConsumerGroup {
    pub fn new(group_id: u16) -> Self {
        ConsumerGroup {
            group_id,
            offset: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_group_starts_at_offset_zero() {
        let group = ConsumerGroup::new(1);
        assert_eq!(group.offset, 0);
    }
}
