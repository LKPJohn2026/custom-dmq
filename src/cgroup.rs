//! Consumer group with partition queues.

use crate::partition::Partition;

pub struct ConsumerGroup {
    pub group_id: u16,
    pub partitions: Vec<Partition>,
    consumer_count: u32,
}

impl ConsumerGroup {
    pub fn new(group_id: u16) -> Self {
        ConsumerGroup {
            group_id,
            partitions: vec![Partition::new()],
            consumer_count: 0,
        }
    }

    /// Assign a partition index for a newly registered consumer connection.
    pub fn assign_partition(&mut self) -> u16 {
        self.consumer_count += 1;
        if self.partitions.len() < self.consumer_count as usize {
            self.partitions.push(Partition::new());
        }
        (self.partitions.len() - 1) as u16
    }

    /// Index of the partition with the fewest buffered messages.
    pub fn smallest_partition_index(&self) -> usize {
        let mut best = 0usize;
        let mut min_len = self.partitions[0].len();
        for (idx, partition) in self.partitions.iter().enumerate().skip(1) {
            let len = partition.len();
            if len < min_len {
                min_len = len;
                best = idx;
            }
        }
        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Partition;

    #[test]
    fn new_group_has_one_partition() {
        let group = ConsumerGroup::new(1);
        assert_eq!(group.partitions.len(), 1);
    }

    #[test]
    fn assign_partition_grows_with_consumers() {
        let mut group = ConsumerGroup::new(1);
        assert_eq!(group.assign_partition(), 0);
        assert_eq!(group.assign_partition(), 1);
        assert_eq!(group.partitions.len(), 2);
    }

    #[test]
    fn smallest_partition_picks_shortest_queue() {
        let mut group = ConsumerGroup::new(1);
        group.partitions.push(Partition::new());
        group.partitions[0].append(b"a");
        group.partitions[0].append(b"b");
        assert_eq!(group.smallest_partition_index(), 1);
    }
}
