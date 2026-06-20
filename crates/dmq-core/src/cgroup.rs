//! Consumer group with partition queues.

use crate::partition::Partition;
use dmq_storage::metadata::store_cgroup_partition_count;
use std::io;
use std::path::Path;

pub struct ConsumerGroup {
    pub group_id: u16,
    pub partitions: Vec<Partition>,
    consumer_count: u32,
}

impl ConsumerGroup {
    pub fn load(data_dir: &Path, topic_id: u16, group_id: u16) -> io::Result<Self> {
        let count =
            dmq_storage::metadata::load_cgroup_partition_count(data_dir, topic_id, group_id)?;
        let partition_count = count.max(1);
        let mut partitions = Vec::with_capacity(partition_count as usize);
        for i in 0..partition_count {
            let partition_id = i as u16 + 1;
            partitions.push(Partition::open(data_dir, topic_id, group_id, partition_id)?);
        }
        Ok(ConsumerGroup {
            group_id,
            partitions,
            consumer_count: partition_count,
        })
    }

    pub fn create_new(data_dir: &Path, topic_id: u16, group_id: u16) -> io::Result<Self> {
        store_cgroup_partition_count(data_dir, topic_id, group_id, 1)?;
        Ok(ConsumerGroup {
            group_id,
            partitions: vec![Partition::open(data_dir, topic_id, group_id, 1)?],
            consumer_count: 0,
        })
    }

    /// Assign a partition index for a newly registered consumer connection.
    pub fn assign_partition(&mut self, data_dir: &Path, topic_id: u16) -> io::Result<u16> {
        self.consumer_count += 1;
        if self.partitions.len() < self.consumer_count as usize {
            let partition_id = self.partitions.len() as u16 + 1;
            self.partitions.push(Partition::open(
                data_dir,
                topic_id,
                self.group_id,
                partition_id,
            )?);
            store_cgroup_partition_count(
                data_dir,
                topic_id,
                self.group_id,
                self.partitions.len() as u32,
            )?;
        }
        Ok((self.partitions.len() - 1) as u16)
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
    use tempfile::tempdir;

    #[test]
    fn new_group_has_one_partition() {
        let dir = tempdir().unwrap();
        let group = ConsumerGroup::create_new(dir.path(), 1, 1).unwrap();
        assert_eq!(group.partitions.len(), 1);
    }

    #[test]
    fn assign_partition_grows_with_consumers() {
        let dir = tempdir().unwrap();
        let mut group = ConsumerGroup::create_new(dir.path(), 1, 1).unwrap();
        assert_eq!(group.assign_partition(dir.path(), 1).unwrap(), 0);
        assert_eq!(group.assign_partition(dir.path(), 1).unwrap(), 1);
        assert_eq!(group.partitions.len(), 2);
    }

    #[test]
    fn smallest_partition_picks_shortest_queue() {
        let dir = tempdir().unwrap();
        let mut group = ConsumerGroup::create_new(dir.path(), 1, 1).unwrap();
        group
            .partitions
            .push(Partition::open(dir.path(), 1, 1, 2).unwrap());
        group.partitions[0].append(b"a");
        group.partitions[0].append(b"b");
        assert_eq!(group.smallest_partition_index(), 1);
    }
}
