//! Per-group partition with a memory-mapped message queue.

use dmq_storage::mmap_queue::MmapQueue;
use std::io;
use std::path::Path;

pub struct Partition {
    pub queue: MmapQueue,
}

impl Partition {
    pub fn open(
        data_dir: &Path,
        topic_id: u16,
        group_id: u16,
        partition_id: u16,
    ) -> io::Result<Self> {
        Ok(Partition {
            queue: MmapQueue::open(data_dir, topic_id, group_id, partition_id)?,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> u64 {
        self.queue.append(payload)
    }

    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> u64 {
        self.queue.live_len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_pop_preserves_order() {
        let dir = tempdir().unwrap();
        let mut p = Partition::open(dir.path(), 1, 1, 1).unwrap();
        p.append(b"a");
        p.append(b"b");
        assert_eq!(p.pop_front().unwrap(), b"a");
        assert_eq!(p.pop_front().unwrap(), b"b");
        assert!(p.pop_front().is_none());
    }
}
