//! Topic staging queue and consumer groups.

use crate::cgroup::ConsumerGroup;
use crate::partition::Partition;
use dmq_storage::constants::{MAX_MSG_SIZE, QUEUE_CAPACITY};
use dmq_storage::metadata::store_topic_groups;
use dmq_storage::mmap_queue::{MmapQueue, STAGING_GROUP_ID, STAGING_PARTITION_ID};
use std::io;
use std::path::{Path, PathBuf};

/// In-memory ring buffer used by unit tests for offset semantics.
pub struct Queue {
    buffer: Box<[u8]>,
    sizes: Box<[u8]>,
    head: usize,
    tail: usize,
    base_offset: u64,
    next_offset: u64,
}

impl Default for Queue {
    fn default() -> Self {
        Self::new()
    }
}

impl Queue {
    pub fn new() -> Self {
        Queue {
            buffer: vec![0u8; MAX_MSG_SIZE * QUEUE_CAPACITY].into_boxed_slice(),
            sizes: vec![0u8; QUEUE_CAPACITY].into_boxed_slice(),
            head: 0,
            tail: 0,
            base_offset: 0,
            next_offset: 0,
        }
    }

    pub fn append(&mut self, payload: &[u8]) -> u64 {
        if self.next_offset - self.base_offset == QUEUE_CAPACITY as u64 {
            self.head = (self.head + 1) % QUEUE_CAPACITY;
            self.base_offset += 1;
        }

        let len = payload.len().min(MAX_MSG_SIZE);
        let slot = self.tail;

        let start = slot * MAX_MSG_SIZE;
        self.buffer[start..start + len].copy_from_slice(&payload[..len]);
        self.sizes[slot] = len as u8;

        self.tail = (self.tail + 1) % QUEUE_CAPACITY;

        let offset = self.next_offset;
        self.next_offset += 1;
        offset
    }

    pub fn read_at(&self, offset: u64) -> Option<&[u8]> {
        if offset >= self.next_offset {
            return None;
        }
        if offset < self.base_offset {
            return None;
        }

        let slot = (offset % QUEUE_CAPACITY as u64) as usize;
        let len = self.sizes[slot] as usize;
        let start = slot * MAX_MSG_SIZE;
        Some(&self.buffer[start..start + len])
    }

    pub fn live_len(&self) -> u64 {
        self.next_offset.saturating_sub(self.base_offset)
    }

    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        if self.base_offset >= self.next_offset {
            return None;
        }
        let bytes = self.read_at(self.base_offset)?.to_vec();
        self.head = (self.head + 1) % QUEUE_CAPACITY;
        self.base_offset += 1;
        Some(bytes)
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

pub struct Topic {
    pub topic_id: u16,
    pub staging: MmapQueue,
    pub cgroups: Vec<ConsumerGroup>,
    data_dir: PathBuf,
}

impl Topic {
    pub fn load(data_dir: &Path, topic_id: u16) -> io::Result<Self> {
        let staging = MmapQueue::open(data_dir, topic_id, STAGING_GROUP_ID, STAGING_PARTITION_ID)?;
        let group_ids = dmq_storage::metadata::load_topic_groups(data_dir, topic_id)?;
        let mut cgroups = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            cgroups.push(ConsumerGroup::load(data_dir, topic_id, group_id)?);
        }
        Ok(Topic {
            topic_id,
            staging,
            cgroups,
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn create(data_dir: &Path, topic_id: u16) -> io::Result<Self> {
        Ok(Topic {
            topic_id,
            staging: MmapQueue::open(data_dir, topic_id, STAGING_GROUP_ID, STAGING_PARTITION_ID)?,
            cgroups: Vec::new(),
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn append_to_staging(&mut self, payload: &[u8]) -> u64 {
        self.staging.append(payload)
    }

    pub fn drain_staging_into(&mut self, partition: &mut Partition) {
        while let Some(msg) = self.staging.pop_front() {
            partition.append(&msg);
        }
    }

    pub fn find_or_create_group(&mut self, group_id: u16) -> io::Result<bool> {
        if self.cgroups.iter().any(|g| g.group_id == group_id) {
            return Ok(false);
        }
        let group = ConsumerGroup::create_new(&self.data_dir, self.topic_id, group_id)?;
        self.cgroups.push(group);
        self.persist_groups()?;
        Ok(true)
    }

    pub fn persist_groups(&self) -> io::Result<()> {
        let group_ids: Vec<u16> = self.cgroups.iter().map(|g| g.group_id).collect();
        store_topic_groups(&self.data_dir, self.topic_id, &group_ids)
    }

    pub fn group(&self, group_id: u16) -> Option<&ConsumerGroup> {
        self.cgroups.iter().find(|g| g.group_id == group_id)
    }

    pub fn group_mut(&mut self, group_id: u16) -> Option<&mut ConsumerGroup> {
        self.cgroups.iter_mut().find(|g| g.group_id == group_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_returns_ascending_offsets() {
        let mut q = Queue::new();
        assert_eq!(q.append(b"a"), 0);
        assert_eq!(q.append(b"b"), 1);
        assert_eq!(q.append(b"c"), 2);
    }

    #[test]
    fn test_pop_front_fifo() {
        let mut q = Queue::new();
        q.append(b"first");
        q.append(b"second");
        assert_eq!(q.pop_front().unwrap(), b"first");
        assert_eq!(q.pop_front().unwrap(), b"second");
    }

    #[test]
    fn test_ring_wraps_and_evicts_oldest() {
        let mut q = Queue::new();
        for i in 0..QUEUE_CAPACITY {
            q.append(format!("msg-{}", i).as_bytes());
        }
        assert!(q.read_at(0).is_some());
        q.append(b"overflow");
        assert!(q.read_at(0).is_none());
        assert!(q.read_at(1).is_some());
    }
}
