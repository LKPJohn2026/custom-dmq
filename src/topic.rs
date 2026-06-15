//! Append-only ring buffer per topic with monotonic offsets.
//!
//! Topics are keyed by `u16` id (was string name). Each topic now owns a
//! `cgroups` list so every consumer group tracks its own read offset into
//! the shared log without deleting records on delivery.

use crate::cgroup::ConsumerGroup;

pub const MAX_MSG_SIZE: usize = 255;
pub const QUEUE_CAPACITY: usize = 10_000;

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

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

pub struct Topic {
    pub topic_id: u16,
    pub queue: Queue,
    pub cgroups: Vec<ConsumerGroup>,
}

impl Topic {
    pub fn new(topic_id: u16) -> Self {
        Topic {
            topic_id,
            queue: Queue::new(),
            cgroups: Vec::new(),
        }
    }

    pub fn append(&mut self, payload: &[u8]) -> u64 {
        self.queue.append(payload)
    }

    pub fn read_at(&self, offset: u64) -> Option<&[u8]> {
        self.queue.read_at(offset)
    }

    pub fn next_offset(&self) -> u64 {
        self.queue.next_offset()
    }

    pub fn find_or_create_group(&mut self, group_id: u16) -> (bool, usize) {
        if let Some(idx) = self.cgroups.iter().position(|g| g.group_id == group_id) {
            return (false, idx);
        }
        self.cgroups.push(ConsumerGroup::new(group_id));
        (true, self.cgroups.len() - 1)
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
    fn test_read_at_returns_correct_payload() {
        let mut q = Queue::new();
        q.append(b"hello");
        q.append(b"world");
        assert_eq!(q.read_at(0).unwrap(), b"hello");
        assert_eq!(q.read_at(1).unwrap(), b"world");
    }

    #[test]
    fn test_messages_non_destructive() {
        let mut q = Queue::new();
        q.append(b"persistent");
        q.read_at(0);
        q.read_at(0);
        assert_eq!(q.read_at(0).unwrap(), b"persistent");
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

    #[test]
    fn test_two_groups_on_topic_are_independent() {
        let mut topic = Topic::new(1);
        topic.append(b"m1");
        let (_, g1) = topic.find_or_create_group(1);
        let (_, g2) = topic.find_or_create_group(2);
        topic.cgroups[g1].offset = 0;
        topic.cgroups[g2].offset = 0;
        assert_eq!(topic.read_at(topic.cgroups[g1].offset).unwrap(), b"m1");
        assert_eq!(topic.read_at(topic.cgroups[g2].offset).unwrap(), b"m1");
    }
}
