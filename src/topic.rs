// ============================================================
// topic.rs — Ring Buffer Queue + Topic
//
// Storage: flat byte array, allocated once, never grows.
// Each slot is exactly MAX_MSG_SIZE bytes wide.
// head/tail are slot indices that wrap around modulo QUEUE_CAPACITY.
//
// Offset model (Kafka-style, non-destructive):
//   - Every appended message gets a monotonically increasing offset
//   - read_at(offset) looks up the slot: offset % QUEUE_CAPACITY
//   - When the buffer wraps, the oldest messages are silently evicted
//     and base_offset advances — identical to Kafka log retention
//
//  Ring layout (capacity = 8, example):
//
//   slot:  [ 0  1  2  3  4  5  6  7 ]
//            ↑head          ↑tail
//   base_offset = 3  (slots 0,1,2 were evicted when tail wrapped)
//   readable offsets: 3,4,5,6  (tail not yet written)
// ============================================================

pub const MAX_MSG_SIZE: usize = 255;
pub const QUEUE_CAPACITY: usize = 10_000;

pub struct Queue {
    buffer: Box<[u8]>, // MAX_MSG_SIZE * QUEUE_CAPACITY — allocated once
    sizes: Box<[u8]>,  // actual byte length of message in each slot
    head: usize,       // slot index of the oldest readable message
    tail: usize,       // slot index of the next write position
    base_offset: u64,  // global offset of the message currently at head
    next_offset: u64,  // next offset to assign on append
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

    /// Append a message. Returns the offset assigned to it.
    /// If the buffer is full, the oldest message is evicted (base_offset advances),
    /// exactly like Kafka log retention.
    pub fn append(&mut self, payload: &[u8]) -> u64 {
        let len = payload.len().min(MAX_MSG_SIZE);
        let slot = self.tail;

        // Write into the flat buffer at slot * MAX_MSG_SIZE
        let start = slot * MAX_MSG_SIZE;
        self.buffer[start..start + len].copy_from_slice(&payload[..len]);
        self.sizes[slot] = len as u8;

        // Advance tail
        self.tail = (self.tail + 1) % QUEUE_CAPACITY;

        // If tail has caught up to head, the ring is full.
        // Evict the oldest message by advancing head and base_offset.
        if self.tail == self.head {
            self.head = (self.head + 1) % QUEUE_CAPACITY;
            self.base_offset += 1;
        }

        let offset = self.next_offset;
        self.next_offset += 1;
        offset
    }

    /// Read a message at the given global offset.
    /// Returns None if the offset has been evicted or hasn't been written yet.
    pub fn read_at(&self, offset: u64) -> Option<&[u8]> {
        // Not yet written
        if offset >= self.next_offset {
            return None;
        }
        // Evicted — too old for the ring
        if offset < self.base_offset {
            return None;
        }

        let slot = (offset % QUEUE_CAPACITY as u64) as usize;
        let len = self.sizes[slot] as usize;
        let start = slot * MAX_MSG_SIZE;
        Some(&self.buffer[start..start + len])
    }

    /// Total messages ever appended (not the current live count).
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

// ============================================================
// Topic — wraps Queue, adds a name
// ============================================================
pub struct Topic {
    pub name: String,
    pub queue: Queue,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        Topic {
            name: name.to_string(),
            queue: Queue::new(),
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
}

// ============================================================
// Tests
// ============================================================
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
    fn test_read_at_unwritten_offset_returns_none() {
        let q = Queue::new();
        assert!(q.read_at(0).is_none());
    }

    #[test]
    fn test_messages_non_destructive() {
        let mut q = Queue::new();
        q.append(b"persistent");
        // Reading does not remove the message
        q.read_at(0);
        q.read_at(0);
        assert_eq!(q.read_at(0).unwrap(), b"persistent");
    }

    #[test]
    fn test_ring_wraps_and_evicts_oldest() {
        // Small capacity to trigger wrap quickly
        let mut q = Queue::new();
        // Fill the entire ring
        for i in 0..QUEUE_CAPACITY {
            q.append(format!("msg-{}", i).as_bytes());
        }
        // Offset 0 is still readable (ring just full, not yet overwritten)
        assert!(q.read_at(0).is_some());

        // One more push evicts offset 0
        q.append(b"overflow");
        assert!(q.read_at(0).is_none()); // evicted
        assert!(q.read_at(1).is_some()); // still readable
    }

    #[test]
    fn test_payload_truncated_to_max_size() {
        let mut q = Queue::new();
        let big = vec![b'x'; MAX_MSG_SIZE + 50];
        let offset = q.append(&big);
        let read = q.read_at(offset).unwrap();
        assert_eq!(read.len(), MAX_MSG_SIZE);
    }

    #[test]
    fn test_topic_wraps_queue_correctly() {
        let mut topic = Topic::new("orders");
        let o1 = topic.append(b"order-1");
        let o2 = topic.append(b"order-2");
        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
        assert_eq!(topic.read_at(0).unwrap(), b"order-1");
    }
}
