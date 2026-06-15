//! Per-group partition with its own message queue.

use crate::topic::Queue;

pub struct Partition {
    pub queue: Queue,
}

impl Default for Partition {
    fn default() -> Self {
        Self::new()
    }
}

impl Partition {
    pub fn new() -> Self {
        Partition {
            queue: Queue::new(),
        }
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
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_pop_preserves_order() {
        let mut p = Partition::new();
        p.append(b"a");
        p.append(b"b");
        assert_eq!(p.pop_front().unwrap(), b"a");
        assert_eq!(p.pop_front().unwrap(), b"b");
        assert!(p.pop_front().is_none());
    }
}
