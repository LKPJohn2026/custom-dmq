//! Consumer group state for push delivery.
//!
//! Added with consumer registration: each group belongs to a topic, holds
//! registered consumer connections, and tracks the next log offset to deliver.
//! Multiple groups on one topic read independently from the same append-only log.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

/// Push request from the group delivery loop to a consumer connection task.
pub struct PushRequest {
    pub payload: Vec<u8>,
    pub ack: oneshot::Sender<()>,
}

/// Handle to a registered consumer's dial-back connection.
pub struct ConsumerHandle {
    pub port: u16,
    pub ready: Arc<AtomicBool>,
    pub push_tx: mpsc::Sender<PushRequest>,
}

pub struct ConsumerGroup {
    pub group_id: u16,
    /// Next log offset this group will receive.
    pub offset: u64,
    pub consumers: Vec<ConsumerHandle>,
}

impl ConsumerGroup {
    pub fn new(group_id: u16) -> Self {
        ConsumerGroup {
            group_id,
            offset: 0,
            consumers: Vec::new(),
        }
    }

    pub fn add_consumer(&mut self, handle: ConsumerHandle) {
        self.consumers.push(handle);
    }

    /// Return a consumer that is ready to accept the next pushed message.
    pub fn find_ready_consumer(&self) -> Option<&ConsumerHandle> {
        self.consumers
            .iter()
            .find(|c| c.ready.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_group_starts_at_offset_zero() {
        let group = ConsumerGroup::new(1);
        assert_eq!(group.offset, 0);
        assert!(group.consumers.is_empty());
    }
}
