//! Idempotent producer state: deduplicate retries by (producer_id, sequence).

use crate::metadata::{load_all_idempotency, store_idempotency_state};
use std::collections::HashMap;
use std::io;
use std::path::Path;

pub type ProducerId = u64;
pub type Sequence = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyAction {
    ReturnOffset(u64),
    Append,
}

#[derive(Debug, Clone, Default)]
pub struct IdempotencyState {
    pub entries: HashMap<(u16, u16, ProducerId), (Sequence, u64)>,
}

impl IdempotencyState {
    pub fn load(data_dir: &Path) -> io::Result<Self> {
        Ok(IdempotencyState {
            entries: load_all_idempotency(data_dir)?,
        })
    }

    pub fn resolve(
        &self,
        topic_id: u16,
        partition_id: u16,
        producer_id: ProducerId,
        sequence: Sequence,
    ) -> io::Result<IdempotencyAction> {
        let key = (topic_id, partition_id, producer_id);
        if let Some((last_seq, last_offset)) = self.entries.get(&key).copied() {
            if sequence == last_seq {
                return Ok(IdempotencyAction::ReturnOffset(last_offset));
            }
            if sequence < last_seq {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("sequence {sequence} is behind last committed {last_seq}"),
                ));
            }
            if sequence > last_seq + 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "sequence gap: expected {} got {sequence}",
                        last_seq + 1
                    ),
                ));
            }
        } else if sequence != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "first sequence for producer must be 0",
            ));
        }
        Ok(IdempotencyAction::Append)
    }

    pub fn record(
        &mut self,
        data_dir: &Path,
        topic_id: u16,
        partition_id: u16,
        producer_id: ProducerId,
        sequence: Sequence,
        offset: u64,
    ) -> io::Result<()> {
        self.entries
            .insert((topic_id, partition_id, producer_id), (sequence, offset));
        store_idempotency_state(
            data_dir,
            topic_id,
            partition_id,
            producer_id,
            sequence,
            offset,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn duplicate_sequence_returns_cached_offset() {
        let mut state = IdempotencyState::default();
        state.entries.insert((1, 0, 7), (0, 42));
        let action = state.resolve(1, 0, 7, 0).unwrap();
        assert_eq!(action, IdempotencyAction::ReturnOffset(42));
    }
}
