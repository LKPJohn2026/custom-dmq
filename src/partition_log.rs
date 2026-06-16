//! Append-only partition log with offset-based fetch.
//!
//! This is the Phase 1 foundation for Kafka-shaped semantics:
//! - appends return monotonically increasing offsets
//! - consumers fetch from an offset without destructively removing data
//! - retention can evict old records while maintaining a base offset

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FetchResult {
    pub base_offset: u64,
    pub next_offset: u64,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone)]
pub struct PartitionLog {
    base_offset: u64,
    next_offset: u64,
    records: Vec<Record>,
    max_records: Option<usize>,
}

impl Default for PartitionLog {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionLog {
    pub fn new() -> Self {
        PartitionLog {
            base_offset: 0,
            next_offset: 0,
            records: Vec::new(),
            max_records: None,
        }
    }

    pub fn with_max_records(max_records: usize) -> Self {
        PartitionLog {
            max_records: Some(max_records),
            ..Self::new()
        }
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn append(&mut self, payload: &[u8]) -> u64 {
        let offset = self.next_offset;
        self.next_offset += 1;
        self.records.push(Record {
            offset,
            payload: payload.to_vec(),
        });
        self.enforce_retention();
        offset
    }

    /// Fetch records starting at `from_offset` (inclusive), constrained by `max_bytes`.
    ///
    /// - If `from_offset` is before `base_offset`, results start at `base_offset`
    ///   (simulating "offset out of range" recovery as "start at earliest available").
    /// - If `from_offset >= next_offset`, returns an empty batch.
    pub fn fetch(&self, from_offset: u64, max_bytes: usize) -> FetchResult {
        let start_offset = from_offset.max(self.base_offset);
        if start_offset >= self.next_offset || self.records.is_empty() {
            return FetchResult {
                base_offset: self.base_offset,
                next_offset: self.next_offset,
                records: Vec::new(),
            };
        }

        let start_idx = (start_offset - self.base_offset) as usize;
        let mut out = Vec::new();
        let mut used = 0usize;

        for rec in self.records.iter().skip(start_idx) {
            let cost = rec.payload.len();
            if !out.is_empty() && used.saturating_add(cost) > max_bytes {
                break;
            }
            used = used.saturating_add(cost);
            out.push(rec.clone());
        }

        FetchResult {
            base_offset: self.base_offset,
            next_offset: self.next_offset,
            records: out,
        }
    }

    fn enforce_retention(&mut self) {
        let Some(max_records) = self.max_records else {
            return;
        };
        if self.records.len() <= max_records {
            return;
        }

        let excess = self.records.len() - max_records;
        self.records.drain(0..excess);
        self.base_offset += excess as u64;

        // Ensure base_offset always points at the first record.
        if let Some(first) = self.records.first() {
            self.base_offset = first.offset;
        } else {
            self.base_offset = self.next_offset;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_returns_monotonic_offsets() {
        let mut log = PartitionLog::new();
        assert_eq!(log.append(b"a"), 0);
        assert_eq!(log.append(b"b"), 1);
        assert_eq!(log.append(b"c"), 2);
        assert_eq!(log.base_offset(), 0);
        assert_eq!(log.next_offset(), 3);
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn fetch_from_offset_returns_expected_records() {
        let mut log = PartitionLog::new();
        log.append(b"a");
        log.append(b"bb");
        log.append(b"ccc");

        let r = log.fetch(1, 1024);
        assert_eq!(r.records.len(), 2);
        assert_eq!(r.records[0].offset, 1);
        assert_eq!(r.records[0].payload, b"bb".to_vec());
        assert_eq!(r.records[1].offset, 2);
        assert_eq!(r.records[1].payload, b"ccc".to_vec());
    }

    #[test]
    fn fetch_respects_max_bytes_with_at_least_one_record() {
        let mut log = PartitionLog::new();
        log.append(b"a");
        log.append(b"bb");
        log.append(b"ccc");

        let r = log.fetch(0, 1);
        assert_eq!(r.records.len(), 1);
        assert_eq!(r.records[0].payload, b"a".to_vec());

        let r = log.fetch(1, 2);
        assert_eq!(r.records.len(), 1);
        assert_eq!(r.records[0].payload, b"bb".to_vec());
    }

    #[test]
    fn retention_evicts_oldest_and_advances_base_offset() {
        let mut log = PartitionLog::with_max_records(3);
        log.append(b"0");
        log.append(b"1");
        log.append(b"2");
        assert_eq!(log.base_offset(), 0);
        assert_eq!(log.len(), 3);

        log.append(b"3");
        assert_eq!(log.len(), 3);
        assert_eq!(log.base_offset(), 1);
        assert_eq!(log.next_offset(), 4);

        let r = log.fetch(0, 1024);
        assert_eq!(r.records[0].offset, 1);
        assert_eq!(r.records[0].payload, b"1".to_vec());
    }
}
