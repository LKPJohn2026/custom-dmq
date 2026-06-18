//! In-process counters for broker operations.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct BrokerMetrics {
    produce_total: AtomicU64,
    produce_bytes: AtomicU64,
    fetch_total: AtomicU64,
    fetch_bytes: AtomicU64,
    fetch_records: AtomicU64,
    commit_total: AtomicU64,
    request_errors: AtomicU64,
}

impl BrokerMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_produce(&self, bytes: usize) {
        self.produce_total.fetch_add(1, Ordering::Relaxed);
        self.produce_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_fetch(&self, bytes: usize, records: usize) {
        self.fetch_total.fetch_add(1, Ordering::Relaxed);
        self.fetch_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.fetch_records
            .fetch_add(records as u64, Ordering::Relaxed);
    }

    pub fn record_commit(&self) {
        self.commit_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.request_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        format!(
            "# HELP dmq_produce_total Total produce requests\n\
             # TYPE dmq_produce_total counter\n\
             dmq_produce_total {}\n\
             # HELP dmq_produce_bytes Total bytes produced\n\
             # TYPE dmq_produce_bytes counter\n\
             dmq_produce_bytes {}\n\
             # HELP dmq_fetch_total Total fetch requests\n\
             # TYPE dmq_fetch_total counter\n\
             dmq_fetch_total {}\n\
             # HELP dmq_fetch_bytes Total bytes fetched\n\
             # TYPE dmq_fetch_bytes counter\n\
             dmq_fetch_bytes {}\n\
             # HELP dmq_fetch_records Total records returned by fetch\n\
             # TYPE dmq_fetch_records counter\n\
             dmq_fetch_records {}\n\
             # HELP dmq_commit_total Total offset commits\n\
             # TYPE dmq_commit_total counter\n\
             dmq_commit_total {}\n\
             # HELP dmq_request_errors Total request errors\n\
             # TYPE dmq_request_errors counter\n\
             dmq_request_errors {}\n",
            self.produce_total.load(Ordering::Relaxed),
            self.produce_bytes.load(Ordering::Relaxed),
            self.fetch_total.load(Ordering::Relaxed),
            self.fetch_bytes.load(Ordering::Relaxed),
            self.fetch_records.load(Ordering::Relaxed),
            self.commit_total.load(Ordering::Relaxed),
            self.request_errors.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        let m = BrokerMetrics::new();
        m.record_produce(10);
        m.record_fetch(5, 1);
        m.record_commit();
        let out = m.render_prometheus();
        assert!(out.contains("dmq_produce_total 1"));
        assert!(out.contains("dmq_fetch_records 1"));
    }
}
