//! In-process counters and latency histograms for broker operations.

use std::sync::atomic::{AtomicU64, Ordering};

const LATENCY_BUCKETS_MS: &[u64] = &[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500];

#[derive(Debug, Default)]
pub struct BrokerMetrics {
    produce_total: AtomicU64,
    produce_bytes: AtomicU64,
    fetch_total: AtomicU64,
    fetch_bytes: AtomicU64,
    fetch_records: AtomicU64,
    commit_total: AtomicU64,
    request_errors: AtomicU64,
    request_latency_sum_ms: AtomicU64,
    request_latency_count: AtomicU64,
    latency_buckets: [AtomicU64; 10],
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

    pub fn record_request_latency(&self, latency_ms: u64) {
        self.request_latency_sum_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.request_latency_count.fetch_add(1, Ordering::Relaxed);
        for (idx, bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if latency_ms <= *bound {
                self.latency_buckets[idx].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = format!(
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
             dmq_request_errors {}\n\
             # HELP dmq_request_latency_ms Request latency sum in milliseconds\n\
             # TYPE dmq_request_latency_ms counter\n\
             dmq_request_latency_ms_sum {}\n\
             dmq_request_latency_ms_count {}\n",
            self.produce_total.load(Ordering::Relaxed),
            self.produce_bytes.load(Ordering::Relaxed),
            self.fetch_total.load(Ordering::Relaxed),
            self.fetch_bytes.load(Ordering::Relaxed),
            self.fetch_records.load(Ordering::Relaxed),
            self.commit_total.load(Ordering::Relaxed),
            self.request_errors.load(Ordering::Relaxed),
            self.request_latency_sum_ms.load(Ordering::Relaxed),
            self.request_latency_count.load(Ordering::Relaxed),
        );
        out.push_str("# HELP dmq_request_latency_ms_bucket Request latency histogram buckets\n");
        out.push_str("# TYPE dmq_request_latency_ms_bucket counter\n");
        for (idx, bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            out.push_str(&format!(
                "dmq_request_latency_ms_bucket{{le=\"{bound}\"}} {}\n",
                self.latency_buckets[idx].load(Ordering::Relaxed)
            ));
        }
        out.push_str(&format!(
            "dmq_request_latency_ms_bucket{{le=\"+Inf\"}} {}\n",
            self.request_latency_count.load(Ordering::Relaxed)
        ));
        out
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
        m.record_request_latency(3);
        let out = m.render_prometheus();
        assert!(out.contains("dmq_produce_total 1"));
        assert!(out.contains("dmq_fetch_records 1"));
        assert!(out.contains("dmq_request_latency_ms_bucket"));
    }
}
