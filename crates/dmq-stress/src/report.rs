//! Stress report formatting.

use hdrhistogram::Histogram;
use std::time::Duration;

#[derive(Debug)]
pub struct LatencySnapshot {
    pub count: u64,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

pub fn latency_snapshot(hist: &Histogram<u64>) -> LatencySnapshot {
    let count = hist.len();
    if count == 0 {
        return LatencySnapshot {
            count: 0,
            avg_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
        };
    }
    LatencySnapshot {
        count,
        avg_ms: hist.mean() / 1000.0,
        p50_ms: hist.value_at_quantile(0.50) as f64 / 1000.0,
        p95_ms: hist.value_at_quantile(0.95) as f64 / 1000.0,
        p99_ms: hist.value_at_quantile(0.99) as f64 / 1000.0,
        max_ms: hist.max() as f64 / 1000.0,
    }
}

pub fn print_stress_report(
    run_id: &str,
    target_rps: u64,
    duration: Duration,
    acked: u64,
    errors: u64,
    latency: &LatencySnapshot,
    ledger_path: &str,
) {
    let secs = duration.as_secs_f64();
    let achieved = if secs > 0.0 { acked as f64 / secs } else { 0.0 };
    let error_pct = if acked + errors == 0 {
        0.0
    } else {
        (errors as f64 / (acked + errors) as f64) * 100.0
    };

    println!("---- dmq stress report ----");
    println!("run_id         : {run_id}");
    println!("duration       : {secs:.2}s");
    println!("target rps     : {target_rps}");
    println!("achieved rps   : {achieved:.1} ({acked} acked produces)");
    println!("errors         : {errors} ({error_pct:.3}%)");
    println!(
        "produce p50/p95/p99 : {:.1} / {:.1} / {:.1} ms",
        latency.p50_ms, latency.p95_ms, latency.p99_ms
    );
    println!(
        "produce avg/max   : {:.1} / {:.1} ms",
        latency.avg_ms, latency.max_ms
    );
    println!("ledger         : {ledger_path}");
}
