//! Open-loop idempotent produce workload.

use crate::client::{self, encode_payload, ProduceSession};
use crate::config::StressConfig;
use crate::ledger::{default_ledger_path, Ledger};
use crate::report::{latency_snapshot, print_stress_report, LatencySnapshot};
use hdrhistogram::Histogram;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::{sleep_until, Instant as TokioInstant};

#[derive(Debug)]
pub struct StressReport {
    pub run_id: String,
    pub acked: u64,
    pub errors: u64,
    pub latency: LatencySnapshot,
    pub ledger_path: PathBuf,
}

pub async fn run_stress(config: StressConfig) -> Result<StressReport, String> {
    let ledger_path = config
        .ledger_path
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| default_ledger_path(&config.run_id));

    let max_records = config
        .expected_messages()
        .saturating_add(config.workers as u64)
        .max(10_000);
    client::create_topic(config.topic_id, 1, max_records as u32)
        .await
        .map_err(|e| format!("create topic: {e}"))?;

    let ledger = Arc::new(Mutex::new(
        Ledger::open(ledger_path.clone()).map_err(|e| format!("open ledger: {e}"))?,
    ));

    let acked = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 120_000_000, 3).map_err(|e| e.to_string())?,
    ));

    if config.warmup > Duration::ZERO {
        run_window(
            &config,
            Arc::clone(&ledger),
            Arc::clone(&acked),
            Arc::clone(&errors),
            Arc::clone(&hist),
            config.warmup,
            true,
        )
        .await?;
        acked.store(0, Ordering::Relaxed);
        errors.store(0, Ordering::Relaxed);
        {
            let mut guard = hist.lock().await;
            *guard =
                Histogram::<u64>::new_with_bounds(1, 120_000_000, 3).map_err(|e| e.to_string())?;
        }
    }

    let measure_started = Instant::now();
    run_window(
        &config,
        ledger,
        Arc::clone(&acked),
        Arc::clone(&errors),
        Arc::clone(&hist),
        config.duration,
        false,
    )
    .await?;
    let measured = measure_started.elapsed();

    let latency = {
        let guard = hist.lock().await;
        latency_snapshot(&guard)
    };

    print_stress_report(
        &config.run_id,
        config.target_rps,
        measured,
        acked.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
        &latency,
        &ledger_path.display().to_string(),
    );

    Ok(StressReport {
        run_id: config.run_id,
        acked: acked.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        latency,
        ledger_path,
    })
}

async fn run_window(
    config: &StressConfig,
    ledger: Arc<Mutex<Ledger>>,
    acked: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
    window: Duration,
    warmup: bool,
) -> Result<(), String> {
    if warmup {
        eprintln!("[stress] warmup {}s", window.as_secs());
    }

    let next_seq = Arc::new(AtomicU64::new(0));
    let deadline = TokioInstant::now() + window;
    let tick_interval = Duration::from_nanos(
        1_000_000_000u64
            .checked_div(config.target_rps.max(1))
            .unwrap_or(1),
    );

    let (work_tx, work_rx) = tokio::sync::mpsc::channel(config.workers * 4);
    let work_rx = Arc::new(tokio::sync::Mutex::new(work_rx));

    let mut tasks = Vec::with_capacity(config.workers);
    for worker in 0..config.workers {
        let cfg = config.clone();
        let ledger = Arc::clone(&ledger);
        let acked = Arc::clone(&acked);
        let errors = Arc::clone(&errors);
        let hist = Arc::clone(&hist);
        let work_rx = Arc::clone(&work_rx);
        let producer_id = config.producer_id + worker as u64;

        tasks.push(tokio::spawn(async move {
            let mut session = ProduceSession::connect(cfg.topic_id, cfg.partition_id, producer_id)
                .await
                .map_err(|e| e.to_string())?;
            let mut local_seq = 0u64;

            loop {
                let seq = {
                    let mut guard = work_rx.lock().await;
                    guard.recv().await
                };
                let Some(global_seq) = seq else {
                    break;
                };
                let payload = encode_payload(&cfg.run_id, global_seq, cfg.payload_bytes);
                match session.produce(local_seq, payload).await {
                    Ok((_offset, latency)) => {
                        local_seq += 1;
                        acked.fetch_add(1, Ordering::Relaxed);
                        ledger
                            .lock()
                            .await
                            .record(global_seq)
                            .map_err(|e| e.to_string())?;
                        let micros = latency.as_micros().min(u128::from(u64::MAX)) as u64;
                        hist.lock()
                            .await
                            .record(micros)
                            .map_err(|e| e.to_string())?;
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok::<(), String>(())
        }));
    }

    let mut ticker = TokioInstant::now();
    while ticker < deadline {
        ticker += tick_interval;
        if ticker > deadline {
            break;
        }
        sleep_until(ticker).await;
        let seq = next_seq.fetch_add(1, Ordering::Relaxed);
        let _ = work_tx.send(seq).await;
    }
    drop(work_tx);

    for task in tasks {
        task.await.map_err(|e| e.to_string())??;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StressConfig;

    #[test]
    fn expected_messages_scales_with_duration() {
        let cfg = StressConfig {
            topic_id: 1,
            partition_id: 0,
            run_id: "t".into(),
            target_rps: 100,
            duration: Duration::from_secs(10),
            warmup: Duration::ZERO,
            workers: 4,
            payload_bytes: 64,
            producer_id: 1,
            ledger_path: None,
        };
        assert_eq!(cfg.expected_messages(), 1000);
    }
}
