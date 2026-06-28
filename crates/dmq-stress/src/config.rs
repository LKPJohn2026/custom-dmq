use std::time::Duration;

#[derive(Debug, Clone)]
pub struct StressConfig {
    pub topic_id: u16,
    pub partition_id: u16,
    pub run_id: String,
    pub target_rps: u64,
    pub duration: Duration,
    pub warmup: Duration,
    pub workers: usize,
    pub payload_bytes: usize,
    pub producer_id: u64,
    pub ledger_path: Option<String>,
}

impl StressConfig {
    pub fn expected_messages(&self) -> u64 {
        self.target_rps.saturating_mul(self.duration.as_secs())
    }
}

#[derive(Debug, Clone)]
pub struct VerifyConfig {
    pub topic_id: u16,
    pub partition_id: u16,
    pub run_id: String,
    pub ledger_path: Option<String>,
    pub group_id: u16,
}

pub fn parse_duration(text: &str) -> Option<Duration> {
    if let Some(secs) = text.strip_suffix('s') {
        secs.parse::<u64>().ok().map(Duration::from_secs)
    } else {
        text.parse::<u64>().ok().map(Duration::from_secs)
    }
}

pub fn parse_stress_args(args: &[String]) -> Result<StressConfig, String> {
    let mut topic_id = 1u16;
    let mut partition_id = 0u16;
    let mut run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".into());
    let mut target_rps = 1000u64;
    let mut duration = Duration::from_secs(30);
    let mut warmup = Duration::from_secs(5);
    let mut workers = 8usize;
    let mut payload_bytes = 64usize;
    let mut producer_id = 1u64;
    let mut ledger_path = None;

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--topic" => {
                idx += 1;
                topic_id = next_u16(args, idx, "topic")?;
            }
            "--partition" => {
                idx += 1;
                partition_id = next_u16(args, idx, "partition")?;
            }
            "--run-id" => {
                idx += 1;
                run_id = next_string(args, idx, "run-id")?;
            }
            "--rps" => {
                idx += 1;
                target_rps = next_u64(args, idx, "rps")?;
            }
            "--duration" => {
                idx += 1;
                let text = next_string(args, idx, "duration")?;
                duration =
                    parse_duration(&text).ok_or_else(|| format!("invalid duration: {text}"))?;
            }
            "--warmup" => {
                idx += 1;
                let text = next_string(args, idx, "warmup")?;
                warmup = parse_duration(&text).ok_or_else(|| format!("invalid warmup: {text}"))?;
            }
            "--workers" => {
                idx += 1;
                workers = next_usize(args, idx, "workers")?;
            }
            "--payload-bytes" => {
                idx += 1;
                payload_bytes = next_usize(args, idx, "payload-bytes")?;
            }
            "--producer-id" => {
                idx += 1;
                producer_id = next_u64(args, idx, "producer-id")?;
            }
            "--ledger" => {
                idx += 1;
                ledger_path = Some(next_string(args, idx, "ledger")?);
            }
            flag => return Err(format!("unknown flag: {flag}")),
        }
        idx += 1;
    }

    if workers == 0 {
        return Err("workers must be >= 1".into());
    }
    if payload_bytes < 16 {
        return Err("payload-bytes must be >= 16".into());
    }
    if payload_bytes > 240 {
        return Err("payload-bytes must be <= 240 for v1 wire framing".into());
    }

    Ok(StressConfig {
        topic_id,
        partition_id,
        run_id,
        target_rps,
        duration,
        warmup,
        workers,
        payload_bytes,
        producer_id,
        ledger_path,
    })
}

pub fn parse_verify_args(args: &[String]) -> Result<VerifyConfig, String> {
    let mut topic_id = 1u16;
    let mut partition_id = 0u16;
    let mut run_id = String::new();
    let mut ledger_path = None;
    let mut group_id = 1u16;

    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--topic" => {
                idx += 1;
                topic_id = next_u16(args, idx, "topic")?;
            }
            "--partition" => {
                idx += 1;
                partition_id = next_u16(args, idx, "partition")?;
            }
            "--run-id" => {
                idx += 1;
                run_id = next_string(args, idx, "run-id")?;
            }
            "--ledger" => {
                idx += 1;
                ledger_path = Some(next_string(args, idx, "ledger")?);
            }
            "--group" => {
                idx += 1;
                group_id = next_u16(args, idx, "group")?;
            }
            flag => return Err(format!("unknown flag: {flag}")),
        }
        idx += 1;
    }

    if run_id.is_empty() {
        return Err("--run-id is required for verify".into());
    }

    Ok(VerifyConfig {
        topic_id,
        partition_id,
        run_id,
        ledger_path,
        group_id,
    })
}

fn next_string(args: &[String], idx: usize, name: &str) -> Result<String, String> {
    args.get(idx)
        .cloned()
        .ok_or_else(|| format!("missing value for --{name}"))
}

fn next_u16(args: &[String], idx: usize, name: &str) -> Result<u16, String> {
    next_string(args, idx, name)?
        .parse()
        .map_err(|_| format!("invalid --{name}"))
}

fn next_u64(args: &[String], idx: usize, name: &str) -> Result<u64, String> {
    next_string(args, idx, name)?
        .parse()
        .map_err(|_| format!("invalid --{name}"))
}

fn next_usize(args: &[String], idx: usize, name: &str) -> Result<usize, String> {
    next_string(args, idx, name)?
        .parse()
        .map_err(|_| format!("invalid --{name}"))
}
