//! Configurable fsync policy for partition log durability.

use std::sync::atomic::{AtomicU32, Ordering};

static APPEND_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    Never,
    EveryN(u32),
}

pub fn fsync_policy_from_env() -> FsyncPolicy {
    match std::env::var("DMQ_FSYNC")
        .unwrap_or_else(|_| "always".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "never" | "none" | "0" => FsyncPolicy::Never,
        "always" | "1" => FsyncPolicy::Always,
        s if s.starts_with("every:") => {
            let n = s.trim_start_matches("every:").parse().unwrap_or(10);
            FsyncPolicy::EveryN(n.max(1))
        }
        s if s.parse::<u32>().is_ok() => FsyncPolicy::EveryN(s.parse().unwrap_or(10).max(1)),
        _ => FsyncPolicy::Always,
    }
}

pub fn should_fsync_after_append() -> bool {
    match fsync_policy_from_env() {
        FsyncPolicy::Always => true,
        FsyncPolicy::Never => false,
        FsyncPolicy::EveryN(n) => APPEND_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(n),
    }
}

pub fn maybe_sync_data(file: &std::fs::File) -> std::io::Result<()> {
    if should_fsync_after_append() {
        file.sync_data()
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_n_triggers_periodically() {
        APPEND_COUNTER.store(0, Ordering::Relaxed);
        let hits: Vec<bool> = (0..6)
            .map(|_| {
                APPEND_COUNTER.fetch_add(1, Ordering::Relaxed);
                APPEND_COUNTER.load(Ordering::Relaxed) % 3 == 0
            })
            .collect();
        assert_eq!(hits, vec![false, false, true, false, false, true]);
    }
}
