//! Post-run audit: compare ack ledger against fetched log records.

use crate::client::{self, parse_payload};
use crate::config::VerifyConfig;
use crate::ledger::{default_ledger_path, load_ledger};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub struct AuditReport {
    pub run_id: String,
    pub expected: usize,
    pub fetched_for_run: usize,
    pub missing: Vec<u64>,
    pub unexpected: Vec<u64>,
    pub committed_offset: u64,
}

impl AuditReport {
    pub fn ok(&self) -> bool {
        self.missing.is_empty()
            && self.unexpected.is_empty()
            && self.expected == self.fetched_for_run
    }
}

pub async fn audit_topic(config: VerifyConfig) -> Result<AuditReport, String> {
    let ledger_path = config
        .ledger_path
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| default_ledger_path(&config.run_id));

    let expected_set = load_ledger(&ledger_path).map_err(|e| format!("load ledger: {e}"))?;
    let (records, committed_offset) = client::fetch_and_commit(
        config.topic_id,
        config.partition_id,
        config.group_id,
        256 * 1024,
    )
    .await
    .map_err(|e| format!("fetch and commit: {e}"))?;

    let mut fetched_set = BTreeSet::new();
    for rec in records {
        let (run_id, seq) = parse_payload(&rec.payload).map_err(|e| e.to_string())?;
        if run_id == config.run_id {
            fetched_set.insert(seq);
        }
    }

    let missing: Vec<u64> = expected_set
        .iter()
        .filter(|seq| !fetched_set.contains(seq))
        .copied()
        .collect();
    let unexpected: Vec<u64> = fetched_set
        .iter()
        .filter(|seq| !expected_set.contains(seq))
        .copied()
        .collect();

    let report = AuditReport {
        run_id: config.run_id.clone(),
        expected: expected_set.len(),
        fetched_for_run: fetched_set.len(),
        missing,
        unexpected,
        committed_offset,
    };
    print_audit_report(&report, &ledger_path);
    Ok(report)
}

pub fn print_audit_report(report: &AuditReport, ledger_path: &Path) {
    println!("---- dmq audit report ----");
    println!("run_id         : {}", report.run_id);
    println!("ledger         : {}", ledger_path.display());
    println!(
        "producer audit : {}/{} acked sequences present in log",
        report.fetched_for_run, report.expected
    );
    println!("committed offset : {}", report.committed_offset);
    if report.missing.is_empty() && report.unexpected.is_empty() {
        println!("result         : PASS (no gaps, no extras)");
    } else {
        println!("result         : FAIL");
        if !report.missing.is_empty() {
            let sample: Vec<_> = report.missing.iter().take(10).copied().collect();
            println!("missing (first 10): {sample:?}");
        }
        if !report.unexpected.is_empty() {
            let sample: Vec<_> = report.unexpected.iter().take(10).copied().collect();
            println!("unexpected (first 10): {sample:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_ok_when_sets_match() {
        let report = AuditReport {
            run_id: "r".into(),
            expected: 3,
            fetched_for_run: 3,
            missing: vec![],
            unexpected: vec![],
            committed_offset: 3,
        };
        assert!(report.ok());
    }
}
