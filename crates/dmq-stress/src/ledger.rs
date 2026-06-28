//! Ack ledger persisted during a stress run for post-run audit.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub fn default_ledger_path(run_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!("dmq-stress-{run_id}.seq"))
}

pub struct Ledger {
    path: PathBuf,
    seen: BTreeSet<u64>,
}

impl Ledger {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        File::create(&path)?;
        Ok(Self {
            path,
            seen: BTreeSet::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn record(&mut self, seq: u64) -> io::Result<()> {
        if !self.seen.insert(seq) {
            return Ok(());
        }
        let mut file = fs::OpenOptions::new().append(true).open(&self.path)?;
        writeln!(file, "{seq}")?;
        Ok(())
    }

    pub fn into_seen(self) -> BTreeSet<u64> {
        self.seen
    }
}

pub fn load_ledger(path: &Path) -> io::Result<BTreeSet<u64>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out = BTreeSet::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let seq = line
            .trim()
            .parse::<u64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        out.insert(seq);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn ledger_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ledger.seq");
        let mut ledger = Ledger::open(path.clone()).unwrap();
        ledger.record(1).unwrap();
        ledger.record(2).unwrap();
        ledger.record(1).unwrap();
        drop(ledger);
        let loaded = load_ledger(&path).unwrap();
        assert_eq!(loaded, BTreeSet::from([1, 2]));
    }
}
