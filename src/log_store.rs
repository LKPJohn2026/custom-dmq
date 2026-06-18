//! Append-only on-disk storage for partition logs.

use crate::partition_log::{PartitionLog, Record};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

pub fn log_data_path(data_dir: &Path, topic_id: u16, partition_id: u16) -> PathBuf {
    data_dir.join(format!("log_data_{topic_id}_{partition_id}.dat"))
}

pub fn log_meta_path(data_dir: &Path, topic_id: u16, partition_id: u16) -> PathBuf {
    data_dir.join(format!("log_meta_{topic_id}_{partition_id}.dat"))
}

pub fn append_record(
    data_dir: &Path,
    topic_id: u16,
    partition_id: u16,
    record: &Record,
) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path = log_data_path(data_dir, topic_id, partition_id);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let len = u16::try_from(record.payload.len()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "payload too large for log segment")
    })?;
    file.write_all(&record.offset.to_be_bytes())?;
    file.write_all(&len.to_be_bytes())?;
    file.write_all(&record.payload)?;
    file.sync_data()?;
    store_meta_offsets(data_dir, topic_id, partition_id, None, Some(record.offset + 1))
}

pub fn store_meta_offsets(
    data_dir: &Path,
    topic_id: u16,
    partition_id: u16,
    base_offset: Option<u64>,
    next_offset: Option<u64>,
) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path = log_meta_path(data_dir, topic_id, partition_id);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    if file.metadata()?.len() < 16 {
        file.set_len(16)?;
        write_u64(&mut file, 0, 0)?;
        write_u64(&mut file, 8, 0)?;
    }
    if let Some(base) = base_offset {
        write_u64(&mut file, 0, base)?;
    }
    if let Some(next) = next_offset {
        write_u64(&mut file, 8, next)?;
    }
    Ok(())
}

pub fn load_partition_log(
    data_dir: &Path,
    topic_id: u16,
    partition_id: u16,
    max_records: Option<usize>,
) -> io::Result<PartitionLog> {
    let data_path = log_data_path(data_dir, topic_id, partition_id);
    let mut log = match max_records {
        Some(n) => PartitionLog::with_max_records(n),
        None => PartitionLog::new(),
    };
    if !data_path.exists() {
        return Ok(log);
    }

    let mut file = File::open(data_path)?;
    loop {
        let mut offset_buf = [0u8; 8];
        if file.read_exact(&mut offset_buf).is_err() {
            break;
        }
        let mut len_buf = [0u8; 2];
        file.read_exact(&mut len_buf)?;
        let len = u16::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        file.read_exact(&mut payload)?;
        let offset = u64::from_be_bytes(offset_buf);
        // Rebuild by appending; offsets in file should match sequential append.
        let appended = log.append(&payload);
        debug_assert_eq!(appended, offset);
    }
    Ok(log)
}

fn write_u64(file: &mut File, offset: u64, value: u64) -> io::Result<()> {
    file.seek(io::SeekFrom::Start(offset))?;
    file.write_all(&value.to_be_bytes())?;
    file.sync_data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_reload_roundtrip() {
        let dir = tempdir().unwrap();
        let record = Record {
            offset: 0,
            payload: b"hello".to_vec(),
        };
        append_record(dir.path(), 1, 0, &record).unwrap();
        let log = load_partition_log(dir.path(), 1, 0, None).unwrap();
        assert_eq!(log.next_offset(), 1);
        assert_eq!(log.fetch(0, 1024).records[0].payload, b"hello");
    }
}
