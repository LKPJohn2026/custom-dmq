//! Small metadata files for broker, topic, and consumer-group recovery.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

pub fn broker_metadata_path(data_dir: &Path) -> PathBuf {
    data_dir.join("broker_metadata.dat")
}

pub fn topic_metadata_path(data_dir: &Path, topic_id: u16) -> PathBuf {
    data_dir.join(format!("topic_metadata_{topic_id}.dat"))
}

pub fn cgroup_metadata_path(data_dir: &Path, topic_id: u16, group_id: u16) -> PathBuf {
    data_dir.join(format!("cgroup_metadata_{topic_id}_{group_id}.dat"))
}

pub fn offset_metadata_path(
    data_dir: &Path,
    group_id: u16,
    topic_id: u16,
    partition_id: u16,
) -> PathBuf {
    data_dir.join(format!(
        "offset_metadata_{group_id}_{topic_id}_{partition_id}.dat"
    ))
}

fn read_u32(file: &mut File, offset: u64) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64(file: &mut File, offset: u64) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

fn write_u64(file: &mut File, offset: u64, value: u64) -> io::Result<()> {
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.write_all(&value.to_be_bytes())?;
    file.sync_data()
}

fn write_u32(file: &mut File, offset: u64, value: u32) -> io::Result<()> {
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.write_all(&value.to_be_bytes())?;
    file.sync_data()
}

fn read_u16(file: &mut File, offset: u64) -> io::Result<u16> {
    let mut buf = [0u8; 2];
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

fn write_u16(file: &mut File, offset: u64, value: u16) -> io::Result<()> {
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.write_all(&value.to_be_bytes())?;
    file.sync_data()
}

pub fn load_broker_topics(data_dir: &Path) -> io::Result<Vec<u16>> {
    let path = broker_metadata_path(data_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path)?;
    let count = read_u32(&mut file, 0)?;
    let mut topics = Vec::with_capacity(count as usize);
    for i in 0..count {
        topics.push(read_u16(&mut file, 4 + u64::from(i) * 2)?);
    }
    Ok(topics)
}

pub fn store_broker_topics(data_dir: &Path, topic_ids: &[u16]) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path = broker_metadata_path(data_dir);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    write_u32(&mut file, 0, topic_ids.len() as u32)?;
    for (i, topic_id) in topic_ids.iter().enumerate() {
        write_u16(&mut file, 4 + u64::from(i as u32) * 2, *topic_id)?;
    }
    Ok(())
}

pub fn load_topic_groups(data_dir: &Path, topic_id: u16) -> io::Result<Vec<u16>> {
    let path = topic_metadata_path(data_dir, topic_id);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path)?;
    let count = read_u32(&mut file, 0)?;
    let mut groups = Vec::with_capacity(count as usize);
    for i in 0..count {
        groups.push(read_u16(&mut file, 4 + u64::from(i) * 2)?);
    }
    Ok(groups)
}

pub fn store_topic_groups(data_dir: &Path, topic_id: u16, group_ids: &[u16]) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path = topic_metadata_path(data_dir, topic_id);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    write_u32(&mut file, 0, group_ids.len() as u32)?;
    for (i, group_id) in group_ids.iter().enumerate() {
        write_u16(&mut file, 4 + u64::from(i as u32) * 2, *group_id)?;
    }
    Ok(())
}

pub fn load_cgroup_partition_count(
    data_dir: &Path,
    topic_id: u16,
    group_id: u16,
) -> io::Result<u32> {
    let path = cgroup_metadata_path(data_dir, topic_id, group_id);
    if !path.exists() {
        return Ok(0);
    }
    let mut file = File::open(path)?;
    read_u32(&mut file, 0)
}

pub fn store_cgroup_partition_count(
    data_dir: &Path,
    topic_id: u16,
    group_id: u16,
    count: u32,
) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path = cgroup_metadata_path(data_dir, topic_id, group_id);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    write_u32(&mut file, 0, count)
}

pub fn load_committed_offset(
    data_dir: &Path,
    group_id: u16,
    topic_id: u16,
    partition_id: u16,
) -> io::Result<Option<u64>> {
    let path = offset_metadata_path(data_dir, group_id, topic_id, partition_id);
    if !path.exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    Ok(Some(read_u64(&mut file, 0)?))
}

pub fn store_committed_offset(
    data_dir: &Path,
    group_id: u16,
    topic_id: u16,
    partition_id: u16,
    offset: u64,
) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path = offset_metadata_path(data_dir, group_id, topic_id, partition_id);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    write_u64(&mut file, 0, offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn broker_topic_list_roundtrip() {
        let dir = tempdir().unwrap();
        store_broker_topics(dir.path(), &[1, 42]).unwrap();
        assert_eq!(load_broker_topics(dir.path()).unwrap(), vec![1, 42]);
    }

    #[test]
    fn topic_group_list_roundtrip() {
        let dir = tempdir().unwrap();
        store_topic_groups(dir.path(), 7, &[3, 9]).unwrap();
        assert_eq!(load_topic_groups(dir.path(), 7).unwrap(), vec![3, 9]);
    }

    #[test]
    fn cgroup_partition_count_roundtrip() {
        let dir = tempdir().unwrap();
        store_cgroup_partition_count(dir.path(), 1, 2, 3).unwrap();
        assert_eq!(load_cgroup_partition_count(dir.path(), 1, 2).unwrap(), 3);
    }

    #[test]
    fn committed_offset_roundtrip() {
        let dir = tempdir().unwrap();
        store_committed_offset(dir.path(), 7, 1, 0, 123).unwrap();
        assert_eq!(
            load_committed_offset(dir.path(), 7, 1, 0).unwrap(),
            Some(123)
        );
    }
}
