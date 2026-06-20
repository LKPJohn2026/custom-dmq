//! Memory-mapped ring queues backed by on-disk arrays and metadata.

use crate::constants::{MAX_MSG_SIZE, QUEUE_CAPACITY};
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

const BUFFER_BYTES: usize = MAX_MSG_SIZE * QUEUE_CAPACITY;

pub const STAGING_GROUP_ID: u16 = 65535;
pub const STAGING_PARTITION_ID: u16 = 65535;

pub fn partition_metadata_path(
    data_dir: &Path,
    topic_id: u16,
    group_id: u16,
    partition_id: u16,
) -> PathBuf {
    data_dir.join(format!(
        "partition_metadata_{topic_id}_{group_id}_{partition_id}.dat"
    ))
}

pub fn under_arr_path(data_dir: &Path, topic_id: u16, group_id: u16, partition_id: u16) -> PathBuf {
    data_dir.join(format!("underArr_{topic_id}_{group_id}_{partition_id}.dat"))
}

pub fn under_size_path(
    data_dir: &Path,
    topic_id: u16,
    group_id: u16,
    partition_id: u16,
) -> PathBuf {
    data_dir.join(format!(
        "underSize_{topic_id}_{group_id}_{partition_id}.dat"
    ))
}

pub struct MmapQueue {
    head: u32,
    tail: u32,
    buffer: MmapMut,
    sizes: MmapMut,
    meta_file: File,
}

impl MmapQueue {
    pub fn open(
        data_dir: &Path,
        topic_id: u16,
        group_id: u16,
        partition_id: u16,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;

        let meta_path = partition_metadata_path(data_dir, topic_id, group_id, partition_id);
        let arr_path = under_arr_path(data_dir, topic_id, group_id, partition_id);
        let size_path = under_size_path(data_dir, topic_id, group_id, partition_id);

        let mut meta_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&meta_path)?;
        if meta_file.metadata()?.len() < 8 {
            meta_file.set_len(8)?;
            write_u32(&mut meta_file, 0, 0)?;
            write_u32(&mut meta_file, 4, 0)?;
        }

        let head = read_u32(&mut meta_file, 0)?;
        let tail = read_u32(&mut meta_file, 4)?;

        let buffer = open_or_create_mmap(&arr_path, BUFFER_BYTES)?;
        let sizes = open_or_create_mmap(&size_path, BUFFER_BYTES)?;

        Ok(MmapQueue {
            head,
            tail,
            buffer,
            sizes,
            meta_file,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> u64 {
        let offset = self.live_len();
        let len = payload.len().min(MAX_MSG_SIZE);

        let start = self.tail as usize;
        self.buffer[start..start + len].copy_from_slice(&payload[..len]);
        self.sizes[start] = len as u8;

        self.tail = self.tail.wrapping_add(MAX_MSG_SIZE as u32) % BUFFER_BYTES as u32;
        write_u32(&mut self.meta_file, 4, self.tail).expect("persist queue tail");

        offset
    }

    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        if self.head == self.tail {
            return None;
        }

        let start = self.head as usize;
        let len = self.sizes[start] as usize;
        let bytes = self.buffer[start..start + len].to_vec();

        self.head = self.head.wrapping_add(MAX_MSG_SIZE as u32) % BUFFER_BYTES as u32;
        write_u32(&mut self.meta_file, 0, self.head).expect("persist queue head");

        Some(bytes)
    }

    pub fn live_len(&self) -> u64 {
        if self.tail >= self.head {
            u64::from((self.tail - self.head) / MAX_MSG_SIZE as u32)
        } else {
            u64::from((BUFFER_BYTES as u32 - self.head + self.tail) / MAX_MSG_SIZE as u32)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    pub fn head(&self) -> u32 {
        self.head
    }

    pub fn tail(&self) -> u32 {
        self.tail
    }
}

fn open_or_create_mmap(path: &Path, len: usize) -> io::Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    if file.metadata()?.len() < len as u64 {
        file.set_len(len as u64)?;
    }
    unsafe { MmapOptions::new().len(len).map_mut(&file) }
}

fn read_u32(file: &mut File, offset: u64) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn write_u32(file: &mut File, offset: u64, value: u32) -> io::Result<()> {
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.write_all(&value.to_be_bytes())?;
    file.sync_data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_pop_persist_head_tail() {
        let dir = tempdir().unwrap();
        let mut queue = MmapQueue::open(dir.path(), 1, 2, 1).unwrap();
        queue.append(b"alpha");
        queue.append(b"beta");
        assert_eq!(queue.pop_front().unwrap(), b"alpha");
        assert_eq!(queue.pop_front().unwrap(), b"beta");
        assert!(queue.pop_front().is_none());

        let restored = MmapQueue::open(dir.path(), 1, 2, 1).unwrap();
        assert!(restored.is_empty());
        assert_eq!(restored.head(), queue.head());
        assert_eq!(restored.tail(), queue.tail());
    }

    #[test]
    fn survives_reopen_with_pending_messages() {
        let dir = tempdir().unwrap();
        {
            let mut queue = MmapQueue::open(dir.path(), 9, 1, 1).unwrap();
            queue.append(b"one");
            queue.append(b"two");
        }
        let mut queue = MmapQueue::open(dir.path(), 9, 1, 1).unwrap();
        assert_eq!(queue.live_len(), 2);
        assert_eq!(queue.pop_front().unwrap(), b"one");
        assert_eq!(queue.pop_front().unwrap(), b"two");
    }

    #[test]
    fn staging_queue_uses_reserved_ids() {
        let dir = tempdir().unwrap();
        let mut queue =
            MmapQueue::open(dir.path(), 3, STAGING_GROUP_ID, STAGING_PARTITION_ID).unwrap();
        queue.append(b"staged");
        let restored =
            MmapQueue::open(dir.path(), 3, STAGING_GROUP_ID, STAGING_PARTITION_ID).unwrap();
        assert_eq!(restored.live_len(), 1);
    }
}
