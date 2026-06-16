//! Broker state, partition routing, and consumer delivery.
//!
//! Producers and consumers register over binary frames; the broker dials back
//! on the client port. Messages land in a topic staging queue until a consumer
//! group exists, then route into per-group partitions. Consumers signal
//! readiness with R_PCM and receive the next message from their assigned partition.
//! Queue data and metadata are persisted under a configurable data directory.

use crate::message::{self, ConsumerRegister, Message, ProducerRegister};
use crate::metadata::store_broker_topics;
use crate::storage::{GroupId, PartitionIdx, Storage, TopicId};
use crate::topic::Topic;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub const BROKER_PORT: u16 = 7777;
pub const DEFAULT_DATA_DIR: &str = "dmq-data";

pub fn broker_addr() -> String {
    let port = std::env::var("DMQ_BROKER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(BROKER_PORT as u32) as u16;
    format!("127.0.0.1:{port}")
}

pub fn broker_port() -> u16 {
    std::env::var("DMQ_BROKER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(BROKER_PORT)
}

pub fn data_dir_from_env() -> PathBuf {
    std::env::var("DMQ_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_DATA_DIR))
}

pub struct Broker {
    topics: HashMap<u16, Topic>,
    data_dir: PathBuf,
    _temp_dir: Option<tempfile::TempDir>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    pub fn open(data_dir: impl AsRef<Path>) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        let topic_ids = crate::metadata::load_broker_topics(&data_dir)?;
        let mut topics = HashMap::new();
        for topic_id in topic_ids {
            topics.insert(topic_id, Topic::load(&data_dir, topic_id)?);
        }
        Ok(Broker {
            topics,
            data_dir,
            _temp_dir: None,
        })
    }

    pub fn open_ephemeral() -> io::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let data_dir = temp_dir.path().to_path_buf();
        let mut broker = Self::open(&data_dir)?;
        broker._temp_dir = Some(temp_dir);
        Ok(broker)
    }

    pub fn new() -> Self {
        Self::open_ephemeral().expect("ephemeral broker data dir")
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    fn persist_topics(&self) -> io::Result<()> {
        let mut topic_ids: Vec<u16> = self.topics.keys().copied().collect();
        topic_ids.sort_unstable();
        store_broker_topics(&self.data_dir, &topic_ids)
    }

    fn topic_mut(&mut self, topic_id: u16) -> io::Result<&mut Topic> {
        if !self.topics.contains_key(&topic_id) {
            let topic = Topic::create(&self.data_dir, topic_id)?;
            self.topics.insert(topic_id, topic);
            self.persist_topics()?;
        }
        Ok(self.topics.get_mut(&topic_id).expect("topic inserted"))
    }

    pub fn process_echo(&self, text: &str) -> String {
        format!("I have receiver: {text}")
    }

    pub fn register_producer(&mut self, reg: &ProducerRegister) -> io::Result<u8> {
        self.topic_mut(reg.topic_id)?;
        Ok(0)
    }

    pub fn register_consumer(&mut self, reg: &ConsumerRegister) -> io::Result<u16> {
        let topic_id = reg.topic_id;
        let group_id = reg.group_id;
        let data_dir = self.data_dir.clone();
        let topic = self.topic_mut(topic_id)?;
        topic.find_or_create_group(group_id)?;
        let partition_idx = topic
            .group_mut(group_id)
            .expect("group exists")
            .assign_partition(&data_dir, topic_id)?;
        Ok(partition_idx)
    }

    pub fn produce_pcm(&mut self, topic_id: u16, payload: &[u8]) -> io::Result<(u8, u64)> {
        let topic = self.topic_mut(topic_id)?;

        if topic.cgroups.is_empty() {
            let offset = topic.append_to_staging(payload);
            return Ok((0, offset));
        }

        let payload = payload.to_vec();
        let group_count = topic.cgroups.len();
        for i in 0..group_count {
            let partition_idx = topic.cgroups[i].smallest_partition_index();
            while let Some(msg) = topic.staging.pop_front() {
                topic.cgroups[i].partitions[partition_idx].append(&msg);
            }
            topic.cgroups[i].partitions[partition_idx].append(&payload);
        }

        Ok((0, 0))
    }

    pub fn topic_ids_with_groups(&self) -> Vec<(u16, Vec<u16>)> {
        self.topics
            .iter()
            .map(|(tid, topic)| (*tid, topic.cgroups.iter().map(|g| g.group_id).collect()))
            .collect()
    }

    pub fn has_topic(&self, topic_id: u16) -> bool {
        self.topics.contains_key(&topic_id)
    }

    pub fn topic_group_count(&self, topic_id: u16) -> Option<usize> {
        self.topics.get(&topic_id).map(|t| t.cgroups.len())
    }

    pub fn topic_group_partition_count(&self, topic_id: u16, group_id: u16) -> Option<usize> {
        self.topics
            .get(&topic_id)
            .and_then(|t| t.group(group_id))
            .map(|g| g.partitions.len())
    }

    pub fn topic_staging_len(&self, topic_id: u16) -> Option<u64> {
        self.topics.get(&topic_id).map(|t| t.staging.live_len())
    }

    /// Pop the next message from a group partition — used by unit tests.
    pub fn consume_from_partition(
        &mut self,
        topic_id: u16,
        group_id: u16,
        partition_idx: u16,
        payload_out: &mut Option<Vec<u8>>,
    ) -> io::Result<bool> {
        let topic = match self.topics.get_mut(&topic_id) {
            Some(t) => t,
            None => return Ok(false),
        };
        topic.find_or_create_group(group_id)?;
        let partition = match topic
            .group_mut(group_id)
            .and_then(|g| g.partitions.get_mut(partition_idx as usize))
        {
            Some(p) => p,
            None => return Ok(false),
        };
        match partition.pop_front() {
            None => {
                *payload_out = None;
                Ok(false)
            }
            Some(bytes) => {
                *payload_out = Some(bytes);
                Ok(true)
            }
        }
    }
}

impl Storage for Broker {
    fn ensure_topic(&mut self, topic_id: TopicId) -> io::Result<()> {
        self.topic_mut(topic_id)?;
        Ok(())
    }

    fn ensure_group(&mut self, topic_id: TopicId, group_id: GroupId) -> io::Result<()> {
        let topic = self.topic_mut(topic_id)?;
        topic.find_or_create_group(group_id)?;
        Ok(())
    }

    fn assign_partition(
        &mut self,
        topic_id: TopicId,
        group_id: GroupId,
    ) -> io::Result<PartitionIdx> {
        let data_dir = self.data_dir.clone();
        let topic = self.topic_mut(topic_id)?;
        topic.find_or_create_group(group_id)?;
        let idx = topic
            .group_mut(group_id)
            .expect("group exists")
            .assign_partition(&data_dir, topic_id)?;
        Ok(idx)
    }

    fn produce(&mut self, topic_id: TopicId, payload: &[u8]) -> io::Result<(u8, u64)> {
        self.produce_pcm(topic_id, payload)
    }

    fn consume_one(
        &mut self,
        topic_id: TopicId,
        group_id: GroupId,
        partition_idx: PartitionIdx,
    ) -> io::Result<Option<Vec<u8>>> {
        let topic = match self.topics.get_mut(&topic_id) {
            Some(t) => t,
            None => return Ok(None),
        };
        topic.find_or_create_group(group_id)?;
        let partition = match topic
            .group_mut(group_id)
            .and_then(|g| g.partitions.get_mut(partition_idx as usize))
        {
            Some(p) => p,
            None => return Ok(None),
        };
        Ok(partition.pop_front())
    }
}

/// Per-consumer task: wait for R_PCM, pop from assigned partition, send PCM.
pub async fn run_consumer_ready_and_send<R, W>(
    broker: Arc<Mutex<Broker>>,
    topic_id: u16,
    group_id: u16,
    partition_idx: u16,
    reader: &mut BufReader<R>,
    writer: &mut W,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        match message::read_message(reader).await {
            Ok(Message::RPcm(_)) => {}
            Ok(other) => {
                eprintln!("[broker→consumer] Expected R_PCM, got {other:?}");
                break;
            }
            Err(e) => {
                eprintln!("[broker→consumer] Read error: {e}");
                break;
            }
        }

        let payload = loop {
            let data = {
                let mut guard = broker.lock().await;
                match guard.topics.get_mut(&topic_id) {
                    Some(topic) => topic
                        .group_mut(group_id)
                        .and_then(|g| g.partitions.get_mut(partition_idx as usize))
                        .and_then(|p| p.pop_front()),
                    None => None,
                }
            };
            if let Some(p) = data {
                break p;
            }
            sleep(Duration::from_millis(10)).await;
        };

        if message::write_message(writer, &Message::Pcm(payload))
            .await
            .is_err()
        {
            break;
        }

        println!(
            "[broker] Delivered message from topic {topic_id} group {group_id} partition {partition_idx}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{ConsumerRegister, ProducerRegister};

    fn setup() -> Broker {
        let mut broker = Broker::new();
        broker
            .register_producer(&ProducerRegister {
                port: 7778,
                topic_id: 1,
            })
            .unwrap();
        broker
    }

    fn setup_with_group(group_id: u16) -> Broker {
        let mut broker = setup();
        broker
            .register_consumer(&ConsumerRegister {
                port: 7779,
                topic_id: 1,
                group_id,
            })
            .unwrap();
        broker
    }

    #[test]
    fn test_register_producer_auto_creates_topic() {
        let mut broker = Broker::new();
        let code = broker
            .register_producer(&ProducerRegister {
                port: 7778,
                topic_id: 1,
            })
            .unwrap();
        assert_eq!(code, 0);
        assert!(broker.topics.contains_key(&1));
    }

    #[test]
    fn test_produce_to_staging_without_groups() {
        let mut broker = setup();
        let (_, o1) = broker.produce_pcm(1, b"msg-a").unwrap();
        let (_, o2) = broker.produce_pcm(1, b"msg-b").unwrap();
        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
    }

    #[test]
    fn test_consume_returns_messages_in_order() {
        let mut broker = setup_with_group(10);
        broker.produce_pcm(1, b"first").unwrap();
        broker.produce_pcm(1, b"second").unwrap();

        let mut payload = None;
        assert!(broker
            .consume_from_partition(1, 10, 0, &mut payload)
            .unwrap());
        assert_eq!(payload.as_deref(), Some(b"first" as &[u8]));

        payload = None;
        assert!(broker
            .consume_from_partition(1, 10, 0, &mut payload)
            .unwrap());
        assert_eq!(payload.as_deref(), Some(b"second" as &[u8]));
    }

    #[test]
    fn test_two_groups_independent() {
        let mut broker = setup_with_group(1);
        broker
            .register_consumer(&ConsumerRegister {
                port: 7780,
                topic_id: 1,
                group_id: 2,
            })
            .unwrap();
        broker.produce_pcm(1, b"msg-a").unwrap();
        broker.produce_pcm(1, b"msg-b").unwrap();

        let mut a = None;
        let mut b = None;
        assert!(broker.consume_from_partition(1, 1, 0, &mut a).unwrap());
        assert!(broker.consume_from_partition(1, 2, 0, &mut b).unwrap());
        assert_eq!(a.as_deref(), Some(b"msg-a" as &[u8]));
        assert_eq!(b.as_deref(), Some(b"msg-a" as &[u8]));

        a = None;
        assert!(broker.consume_from_partition(1, 1, 0, &mut a).unwrap());
        assert_eq!(a.as_deref(), Some(b"msg-b" as &[u8]));
    }

    #[test]
    fn test_staging_drains_when_group_registers_after_produce() {
        let mut broker = setup();
        broker.produce_pcm(1, b"early").unwrap();
        broker
            .register_consumer(&ConsumerRegister {
                port: 7779,
                topic_id: 1,
                group_id: 1,
            })
            .unwrap();
        broker.produce_pcm(1, b"late").unwrap();

        let mut payload = None;
        assert!(broker
            .consume_from_partition(1, 1, 0, &mut payload)
            .unwrap());
        assert_eq!(payload.as_deref(), Some(b"early" as &[u8]));
        payload = None;
        assert!(broker
            .consume_from_partition(1, 1, 0, &mut payload)
            .unwrap());
        assert_eq!(payload.as_deref(), Some(b"late" as &[u8]));
    }

    #[test]
    fn broker_reopen_restores_staged_messages() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut broker = Broker::open(dir.path()).unwrap();
            broker
                .register_producer(&ProducerRegister {
                    port: 7778,
                    topic_id: 5,
                })
                .unwrap();
            broker.produce_pcm(5, b"survives").unwrap();
        }
        let broker = Broker::open(dir.path()).unwrap();
        assert_eq!(broker.topic_staging_len(5), Some(1));
    }
}
