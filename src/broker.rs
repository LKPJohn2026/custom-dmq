//! Broker state, partition routing, and consumer delivery.
//!
//! Producers and consumers register over binary frames; the broker dials back
//! on the client port. Messages land in a topic staging queue until a consumer
//! group exists, then route into per-group partitions. Consumers signal
//! readiness with R_PCM and receive the next message from their assigned partition.
//! Queue data and metadata are persisted under a configurable data directory.

use crate::acl::{Acl, Operation};
use crate::cluster::{BrokerId, ClusterConfig};
use crate::cluster_state::{self, ClusterState};
use crate::coordinator::{self, GroupState};
use crate::idempotency::{IdempotencyAction, IdempotencyState};
use crate::limits;
use crate::log_store;
use crate::message::{
    self, BrokerHeartbeatRequest, CommitOffsetRequest, ConsumerRegister, FetchRequest,
    GroupHeartbeatRequest, IdempotentProduceRequest, JoinGroupRequest, Message, ProducerRegister,
};
use crate::metadata::{
    load_committed_offset, load_topic_config, store_broker_topics, store_committed_offset,
    store_topic_config,
};
use crate::metrics::BrokerMetrics;
use crate::partition_log::{PartitionLog, Record};
use crate::storage::{GroupId, PartitionIdx, Storage, TopicId};
use crate::topic::Topic;
use crate::topic_config::TopicConfig;
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
    broker_addr_for(ClusterConfig::local_broker_id())
}

pub fn broker_addr_for(broker_id: BrokerId) -> String {
    if let Ok(Some(cfg)) = ClusterConfig::from_env() {
        if let Some(addr) = cfg.broker_addr(broker_id) {
            return addr;
        }
    }
    let port = std::env::var("DMQ_BROKER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(BROKER_PORT as u32) as u16;
    format!("127.0.0.1:{port}")
}

pub fn broker_id() -> BrokerId {
    ClusterConfig::local_broker_id()
}

pub fn cluster_config_from_env() -> io::Result<Option<ClusterConfig>> {
    ClusterConfig::from_env()
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
    logs: HashMap<(u16, u16), PartitionLog>,
    topic_configs: HashMap<u16, TopicConfig>,
    committed_offsets: HashMap<(u16, u16, u16), u64>,
    data_dir: PathBuf,
    metrics: Arc<BrokerMetrics>,
    broker_id: BrokerId,
    cluster: Option<ClusterConfig>,
    cluster_state: Option<ClusterState>,
    groups: HashMap<(u16, u16), GroupState>,
    idempotency: IdempotencyState,
    acl: Acl,
    _temp_dir: Option<tempfile::TempDir>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    pub fn open(data_dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_cluster(data_dir, ClusterConfig::from_env()?)
    }

    pub fn open_with_cluster(
        data_dir: impl AsRef<Path>,
        cluster: Option<ClusterConfig>,
    ) -> io::Result<Self> {
        let broker_id = ClusterConfig::local_broker_id();
        Self::open_with_cluster_and_id(data_dir, cluster, broker_id)
    }

    pub fn open_with_cluster_and_id(
        data_dir: impl AsRef<Path>,
        cluster: Option<ClusterConfig>,
        broker_id: BrokerId,
    ) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        let topic_ids = crate::metadata::load_broker_topics(&data_dir)?;
        let mut topics = HashMap::new();
        let mut topic_configs = HashMap::new();
        for &topic_id in &topic_ids {
            topics.insert(topic_id, Topic::load(&data_dir, topic_id)?);
            if let Some((partition_count, max_records)) = load_topic_config(&data_dir, topic_id)? {
                topic_configs.insert(
                    topic_id,
                    TopicConfig::new(topic_id, partition_count, max_records),
                );
            }
        }
        let (cluster, cluster_state) = match cluster {
            Some(seed) => {
                let state = ClusterState::open_or_bootstrap(&data_dir, &seed)?;
                let live = state.to_cluster_config();
                (Some(live), Some(state))
            }
            None => (None, None),
        };
        let mut broker = Broker {
            topics,
            logs: HashMap::new(),
            topic_configs,
            committed_offsets: HashMap::new(),
            data_dir: data_dir.clone(),
            metrics: Arc::new(BrokerMetrics::new()),
            broker_id,
            cluster,
            cluster_state,
            groups: HashMap::new(),
            idempotency: IdempotencyState::load(&data_dir)?,
            acl: Acl::from_env(),
            _temp_dir: None,
        };
        broker.load_logs()?;
        Ok(broker)
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

    pub fn metrics(&self) -> Arc<BrokerMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    pub fn cluster(&self) -> Option<&ClusterConfig> {
        self.cluster.as_ref()
    }

    pub fn cluster_state(&self) -> Option<&ClusterState> {
        self.cluster_state.as_ref()
    }

    pub fn cluster_state_mut(&mut self) -> Option<&mut ClusterState> {
        self.cluster_state.as_mut()
    }

    pub fn is_controller(&self) -> bool {
        self.cluster_state
            .as_ref()
            .map(|s| s.is_controller(self.broker_id))
            .unwrap_or(false)
    }

    fn sync_cluster_view(&mut self) {
        if let Some(state) = &self.cluster_state {
            self.cluster = Some(state.to_cluster_config());
        }
    }

    pub fn apply_cluster_state(&mut self, state: ClusterState) -> io::Result<()> {
        state.store(&self.data_dir)?;
        self.cluster_state = Some(state);
        self.sync_cluster_view();
        Ok(())
    }

    pub fn handle_broker_heartbeat(&mut self, req: &BrokerHeartbeatRequest) -> io::Result<u8> {
        let Some(state) = &mut self.cluster_state else {
            return Ok(1);
        };
        if !state.is_controller(self.broker_id) {
            return Ok(1);
        }
        let now = cluster_state::now_ms();
        state.record_heartbeat(req.broker_id, now);
        let timeout = cluster_state::heartbeat_timeout_ms();
        let _changed = state.failover_dead_leaders(now, timeout);
        state.store(&self.data_dir)?;
        self.sync_cluster_view();
        Ok(0)
    }

    pub fn controller_tick(&mut self) -> io::Result<()> {
        let Some(state) = &mut self.cluster_state else {
            return Ok(());
        };
        if !state.is_controller(self.broker_id) {
            return Ok(());
        }
        let now = cluster_state::now_ms();
        state.record_heartbeat(self.broker_id, now);
        let changed_len = self.run_failover_at(now)?;
        if changed_len > 0 {
            eprintln!("[controller] failover: {changed_len} partition(s) reassigned");
        }
        Ok(())
    }

    pub fn run_failover_at(&mut self, now_ms: u64) -> io::Result<usize> {
        let Some(state) = &mut self.cluster_state else {
            return Ok(0);
        };
        if !state.is_controller(self.broker_id) {
            return Ok(0);
        }
        let timeout = cluster_state::heartbeat_timeout_ms();
        let changed = state.failover_dead_leaders(now_ms, timeout);
        if !changed.is_empty() {
            state.store(&self.data_dir)?;
            self.sync_cluster_view();
        }
        Ok(changed.len())
    }

    pub fn controller_addr(&self) -> Option<String> {
        let state = self.cluster_state.as_ref()?;
        let controller_id = state.controller_id()?;
        state
            .brokers
            .iter()
            .find(|b| b.id == controller_id)
            .map(|b| format!("{}:{}", b.host, b.port))
    }

    pub fn join_group(&mut self, req: &JoinGroupRequest) -> io::Result<(u8, u64, u32, Vec<u16>)> {
        self.topic_mut(req.topic_id)?;
        let partition_count = self.topic_config(req.topic_id).partition_count;
        let key = (req.topic_id, req.group_id);
        let group = self
            .groups
            .entry(key)
            .or_insert_with(|| GroupState::new(req.group_id, req.topic_id));
        let (member_id, generation, parts) =
            group.join(req.member_id, partition_count, cluster_state::now_ms())?;
        Ok((0, member_id, generation, parts))
    }

    pub fn group_heartbeat(&mut self, req: &GroupHeartbeatRequest) -> io::Result<(u8, u8)> {
        let key = self
            .groups
            .iter()
            .find(|((_, gid), _)| *gid == req.group_id)
            .map(|(k, _)| *k);
        let Some(key) = key else {
            return Ok((1, 1));
        };
        let topic_id = key.0;
        let partition_count = self.topic_config(topic_id).partition_count;
        let group = self.groups.get_mut(&key).expect("group exists");
        let timeout = coordinator::session_timeout_ms();
        if group.expire_stale_members(cluster_state::now_ms(), timeout) {
            group.rebalance(partition_count, cluster_state::now_ms());
        }
        let (code, rebalance) =
            group.heartbeat(req.member_id, req.generation, cluster_state::now_ms())?;
        Ok((code, u8::from(rebalance)))
    }

    pub fn legacy_push_enabled(&self) -> bool {
        if let Ok(v) = std::env::var("DMQ_LEGACY_PUSH") {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        self.cluster_state.is_none()
    }

    pub fn fetch_requires_leader() -> bool {
        std::env::var("DMQ_FETCH_CONSISTENCY")
            .map(|v| v.eq_ignore_ascii_case("leader"))
            .unwrap_or(false)
    }

    pub fn fetch_redirect_leader(&self, topic_id: u16, partition_id: u16) -> Option<BrokerId> {
        if Self::fetch_requires_leader()
            && self.cluster.is_some()
            && !self.is_partition_leader(topic_id, partition_id)
        {
            Some(self.partition_leader(topic_id, partition_id))
        } else {
            None
        }
    }

    pub fn check_produce_acl(&self, principal: &str, topic_id: u16) -> io::Result<()> {
        self.acl.check(principal, Operation::Produce, topic_id)
    }

    pub fn check_fetch_acl(&self, principal: &str, topic_id: u16) -> io::Result<()> {
        self.acl.check(principal, Operation::Fetch, topic_id)
    }

    pub fn check_admin_acl(&self, principal: &str, topic_id: u16) -> io::Result<()> {
        self.acl.check(principal, Operation::Admin, topic_id)
    }

    pub fn partition_leader(&self, topic_id: u16, partition_id: u16) -> BrokerId {
        self.cluster
            .as_ref()
            .and_then(|c| c.leader_for(topic_id, partition_id))
            .unwrap_or(self.broker_id)
    }

    pub fn partition_replicas(&self, topic_id: u16, partition_id: u16) -> Vec<BrokerId> {
        self.cluster
            .as_ref()
            .map(|c| c.replicas_for(topic_id, partition_id))
            .unwrap_or_else(|| vec![self.broker_id])
    }

    pub fn is_partition_leader(&self, topic_id: u16, partition_id: u16) -> bool {
        self.partition_leader(topic_id, partition_id) == self.broker_id
    }

    pub fn cluster_info_bytes(&self) -> Vec<u8> {
        if let Some(state) = &self.cluster_state {
            return state.encode_cluster_info();
        }
        match &self.cluster {
            Some(cfg) => cfg.encode(),
            None => ClusterConfig {
                min_insync_replicas: 1,
                brokers: vec![crate::cluster::BrokerNode {
                    id: self.broker_id,
                    host: "127.0.0.1".into(),
                    port: broker_port(),
                }],
                assignments: Vec::new(),
            }
            .encode(),
        }
    }

    fn topic_config(&self, topic_id: u16) -> TopicConfig {
        self.topic_configs
            .get(&topic_id)
            .cloned()
            .unwrap_or_else(|| TopicConfig::default_for(topic_id))
    }

    fn load_logs(&mut self) -> io::Result<()> {
        let topic_ids: Vec<u16> = self.topics.keys().copied().collect();
        for topic_id in topic_ids {
            let cfg = self.topic_config(topic_id);
            for p in 0..cfg.partition_count {
                let log = log_store::load_partition_log(
                    &self.data_dir,
                    topic_id,
                    p,
                    Some(cfg.max_records as usize),
                )?;
                self.logs.insert((topic_id, p), log);
            }
        }
        Ok(())
    }

    fn persist_log_meta(&self, topic_id: u16, partition_id: u16) -> io::Result<()> {
        if let Some(log) = self.logs.get(&(topic_id, partition_id)) {
            log_store::store_meta_offsets(
                &self.data_dir,
                topic_id,
                partition_id,
                Some(log.base_offset()),
                Some(log.next_offset()),
            )
        } else {
            Ok(())
        }
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
            self.topic_configs
                .entry(topic_id)
                .or_insert_with(|| TopicConfig::default_for(topic_id));
            self.persist_topics()?;
        }
        self.ensure_log_with_config(topic_id, 0);
        Ok(self.topics.get_mut(&topic_id).expect("topic inserted"))
    }

    fn ensure_log_with_config(&mut self, topic_id: u16, partition_id: u16) {
        let cfg = self.topic_config(topic_id);
        self.logs
            .entry((topic_id, partition_id))
            .or_insert_with(|| PartitionLog::with_max_records(cfg.max_records as usize));
    }

    fn ensure_log(&mut self, topic_id: u16, partition_id: u16) {
        self.ensure_log_with_config(topic_id, partition_id);
    }

    pub fn append_log(
        &mut self,
        topic_id: u16,
        partition_id: u16,
        payload: &[u8],
    ) -> io::Result<u64> {
        limits::validate_produce_payload(payload.len())?;
        self.topic_mut(topic_id)?;
        self.ensure_log_with_config(topic_id, partition_id);
        let offset = self
            .logs
            .get_mut(&(topic_id, partition_id))
            .expect("log exists")
            .append(payload);
        let record = Record {
            offset,
            payload: payload.to_vec(),
        };
        log_store::append_record(&self.data_dir, topic_id, partition_id, &record)?;
        self.persist_log_meta(topic_id, partition_id)?;
        self.metrics.record_produce(payload.len());
        Ok(offset)
    }

    pub fn produce_idempotent(&mut self, req: &IdempotentProduceRequest) -> io::Result<u64> {
        limits::validate_produce_payload(req.payload.len())?;
        match self.idempotency.resolve(
            req.topic_id,
            req.partition_id,
            req.producer_id,
            req.sequence,
        )? {
            IdempotencyAction::ReturnOffset(offset) => return Ok(offset),
            IdempotencyAction::Append => {}
        }
        let offset = self.append_log(req.topic_id, req.partition_id, &req.payload)?;
        self.idempotency.record(
            &self.data_dir,
            req.topic_id,
            req.partition_id,
            req.producer_id,
            req.sequence,
            offset,
        )?;
        Ok(offset)
    }

    pub fn apply_replica(
        &mut self,
        topic_id: u16,
        partition_id: u16,
        offset: u64,
        payload: &[u8],
    ) -> io::Result<()> {
        if let Some(cluster) = &self.cluster {
            if !cluster.is_replica(self.broker_id, topic_id, partition_id) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "broker is not a replica for partition",
                ));
            }
        }
        self.ensure_log_with_config(topic_id, partition_id);
        let log = self
            .logs
            .get_mut(&(topic_id, partition_id))
            .expect("log exists");
        if offset < log.next_offset() {
            return Ok(());
        }
        if offset > log.next_offset() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("replica gap: expected {} got {}", log.next_offset(), offset),
            ));
        }
        let assigned = log.append(payload);
        debug_assert_eq!(assigned, offset);
        let record = Record {
            offset,
            payload: payload.to_vec(),
        };
        log_store::append_record(&self.data_dir, topic_id, partition_id, &record)?;
        self.persist_log_meta(topic_id, partition_id)?;
        Ok(())
    }

    pub fn is_partition_replica(&self, topic_id: u16, partition_id: u16) -> bool {
        self.cluster
            .as_ref()
            .map(|c| c.is_replica(self.broker_id, topic_id, partition_id))
            .unwrap_or(true)
    }

    pub fn fetch_log(&mut self, req: &FetchRequest) -> io::Result<Vec<Record>> {
        if self.cluster.is_some() && !self.is_partition_replica(req.topic_id, req.partition_id) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "broker is not a replica for partition",
            ));
        }
        self.ensure_log(req.topic_id, req.partition_id);
        let max_bytes = limits::clamp_fetch_bytes(req.max_bytes) as usize;
        let log = self
            .logs
            .get(&(req.topic_id, req.partition_id))
            .expect("log exists");
        let result = log.fetch(req.offset, max_bytes);
        let bytes: usize = result.records.iter().map(|r| r.payload.len()).sum();
        self.metrics.record_fetch(bytes, result.records.len());
        Ok(result.records)
    }

    pub fn commit_offset(&mut self, req: &CommitOffsetRequest) -> io::Result<()> {
        self.committed_offsets
            .insert((req.group_id, req.topic_id, req.partition_id), req.offset);
        store_committed_offset(
            &self.data_dir,
            req.group_id,
            req.topic_id,
            req.partition_id,
            req.offset,
        )?;
        self.metrics.record_commit();
        Ok(())
    }

    pub fn create_topic(&mut self, cfg: TopicConfig) -> io::Result<u8> {
        if self.topics.contains_key(&cfg.topic_id) {
            return Ok(1);
        }
        store_topic_config(
            &self.data_dir,
            cfg.topic_id,
            cfg.partition_count,
            cfg.max_records,
        )?;
        self.topic_configs.insert(cfg.topic_id, cfg.clone());
        self.topic_mut(cfg.topic_id)?;
        for p in 0..cfg.partition_count {
            self.ensure_log_with_config(cfg.topic_id, p);
        }
        Ok(0)
    }

    pub fn describe_topic(&self, topic_id: u16) -> Vec<u8> {
        if !self.topics.contains_key(&topic_id) {
            return vec![0, 0];
        }
        let cfg = self.topic_config(topic_id);
        let mut out = Vec::new();
        out.extend_from_slice(&cfg.partition_count.to_be_bytes());
        for p in 0..cfg.partition_count {
            let log = self
                .logs
                .get(&(topic_id, p))
                .cloned()
                .unwrap_or_else(|| PartitionLog::with_max_records(cfg.max_records as usize));
            let leader = self.partition_leader(topic_id, p);
            let replicas = self.partition_replicas(topic_id, p);
            out.extend_from_slice(&p.to_be_bytes());
            out.extend_from_slice(&log.base_offset().to_be_bytes());
            out.extend_from_slice(&log.next_offset().to_be_bytes());
            out.extend_from_slice(&(log.len() as u32).to_be_bytes());
            out.extend_from_slice(&leader.to_be_bytes());
            out.extend_from_slice(&(replicas.len() as u16).to_be_bytes());
            for r in &replicas {
                out.extend_from_slice(&r.to_be_bytes());
            }
        }
        out
    }

    pub fn list_topics(&self) -> Vec<u8> {
        let mut ids: Vec<u16> = self.topics.keys().copied().collect();
        ids.sort_unstable();
        let mut out = Vec::with_capacity(2 + ids.len() * 2);
        out.extend_from_slice(&(ids.len() as u16).to_be_bytes());
        for id in ids {
            out.extend_from_slice(&id.to_be_bytes());
        }
        out
    }

    pub fn get_lag(&self, group_id: u16, topic_id: u16) -> Vec<u8> {
        let cfg = self.topic_config(topic_id);
        let mut out = Vec::new();
        out.extend_from_slice(&cfg.partition_count.to_be_bytes());
        for p in 0..cfg.partition_count {
            let committed = self.committed_offset(group_id, topic_id, p).unwrap_or(0);
            let log_end = self
                .logs
                .get(&(topic_id, p))
                .map(|l| l.next_offset())
                .unwrap_or(0);
            let lag = log_end.saturating_sub(committed);
            out.extend_from_slice(&p.to_be_bytes());
            out.extend_from_slice(&committed.to_be_bytes());
            out.extend_from_slice(&log_end.to_be_bytes());
            out.extend_from_slice(&lag.to_be_bytes());
        }
        out
    }

    pub fn committed_offset(&self, group_id: u16, topic_id: u16, partition_id: u16) -> Option<u64> {
        self.committed_offsets
            .get(&(group_id, topic_id, partition_id))
            .copied()
    }

    pub fn load_committed_offset(
        &mut self,
        group_id: u16,
        topic_id: u16,
        partition_id: u16,
    ) -> io::Result<Option<u64>> {
        if let Some(v) = self.committed_offset(group_id, topic_id, partition_id) {
            return Ok(Some(v));
        }
        let v = load_committed_offset(&self.data_dir, group_id, topic_id, partition_id)?;
        if let Some(offset) = v {
            self.committed_offsets
                .insert((group_id, topic_id, partition_id), offset);
        }
        Ok(v)
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
        let log_offset = self.append_log(topic_id, 0, payload)?;

        if !self.legacy_push_enabled() {
            return Ok((0, log_offset));
        }

        let topic = self.topic_mut(topic_id)?;

        if topic.cgroups.is_empty() {
            let offset = topic.append_to_staging(payload);
            return Ok((0, log_offset.max(offset)));
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

        Ok((0, log_offset))
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
    use crate::topic_config::TopicConfig;

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

    #[test]
    fn commit_and_fetch_log_roundtrip() {
        let mut broker = Broker::new();
        broker
            .register_producer(&ProducerRegister {
                port: 7778,
                topic_id: 1,
            })
            .unwrap();
        broker.produce_pcm(1, b"a").unwrap();
        broker.produce_pcm(1, b"bb").unwrap();

        let records = broker
            .fetch_log(&FetchRequest {
                topic_id: 1,
                partition_id: 0,
                offset: 0,
                max_bytes: 1024,
                max_wait_ms: 0,
            })
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"a".to_vec());

        broker
            .commit_offset(&CommitOffsetRequest {
                group_id: 7,
                topic_id: 1,
                partition_id: 0,
                offset: 2,
            })
            .unwrap();
        assert_eq!(broker.committed_offset(7, 1, 0), Some(2));
    }

    #[test]
    fn create_topic_and_describe() {
        let mut broker = Broker::new();
        let code = broker.create_topic(TopicConfig::new(9, 2, 100)).unwrap();
        assert_eq!(code, 0);
        let bytes = broker.describe_topic(9);
        let partition_count = u16::from_be_bytes([bytes[0], bytes[1]]);
        assert_eq!(partition_count, 2);
        let leader = u16::from_be_bytes([bytes[24], bytes[25]]);
        assert_eq!(leader, broker.broker_id());
    }

    #[test]
    fn partition_log_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut broker = Broker::open(dir.path()).unwrap();
            broker.append_log(4, 0, b"one").unwrap();
            broker.append_log(4, 0, b"two").unwrap();
        }
        let mut broker = Broker::open(dir.path()).unwrap();
        let records = broker
            .fetch_log(&FetchRequest {
                topic_id: 4,
                partition_id: 0,
                offset: 0,
                max_bytes: 1024,
                max_wait_ms: 0,
            })
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"one".to_vec());
        assert_eq!(records[1].payload, b"two".to_vec());
    }

    #[test]
    fn get_lag_reflects_uncommitted_records() {
        let mut broker = Broker::new();
        broker.append_log(1, 0, b"x").unwrap();
        broker.append_log(1, 0, b"y").unwrap();
        let bytes = broker.get_lag(1, 1);
        let lag = u64::from_be_bytes(bytes[20..28].try_into().unwrap());
        assert_eq!(lag, 2);
    }
}
