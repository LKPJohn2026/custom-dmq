//! Dynamic cluster metadata: live broker liveness, partition leadership, and epochs.
//!
//! Bootstrapped from static TOML (`ClusterConfig`) and persisted under the data directory.
//! The lowest broker id acts as the embedded controller unless `DMQ_CONTROLLER_ID` is set.

use crate::cluster::{BrokerId, BrokerNode, ClusterConfig, PartitionAssignment};
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 6] = b"DMQCS\x01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveAssignment {
    pub topic_id: u16,
    pub partition_id: u16,
    pub leader: BrokerId,
    pub replicas: Vec<BrokerId>,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    pub min_insync_replicas: u16,
    pub brokers: Vec<BrokerNode>,
    pub assignments: Vec<LiveAssignment>,
    pub broker_last_seen_ms: HashMap<BrokerId, u64>,
}

pub fn controller_broker_id(brokers: &[BrokerNode]) -> Option<BrokerId> {
    if let Ok(id) = std::env::var("DMQ_CONTROLLER_ID") {
        return id.parse().ok();
    }
    brokers.iter().map(|b| b.id).min()
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn heartbeat_timeout_ms() -> u64 {
    std::env::var("DMQ_HEARTBEAT_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

pub fn heartbeat_interval_ms() -> u64 {
    std::env::var("DMQ_HEARTBEAT_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3_000)
}

impl ClusterState {
    pub fn from_seed(seed: &ClusterConfig) -> Self {
        let now = now_ms();
        let broker_last_seen_ms = seed.brokers.iter().map(|b| (b.id, now)).collect();
        Self {
            min_insync_replicas: seed.min_insync_replicas,
            brokers: seed.brokers.clone(),
            assignments: seed
                .assignments
                .iter()
                .map(|a| LiveAssignment {
                    topic_id: a.topic_id,
                    partition_id: a.partition_id,
                    leader: a.leader,
                    replicas: a.replicas.clone(),
                    leader_epoch: 1,
                })
                .collect(),
            broker_last_seen_ms,
        }
    }

    pub fn open_or_bootstrap(data_dir: &Path, seed: &ClusterConfig) -> io::Result<Self> {
        let path = cluster_state_path(data_dir);
        if path.exists() {
            load(&path)
        } else {
            let state = Self::from_seed(seed);
            state.store(data_dir)?;
            Ok(state)
        }
    }

    pub fn store(&self, data_dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(data_dir)?;
        let path = cluster_state_path(data_dir);
        let bytes = self.encode();
        std::fs::write(path, bytes)
    }

    pub fn to_cluster_config(&self) -> ClusterConfig {
        ClusterConfig {
            min_insync_replicas: self.min_insync_replicas,
            brokers: self.brokers.clone(),
            assignments: self
                .assignments
                .iter()
                .map(|a| PartitionAssignment {
                    topic_id: a.topic_id,
                    partition_id: a.partition_id,
                    leader: a.leader,
                    replicas: a.replicas.clone(),
                })
                .collect(),
        }
    }

    pub fn controller_id(&self) -> Option<BrokerId> {
        controller_broker_id(&self.brokers)
    }

    pub fn is_controller(&self, broker_id: BrokerId) -> bool {
        self.controller_id() == Some(broker_id)
    }

    pub fn record_heartbeat(&mut self, broker_id: BrokerId, at_ms: u64) {
        if self.brokers.iter().any(|b| b.id == broker_id) {
            self.broker_last_seen_ms.insert(broker_id, at_ms);
        }
    }

    pub fn broker_alive(&self, broker_id: BrokerId, now_ms: u64, timeout_ms: u64) -> bool {
        self.broker_last_seen_ms
            .get(&broker_id)
            .map(|last| now_ms.saturating_sub(*last) <= timeout_ms)
            .unwrap_or(false)
    }

    pub fn leader_epoch(&self, topic_id: u16, partition_id: u16) -> u64 {
        self.assignments
            .iter()
            .find(|a| a.topic_id == topic_id && a.partition_id == partition_id)
            .map(|a| a.leader_epoch)
            .unwrap_or(0)
    }

    pub fn leader_for(&self, topic_id: u16, partition_id: u16) -> Option<BrokerId> {
        self.assignments
            .iter()
            .find(|a| a.topic_id == topic_id && a.partition_id == partition_id)
            .map(|a| a.leader)
    }

    /// Promote the next alive replica when the current leader is dead. Returns changed assignments.
    pub fn failover_dead_leaders(&mut self, now_ms: u64, timeout_ms: u64) -> Vec<LiveAssignment> {
        let dead_leaders: Vec<usize> = self
            .assignments
            .iter()
            .enumerate()
            .filter(|(_, a)| !self.broker_alive(a.leader, now_ms, timeout_ms))
            .map(|(idx, _)| idx)
            .collect();
        let mut changed = Vec::new();
        for idx in dead_leaders {
            let assignment = &self.assignments[idx];
            let Some(new_leader) =
                assignment.replicas.iter().copied().find(|id| {
                    *id != assignment.leader && self.broker_alive(*id, now_ms, timeout_ms)
                })
            else {
                continue;
            };
            let assignment = &mut self.assignments[idx];
            assignment.leader = new_leader;
            assignment.leader_epoch = assignment.leader_epoch.saturating_add(1);
            changed.push(assignment.clone());
        }
        changed
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.min_insync_replicas.to_be_bytes());
        out.extend_from_slice(&(self.brokers.len() as u16).to_be_bytes());
        for b in &self.brokers {
            out.extend_from_slice(&b.id.to_be_bytes());
            let host = b.host.as_bytes();
            out.push(host.len().min(255) as u8);
            out.extend_from_slice(&host[..host.len().min(255)]);
            out.extend_from_slice(&b.port.to_be_bytes());
        }
        out.extend_from_slice(&(self.assignments.len() as u16).to_be_bytes());
        for a in &self.assignments {
            out.extend_from_slice(&a.topic_id.to_be_bytes());
            out.extend_from_slice(&a.partition_id.to_be_bytes());
            out.extend_from_slice(&a.leader.to_be_bytes());
            out.extend_from_slice(&(a.replicas.len() as u16).to_be_bytes());
            for r in &a.replicas {
                out.extend_from_slice(&r.to_be_bytes());
            }
            out.extend_from_slice(&a.leader_epoch.to_be_bytes());
        }
        out.extend_from_slice(&(self.broker_last_seen_ms.len() as u16).to_be_bytes());
        for (id, seen) in &self.broker_last_seen_ms {
            out.extend_from_slice(&id.to_be_bytes());
            out.extend_from_slice(&seen.to_be_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < MAGIC.len() || &bytes[..MAGIC.len()] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid cluster state magic",
            ));
        }
        Self::decode_body(&bytes[MAGIC.len()..])
    }

    /// Wire format for GET_CLUSTER: version 2 cluster config with leader epochs.
    pub fn encode_cluster_info(&self) -> Vec<u8> {
        let mut out = vec![2u8]; // version 2
        out.extend_from_slice(&self.min_insync_replicas.to_be_bytes());
        out.extend_from_slice(&(self.brokers.len() as u16).to_be_bytes());
        for b in &self.brokers {
            out.extend_from_slice(&b.id.to_be_bytes());
            let host = b.host.as_bytes();
            out.push(host.len().min(255) as u8);
            out.extend_from_slice(&host[..host.len().min(255)]);
            out.extend_from_slice(&b.port.to_be_bytes());
        }
        out.extend_from_slice(&(self.assignments.len() as u16).to_be_bytes());
        for a in &self.assignments {
            out.extend_from_slice(&a.topic_id.to_be_bytes());
            out.extend_from_slice(&a.partition_id.to_be_bytes());
            out.extend_from_slice(&a.leader.to_be_bytes());
            out.extend_from_slice(&(a.replicas.len() as u16).to_be_bytes());
            for r in &a.replicas {
                out.extend_from_slice(&r.to_be_bytes());
            }
            out.extend_from_slice(&a.leader_epoch.to_be_bytes());
        }
        out
    }

    pub fn decode_cluster_info(bytes: &[u8]) -> io::Result<Self> {
        if bytes.is_empty() {
            return Err(short_buffer());
        }
        if bytes[0] == 1 {
            let cfg = ClusterConfig::decode(&bytes[1..])?;
            return Ok(Self::from_seed(&cfg));
        }
        if bytes[0] != 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported cluster info version",
            ));
        }
        Self::decode_body(&bytes[1..])
    }

    fn decode_body(bytes: &[u8]) -> io::Result<Self> {
        let mut offset = 0usize;
        let min_isr = read_u16(bytes, &mut offset)?;
        let broker_count = read_u16(bytes, &mut offset)? as usize;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let id = read_u16(bytes, &mut offset)?;
            let host_len = read_u8(bytes, &mut offset)? as usize;
            if offset + host_len + 2 > bytes.len() {
                return Err(short_buffer());
            }
            let host = String::from_utf8(bytes[offset..offset + host_len].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            offset += host_len;
            let port = read_u16(bytes, &mut offset)?;
            brokers.push(BrokerNode { id, host, port });
        }
        let assignment_count = read_u16(bytes, &mut offset)? as usize;
        let mut assignments = Vec::with_capacity(assignment_count);
        for _ in 0..assignment_count {
            let topic_id = read_u16(bytes, &mut offset)?;
            let partition_id = read_u16(bytes, &mut offset)?;
            let leader = read_u16(bytes, &mut offset)?;
            let replica_count = read_u16(bytes, &mut offset)? as usize;
            let mut replicas = Vec::with_capacity(replica_count);
            for _ in 0..replica_count {
                replicas.push(read_u16(bytes, &mut offset)?);
            }
            let leader_epoch = read_u64(bytes, &mut offset)?;
            assignments.push(LiveAssignment {
                topic_id,
                partition_id,
                leader,
                replicas,
                leader_epoch,
            });
        }
        let liveness_count = if offset + 2 <= bytes.len() {
            read_u16(bytes, &mut offset)? as usize
        } else {
            0
        };
        let mut broker_last_seen_ms = HashMap::with_capacity(liveness_count);
        for _ in 0..liveness_count {
            let id = read_u16(bytes, &mut offset)?;
            let seen = read_u64(bytes, &mut offset)?;
            broker_last_seen_ms.insert(id, seen);
        }
        Ok(ClusterState {
            min_insync_replicas: min_isr,
            brokers,
            assignments,
            broker_last_seen_ms,
        })
    }
}

pub fn cluster_state_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("cluster_state.dat")
}

fn load(path: &Path) -> io::Result<ClusterState> {
    let bytes = std::fs::read(path)?;
    ClusterState::decode(&bytes)
}

fn read_u8(payload: &[u8], offset: &mut usize) -> io::Result<u8> {
    if *offset >= payload.len() {
        return Err(short_buffer());
    }
    let v = payload[*offset];
    *offset += 1;
    Ok(v)
}

fn read_u16(payload: &[u8], offset: &mut usize) -> io::Result<u16> {
    if *offset + 2 > payload.len() {
        return Err(short_buffer());
    }
    let v = u16::from_be_bytes([payload[*offset], payload[*offset + 1]]);
    *offset += 2;
    Ok(v)
}

fn read_u64(payload: &[u8], offset: &mut usize) -> io::Result<u64> {
    if *offset + 8 > payload.len() {
        return Err(short_buffer());
    }
    let v = u64::from_be_bytes([
        payload[*offset],
        payload[*offset + 1],
        payload[*offset + 2],
        payload[*offset + 3],
        payload[*offset + 4],
        payload[*offset + 5],
        payload[*offset + 6],
        payload[*offset + 7],
    ]);
    *offset += 8;
    Ok(v)
}

fn short_buffer() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "short buffer")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_seed() -> ClusterConfig {
        ClusterConfig {
            min_insync_replicas: 2,
            brokers: vec![
                BrokerNode {
                    id: 1,
                    host: "127.0.0.1".into(),
                    port: 7777,
                },
                BrokerNode {
                    id: 2,
                    host: "127.0.0.1".into(),
                    port: 7778,
                },
                BrokerNode {
                    id: 3,
                    host: "127.0.0.1".into(),
                    port: 7779,
                },
            ],
            assignments: vec![PartitionAssignment {
                topic_id: 1,
                partition_id: 0,
                leader: 1,
                replicas: vec![1, 2, 3],
            }],
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let state = ClusterState::from_seed(&sample_seed());
        let decoded = ClusterState::decode(&state.encode()).unwrap();
        assert_eq!(state, decoded);
    }

    #[test]
    fn bootstrap_and_persist() {
        let dir = tempdir().unwrap();
        let seed = sample_seed();
        let state = ClusterState::open_or_bootstrap(dir.path(), &seed).unwrap();
        assert_eq!(state.leader_for(1, 0), Some(1));
        assert_eq!(state.leader_epoch(1, 0), 1);

        let reloaded = ClusterState::open_or_bootstrap(dir.path(), &seed).unwrap();
        assert_eq!(reloaded.assignments, state.assignments);
    }

    #[test]
    fn failover_promotes_alive_replica() {
        let mut state = ClusterState::from_seed(&sample_seed());
        let now = 100_000u64;
        state.record_heartbeat(2, now);
        state.record_heartbeat(3, now);
        state.broker_last_seen_ms.insert(1, now - 20_000);

        let changed = state.failover_dead_leaders(now, 10_000);
        assert_eq!(changed.len(), 1);
        assert_eq!(state.leader_for(1, 0), Some(2));
        assert_eq!(state.leader_epoch(1, 0), 2);
    }

    #[test]
    fn cluster_info_v2_roundtrip() {
        let state = ClusterState::from_seed(&sample_seed());
        let bytes = state.encode_cluster_info();
        let decoded = ClusterState::decode_cluster_info(&bytes).unwrap();
        assert_eq!(decoded.assignments, state.assignments);
    }

    #[test]
    fn controller_is_lowest_broker_id() {
        let state = ClusterState::from_seed(&sample_seed());
        assert_eq!(state.controller_id(), Some(1));
    }
}
