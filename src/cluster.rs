//! Static cluster configuration: broker registry and partition assignments.

use serde::Deserialize;
use std::collections::HashMap;
use std::io;
use std::path::Path;

pub type BrokerId = u16;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BrokerNode {
    pub id: BrokerId,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PartitionAssignment {
    pub topic_id: u16,
    pub partition_id: u16,
    pub leader: BrokerId,
    pub replicas: Vec<BrokerId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ClusterConfig {
    #[serde(default = "default_min_isr")]
    pub min_insync_replicas: u16,
    pub brokers: Vec<BrokerNode>,
    pub assignments: Vec<PartitionAssignment>,
}

fn default_min_isr() -> u16 {
    1
}

impl ClusterConfig {
    pub fn load(path: &Path) -> io::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        toml::from_str(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn from_env() -> io::Result<Option<Self>> {
        match std::env::var("DMQ_CLUSTER_CONFIG") {
            Ok(path) => Ok(Some(Self::load(Path::new(&path))?)),
            Err(_) => Ok(None),
        }
    }

    pub fn local_broker_id() -> BrokerId {
        std::env::var("DMQ_BROKER_ID")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1)
    }

    pub fn broker(&self, id: BrokerId) -> Option<&BrokerNode> {
        self.brokers.iter().find(|b| b.id == id)
    }

    pub fn broker_addr(&self, id: BrokerId) -> Option<String> {
        self.broker(id).map(|b| format!("{}:{}", b.host, b.port))
    }

    pub fn assignment(&self, topic_id: u16, partition_id: u16) -> Option<&PartitionAssignment> {
        self.assignments
            .iter()
            .find(|a| a.topic_id == topic_id && a.partition_id == partition_id)
    }

    pub fn leader_for(&self, topic_id: u16, partition_id: u16) -> Option<BrokerId> {
        self.assignment(topic_id, partition_id)
            .map(|a| a.leader)
    }

    pub fn replicas_for(&self, topic_id: u16, partition_id: u16) -> Vec<BrokerId> {
        self.assignment(topic_id, partition_id)
            .map(|a| a.replicas.clone())
            .unwrap_or_default()
    }

    pub fn is_leader(&self, broker_id: BrokerId, topic_id: u16, partition_id: u16) -> bool {
        self.leader_for(topic_id, partition_id) == Some(broker_id)
    }

    pub fn is_replica(&self, broker_id: BrokerId, topic_id: u16, partition_id: u16) -> bool {
        self.replicas_for(topic_id, partition_id).contains(&broker_id)
    }

    pub fn leader_addr(&self, topic_id: u16, partition_id: u16) -> Option<String> {
        let leader = self.leader_for(topic_id, partition_id)?;
        self.broker_addr(leader)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
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
        }
        out
    }

    pub fn decode(payload: &[u8]) -> io::Result<Self> {
        let mut offset = 0usize;
        let min_isr = read_u16(payload, &mut offset)?;
        let broker_count = read_u16(payload, &mut offset)? as usize;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let id = read_u16(payload, &mut offset)?;
            let host_len = read_u8(payload, &mut offset)? as usize;
            let host = String::from_utf8(payload[offset..offset + host_len].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            offset += host_len;
            let port = read_u16(payload, &mut offset)?;
            brokers.push(BrokerNode { id, host, port });
        }
        let assignment_count = read_u16(payload, &mut offset)? as usize;
        let mut assignments = Vec::with_capacity(assignment_count);
        for _ in 0..assignment_count {
            let topic_id = read_u16(payload, &mut offset)?;
            let partition_id = read_u16(payload, &mut offset)?;
            let leader = read_u16(payload, &mut offset)?;
            let replica_count = read_u16(payload, &mut offset)? as usize;
            let mut replicas = Vec::with_capacity(replica_count);
            for _ in 0..replica_count {
                replicas.push(read_u16(payload, &mut offset)?);
            }
            assignments.push(PartitionAssignment {
                topic_id,
                partition_id,
                leader,
                replicas,
            });
        }
        Ok(ClusterConfig {
            min_insync_replicas: min_isr,
            brokers,
            assignments,
        })
    }

    pub fn broker_index(&self) -> HashMap<BrokerId, BrokerNode> {
        self.brokers.iter().map(|b| (b.id, b.clone())).collect()
    }

    pub fn resolve_leader_addr(topic_id: u16, partition_id: u16) -> String {
        match Self::from_env() {
            Ok(Some(cfg)) => cfg
                .leader_addr(topic_id, partition_id)
                .unwrap_or_else(default_broker_addr),
            _ => default_broker_addr(),
        }
    }
}

fn default_broker_addr() -> String {
    let port = std::env::var("DMQ_BROKER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7777);
    format!("127.0.0.1:{port}")
}

fn read_u8(payload: &[u8], offset: &mut usize) -> io::Result<u8> {
    if *offset >= payload.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short buffer"));
    }
    let v = payload[*offset];
    *offset += 1;
    Ok(v)
}

fn read_u16(payload: &[u8], offset: &mut usize) -> io::Result<u16> {
    if *offset + 2 > payload.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short buffer"));
    }
    let v = u16::from_be_bytes([payload[*offset], payload[*offset + 1]]);
    *offset += 2;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> ClusterConfig {
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
        let cfg = sample_config();
        let bytes = cfg.encode();
        let decoded = ClusterConfig::decode(&bytes).unwrap();
        assert_eq!(cfg, decoded);
    }

    #[test]
    fn leader_and_replica_lookup() {
        let cfg = sample_config();
        assert_eq!(cfg.leader_for(1, 0), Some(1));
        assert!(cfg.is_replica(2, 1, 0));
        assert!(!cfg.is_leader(2, 1, 0));
        assert_eq!(cfg.leader_addr(1, 0).as_deref(), Some("127.0.0.1:7777"));
    }

    #[test]
    fn load_example_toml() {
        let path = Path::new("config/cluster.example.toml");
        if !path.exists() {
            return;
        }
        let cfg = ClusterConfig::load(path).unwrap();
        assert_eq!(cfg.brokers.len(), 3);
        assert!(!cfg.assignments.is_empty());
    }
}
