//! Leader-to-follower record replication over TCP.

use crate::broker::broker_id;
use crate::cluster::{BrokerId, ClusterConfig};
use crate::message::{self, Message, ReplicateRequest};
use std::io;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

pub const DEFAULT_ACKS: &str = "leader";
pub const ACKS_ALL: &str = "all";

pub fn acks_mode() -> String {
    std::env::var("DMQ_ACKS").unwrap_or_else(|_| DEFAULT_ACKS.into())
}

pub fn requires_all_replicas() -> bool {
    acks_mode().eq_ignore_ascii_case(ACKS_ALL)
}

pub async fn replicate_to_followers(
    cluster: &ClusterConfig,
    local_id: BrokerId,
    topic_id: u16,
    partition_id: u16,
    offset: u64,
    payload: &[u8],
) -> io::Result<usize> {
    let replicas = cluster.replicas_for(topic_id, partition_id);
    let mut acks = 0usize;
    for replica_id in replicas {
        if replica_id == local_id {
            acks += 1;
            continue;
        }
        let Some(addr) = cluster.broker_addr(replica_id) else {
            continue;
        };
        if send_replicate(&addr, topic_id, partition_id, offset, payload)
            .await
            .is_ok()
        {
            acks += 1;
        }
    }
    Ok(acks)
}

async fn send_replicate(
    addr: &str,
    topic_id: u16,
    partition_id: u16,
    offset: u64,
    payload: &[u8],
) -> io::Result<()> {
    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "replicate connect timeout"))??;
    let req = ReplicateRequest {
        topic_id,
        partition_id,
        offset,
        payload: payload.to_vec(),
    };
    message::write_message(&mut stream, &Message::Replicate(req)).await?;
    let mut reader = BufReader::new(stream);
    let resp = timeout(Duration::from_secs(2), message::read_message(&mut reader))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "replicate response timeout"))??;
    match resp {
        Message::RReplicate(0) => Ok(()),
        Message::RReplicate(code) => Err(io::Error::other(format!(
            "follower replicate failed code={code}"
        ))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected replicate response: {other:?}"),
        )),
    }
}

pub fn min_required_acks(cluster: &ClusterConfig) -> usize {
    if requires_all_replicas() {
        cluster.min_insync_replicas.max(1) as usize
    } else {
        1
    }
}

pub fn local_broker_id() -> BrokerId {
    broker_id()
}

#[cfg(test)]
mod tests {
    use crate::broker::Broker;
    use crate::topic_config::TopicConfig;

    #[test]
    fn apply_replica_is_idempotent() {
        let mut broker = Broker::new();
        broker
            .create_topic(TopicConfig::new(1, 1, 100))
            .unwrap();
        broker.apply_replica(1, 0, 0, b"a").unwrap();
        broker.apply_replica(1, 0, 0, b"a").unwrap();
        let records = broker.fetch_log(&crate::message::FetchRequest {
            topic_id: 1,
            partition_id: 0,
            offset: 0,
            max_bytes: 1024,
        })
        .unwrap();
        assert_eq!(records.len(), 1);
    }
}
