//! Leader failover and consumer group coordinator integration tests.

use custom_dmq::broker::Broker;
use custom_dmq::cluster::{BrokerNode, ClusterConfig, PartitionAssignment};
use custom_dmq::cluster_state::{self, ClusterState};
use custom_dmq::message::{self, GroupHeartbeatRequest, JoinGroupRequest, Message};
use custom_dmq::topic_config::TopicConfig;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

fn three_broker_cluster(p1: u16, p2: u16, p3: u16) -> ClusterConfig {
    ClusterConfig {
        min_insync_replicas: 2,
        brokers: vec![
            BrokerNode {
                id: 1,
                host: "127.0.0.1".into(),
                port: p1,
            },
            BrokerNode {
                id: 2,
                host: "127.0.0.1".into(),
                port: p2,
            },
            BrokerNode {
                id: 3,
                host: "127.0.0.1".into(),
                port: p3,
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
fn controller_failover_promotes_in_sync_follower() {
    let cluster = three_broker_cluster(7777, 7778, 7779);
    let dir = tempdir().unwrap();
    let mut broker = Broker::open_with_cluster_and_id(dir.path(), Some(cluster), 1).unwrap();
    broker.create_topic(TopicConfig::new(1, 2, 1000)).unwrap();

    let now = cluster_state::now_ms();
    {
        let state = broker.cluster_state_mut().unwrap();
        state.record_heartbeat(2, now);
        state.record_heartbeat(3, now);
        state
            .broker_last_seen_ms
            .insert(1, now.saturating_sub(20_000));
    }
    broker.run_failover_at(now).unwrap();

    assert_eq!(broker.partition_leader(1, 0), 2);
    assert_eq!(broker.cluster_state().unwrap().leader_epoch(1, 0), 2);
    assert!(!broker.is_partition_leader(1, 0));
}

#[test]
fn stale_leader_rejected_after_failover_view_applied() {
    let cluster = three_broker_cluster(7777, 7778, 7779);
    let leader_dir = tempdir().unwrap();
    let follower_dir = tempdir().unwrap();
    let mut controller =
        Broker::open_with_cluster_and_id(leader_dir.path(), Some(cluster.clone()), 1).unwrap();
    let mut follower =
        Broker::open_with_cluster_and_id(follower_dir.path(), Some(cluster), 2).unwrap();

    controller
        .create_topic(TopicConfig::new(1, 1, 1000))
        .unwrap();
    follower.create_topic(TopicConfig::new(1, 1, 1000)).unwrap();

    let now = cluster_state::now_ms();
    {
        let state = controller.cluster_state_mut().unwrap();
        state.record_heartbeat(2, now);
        state.record_heartbeat(3, now);
        state
            .broker_last_seen_ms
            .insert(1, now.saturating_sub(20_000));
    }
    controller.run_failover_at(now).unwrap();

    let live = controller.cluster_state().unwrap().clone();
    follower.apply_cluster_state(live).unwrap();

    assert!(!controller.is_partition_leader(1, 0));
    assert!(follower.is_partition_leader(1, 0));
    assert_eq!(follower.partition_leader(1, 0), 2);
    assert!(follower.append_log(1, 0, b"ok").is_ok());
}

#[tokio::test]
async fn join_group_assigns_topic_partitions() {
    let port = pick_free_port();
    let cluster = ClusterConfig {
        min_insync_replicas: 1,
        brokers: vec![BrokerNode {
            id: 1,
            host: "127.0.0.1".into(),
            port,
        }],
        assignments: vec![],
    };
    let dir = tempdir().unwrap();
    let broker = Arc::new(Mutex::new(
        Broker::open_with_cluster_and_id(dir.path(), Some(cluster), 1).unwrap(),
    ));
    {
        let mut b = broker.lock().await;
        b.create_topic(TopicConfig::new(1, 4, 1000)).unwrap();
    }

    let server = tokio::spawn(run_join_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::JoinGroup(JoinGroupRequest {
            group_id: 1,
            topic_id: 1,
            member_id: 0,
        }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    let Message::RJoinGroup(bytes) = resp else {
        panic!("expected join response");
    };
    assert_eq!(bytes[0], 0);
    assert!(bytes.len() > 11);

    server.abort();
}

#[tokio::test]
async fn group_heartbeat_keeps_membership() {
    let port = pick_free_port();
    let dir = tempdir().unwrap();
    let broker = Arc::new(Mutex::new(Broker::open(dir.path()).unwrap()));
    {
        let mut b = broker.lock().await;
        b.create_topic(TopicConfig::new(1, 2, 1000)).unwrap();
    }

    let server = tokio::spawn(run_join_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::JoinGroup(JoinGroupRequest {
            group_id: 2,
            topic_id: 1,
            member_id: 0,
        }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let join_resp = message::read_message(&mut reader).await.unwrap();
    let Message::RJoinGroup(join_bytes) = join_resp else {
        panic!("expected join");
    };
    let member_id = u64::from_be_bytes([
        join_bytes[1],
        join_bytes[2],
        join_bytes[3],
        join_bytes[4],
        join_bytes[5],
        join_bytes[6],
        join_bytes[7],
        join_bytes[8],
    ]);
    let generation = u32::from_be_bytes([
        join_bytes[9],
        join_bytes[10],
        join_bytes[11],
        join_bytes[12],
    ]);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::GroupHeartbeat(GroupHeartbeatRequest {
            group_id: 2,
            member_id,
            generation,
        }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let hb = message::read_message(&mut reader).await.unwrap();
    assert_eq!(hb, Message::RGroupHeartbeat(0, 0));

    server.abort();
}

#[tokio::test]
async fn get_cluster_v2_includes_leader_epoch() {
    let port = pick_free_port();
    let cluster = three_broker_cluster(port, port + 1, port + 2);
    let dir = tempdir().unwrap();
    let broker = Arc::new(Mutex::new(
        Broker::open_with_cluster_and_id(dir.path(), Some(cluster), 1).unwrap(),
    ));

    let server = tokio::spawn(run_cluster_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(&mut stream, &Message::GetCluster)
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    let Message::RGetCluster(bytes) = resp else {
        panic!("expected cluster info");
    };
    assert_eq!(bytes[0], 2);
    let decoded = ClusterState::decode_cluster_info(&bytes).unwrap();
    assert_eq!(decoded.leader_epoch(1, 0), 1);

    server.abort();
}

async fn run_join_server(broker: Arc<Mutex<Broker>>, port: u16) {
    run_handler_server(broker, port, |broker, msg| async move {
        match msg {
            Message::JoinGroup(req) => {
                let bytes = {
                    let mut b = broker.lock().await;
                    match b.join_group(&req) {
                        Ok((code, member_id, generation, parts)) => {
                            message::encode_join_group_response(code, member_id, generation, &parts)
                        }
                        Err(_) => message::encode_join_group_response(1, 0, 0, &[]),
                    }
                };
                Some(Message::RJoinGroup(bytes))
            }
            Message::GroupHeartbeat(req) => {
                let (code, flag) = {
                    let mut b = broker.lock().await;
                    b.group_heartbeat(&req).unwrap_or((1, 1))
                };
                Some(Message::RGroupHeartbeat(code, flag))
            }
            _ => None,
        }
    })
    .await;
}

async fn run_cluster_server(broker: Arc<Mutex<Broker>>, port: u16) {
    run_handler_server(broker, port, |broker, msg| async move {
        match msg {
            Message::GetCluster => {
                let bytes = {
                    let b = broker.lock().await;
                    b.cluster_info_bytes()
                };
                Some(Message::RGetCluster(bytes))
            }
            _ => None,
        }
    })
    .await;
}

async fn run_handler_server<F, Fut>(broker: Arc<Mutex<Broker>>, port: u16, handler: F)
where
    F: Fn(Arc<Mutex<Broker>>, Message) -> Fut + Send + Sync + Copy + 'static,
    Fut: std::future::Future<Output = Option<Message>> + Send,
{
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            break;
        };
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let Ok(msg) = message::read_message(&mut reader).await else {
                return;
            };
            if let Some(response) = handler(Arc::clone(&broker), msg).await {
                let _ = message::write_message(&mut writer, &response).await;
            }
        });
    }
}

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
