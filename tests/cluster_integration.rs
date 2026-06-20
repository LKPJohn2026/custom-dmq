//! Multi-broker cluster integration tests.

use custom_dmq::broker::Broker;
use custom_dmq::cluster::{BrokerNode, ClusterConfig, PartitionAssignment};
use custom_dmq::message::{self, FetchRequest, Message, ProduceRequest, ReplicateRequest};
use custom_dmq::topic_config::TopicConfig;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

fn sample_cluster(leader_port: u16, follower_port: u16) -> ClusterConfig {
    ClusterConfig {
        min_insync_replicas: 2,
        brokers: vec![
            BrokerNode {
                id: 1,
                host: "127.0.0.1".into(),
                port: leader_port,
            },
            BrokerNode {
                id: 2,
                host: "127.0.0.1".into(),
                port: follower_port,
            },
        ],
        assignments: vec![PartitionAssignment {
            topic_id: 1,
            partition_id: 0,
            leader: 1,
            replicas: vec![1, 2],
        }],
    }
}

#[tokio::test]
async fn replicate_frame_applies_on_follower_broker() {
    let leader_port = pick_free_port();
    let follower_port = pick_free_port();
    let cluster = Arc::new(sample_cluster(leader_port, follower_port));
    let leader_dir = tempdir().unwrap();
    let follower_dir = tempdir().unwrap();

    let leader = Arc::new(Mutex::new(
        Broker::open_with_cluster_and_id(leader_dir.path(), Some((*cluster).clone()), 1).unwrap(),
    ));
    let follower = Arc::new(Mutex::new(
        Broker::open_with_cluster_and_id(follower_dir.path(), Some((*cluster).clone()), 2).unwrap(),
    ));

    {
        let mut b = leader.lock().await;
        b.create_topic(TopicConfig::new(1, 1, 1000)).unwrap();
        b.append_log(1, 0, b"hello").unwrap();
    }

    let server = tokio::spawn(run_replicate_server(Arc::clone(&follower), follower_port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{follower_port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::Replicate(ReplicateRequest {
            topic_id: 1,
            partition_id: 0,
            offset: 0,
            payload: b"hello".to_vec(),
        }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    assert_eq!(resp, Message::RReplicate(0));

    let records = {
        let mut b = follower.lock().await;
        b.fetch_log(&FetchRequest {
            topic_id: 1,
            partition_id: 0,
            offset: 0,
            max_bytes: 1024,
        })
        .unwrap()
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, b"hello".to_vec());

    server.abort();
}

#[tokio::test]
async fn non_leader_returns_not_leader_on_produce() {
    let port = pick_free_port();
    let cluster = sample_cluster(port + 1, port);
    let dir = tempdir().unwrap();
    let broker = Arc::new(Mutex::new(
        Broker::open_with_cluster_and_id(dir.path(), Some(cluster), 2).unwrap(),
    ));
    {
        let mut b = broker.lock().await;
        b.create_topic(TopicConfig::new(1, 1, 1000)).unwrap();
    }

    let server = tokio::spawn(run_produce_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
    message::write_message(
        &mut stream,
        &Message::Produce(ProduceRequest {
            topic_id: 1,
            partition_id: 0,
            payload: b"x".to_vec(),
        }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    assert_eq!(resp, Message::RNotLeader(1));

    server.abort();
}

#[tokio::test]
async fn get_cluster_returns_broker_metadata() {
    let port = pick_free_port();
    let cluster = sample_cluster(port, port + 1);
    let dir = tempdir().unwrap();
    let broker = Arc::new(Mutex::new(
        Broker::open_with_cluster_and_id(dir.path(), Some(cluster.clone()), 1).unwrap(),
    ));

    let server = tokio::spawn(run_cluster_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
    message::write_message(&mut stream, &Message::GetCluster)
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    let Message::RGetCluster(bytes) = resp else {
        panic!("expected cluster info");
    };
    let decoded = if bytes.first() == Some(&2) {
        custom_dmq::cluster_state::ClusterState::decode_cluster_info(&bytes)
            .unwrap()
            .to_cluster_config()
    } else {
        ClusterConfig::decode(&bytes).unwrap()
    };
    assert_eq!(decoded.brokers.len(), 2);

    server.abort();
}

async fn run_replicate_server(broker: Arc<Mutex<Broker>>, port: u16) {
    run_handler_server(broker, port, |broker, msg| async move {
        match msg {
            Message::Replicate(req) => {
                let code = {
                    let mut b = broker.lock().await;
                    match b.apply_replica(
                        req.topic_id,
                        req.partition_id,
                        req.offset,
                        &req.payload,
                    ) {
                        Ok(()) => 0,
                        Err(_) => 1,
                    }
                };
                Some(Message::RReplicate(code))
            }
            _ => None,
        }
    })
    .await;
}

async fn run_produce_server(broker: Arc<Mutex<Broker>>, port: u16) {
    run_handler_server(broker, port, |broker, msg| async move {
        match msg {
            Message::Produce(req) => {
                let b = broker.lock().await;
                if b.cluster().is_some() && !b.is_partition_leader(req.topic_id, req.partition_id) {
                    Some(Message::RNotLeader(b.partition_leader(
                        req.topic_id,
                        req.partition_id,
                    )))
                } else {
                    None
                }
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
