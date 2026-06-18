//! Integration tests for admin API messages.

use custom_dmq::broker::Broker;
use custom_dmq::message::{self, Message};
use custom_dmq::topic_config::TopicConfig;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn create_and_list_topics() {
    let port = pick_free_port();
    std::env::set_var("DMQ_BROKER_PORT", port.to_string());

    let broker: Arc<Mutex<Broker>> = Arc::new(Mutex::new(Broker::new()));
    let server = tokio::spawn(run_admin_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::CreateTopic(custom_dmq::message::CreateTopicRequest {
            topic_id: 42,
            partition_count: 2,
            max_records: 1000,
        }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    assert_eq!(resp, Message::RCreateTopic(0));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(&mut stream, &Message::ListTopics)
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    let Message::RListTopics(bytes) = resp else {
        panic!("expected list");
    };
    let count = u16::from_be_bytes([bytes[0], bytes[1]]);
    assert_eq!(count, 1);
    assert_eq!(u16::from_be_bytes([bytes[2], bytes[3]]), 42);

    server.abort();
}

#[tokio::test]
async fn describe_and_lag_after_produce() {
    let port = pick_free_port();
    std::env::set_var("DMQ_BROKER_PORT", port.to_string());

    let broker: Arc<Mutex<Broker>> = Arc::new(Mutex::new(Broker::new()));
    {
        let mut b = broker.lock().await;
        b.create_topic(TopicConfig::new(7, 1, 100)).unwrap();
        b.append_log(7, 0, b"msg").unwrap();
    }

    let server = tokio::spawn(run_admin_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::DescribeTopic(custom_dmq::message::DescribeTopicRequest { topic_id: 7 }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    let Message::RDescribeTopic(bytes) = resp else {
        panic!("expected describe");
    };
    let next = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    assert_eq!(next, 1);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::GetLag(custom_dmq::message::GetLagRequest {
            group_id: 3,
            topic_id: 7,
        }),
    )
    .await
    .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    let Message::RGetLag(bytes) = resp else {
        panic!("expected lag");
    };
    let lag = u64::from_be_bytes(bytes[20..28].try_into().unwrap());
    assert_eq!(lag, 1);

    server.abort();
}

async fn run_admin_server(broker: Arc<Mutex<Broker>>, port: u16) {
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
            let response = match msg {
                Message::CreateTopic(req) => {
                    let code = {
                        let mut b = broker.lock().await;
                        b.create_topic(TopicConfig::new(
                            req.topic_id,
                            req.partition_count,
                            req.max_records,
                        ))
                        .unwrap_or(1)
                    };
                    Message::RCreateTopic(code)
                }
                Message::DescribeTopic(req) => {
                    let bytes = {
                        let b = broker.lock().await;
                        b.describe_topic(req.topic_id)
                    };
                    Message::RDescribeTopic(bytes)
                }
                Message::ListTopics => {
                    let bytes = {
                        let b = broker.lock().await;
                        b.list_topics()
                    };
                    Message::RListTopics(bytes)
                }
                Message::GetLag(req) => {
                    let bytes = {
                        let b = broker.lock().await;
                        b.get_lag(req.group_id, req.topic_id)
                    };
                    Message::RGetLag(bytes)
                }
                _ => return,
            };
            let _ = message::write_message(&mut writer, &response).await;
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
