//! Integration tests for idempotent produce deduplication.

use custom_dmq::broker::Broker;
use custom_dmq::message::{self, IdempotentProduceRequest, Message};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn duplicate_idempotent_produce_returns_same_offset() {
    let port = pick_free_port();
    let dir = tempdir().unwrap();
    let broker = Arc::new(Mutex::new(Broker::open(dir.path()).expect("open broker")));
    let server = tokio::spawn(run_idempotent_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let req = IdempotentProduceRequest {
        topic_id: 1,
        partition_id: 0,
        producer_id: 42,
        sequence: 0,
        payload: b"once".to_vec(),
    };
    let first = send_idempotent(port, &req).await;
    let second = send_idempotent(port, &req).await;
    assert_eq!(first, second);

    server.abort();
}

async fn send_idempotent(port: u16, req: &IdempotentProduceRequest) -> u64 {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(&mut stream, &Message::IdempotentProduce(req.clone()))
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    match resp {
        Message::RProduce(offset) => offset,
        other => panic!("unexpected response: {other:?}"),
    }
}

async fn run_idempotent_server(broker: Arc<Mutex<Broker>>, port: u16) {
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
            if let Message::IdempotentProduce(req) = msg {
                let offset = {
                    let mut b = broker.lock().await;
                    b.produce_idempotent(&req).unwrap_or(0)
                };
                let _ = message::write_message(&mut writer, &Message::RProduce(offset)).await;
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
