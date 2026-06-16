//! Integration tests for FETCH/COMMIT protocol.

use custom_dmq::broker::Broker;
use custom_dmq::fetch_batch::decode_records;
use custom_dmq::message::{self, CommitOffsetRequest, FetchRequest, Message, ProducerRegister};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn fetch_returns_records_from_log() {
    let port = pick_free_port();
    std::env::set_var("DMQ_BROKER_PORT", port.to_string());

    let broker: Arc<Mutex<Broker>> = Arc::new(Mutex::new(Broker::new()));
    seed_log(Arc::clone(&broker)).await;

    let server = tokio::spawn(run_minimal_fetch_server(Arc::clone(&broker), port));
    sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    message::write_message(
        &mut stream,
        &Message::Fetch(FetchRequest {
            topic_id: 1,
            partition_id: 0,
            offset: 0,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap();

    let mut reader = BufReader::new(stream);
    let resp = message::read_message(&mut reader).await.unwrap();
    let Message::RFetch(bytes) = resp else {
        panic!("expected RFetch");
    };
    let records = decode_records(&bytes).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, b"a".to_vec());
    assert_eq!(records[1].payload, b"bb".to_vec());

    server.abort();
}

#[tokio::test]
async fn commit_persists_in_broker_memory() {
    let broker: Arc<Mutex<Broker>> = Arc::new(Mutex::new(Broker::new()));
    {
        let mut b = broker.lock().await;
        b.commit_offset(&CommitOffsetRequest {
            group_id: 9,
            topic_id: 1,
            partition_id: 0,
            offset: 123,
        });
        assert_eq!(b.committed_offset(9, 1, 0), Some(123));
    }
}

async fn seed_log(broker: Arc<Mutex<Broker>>) {
    let mut b = broker.lock().await;
    b.register_producer(&ProducerRegister {
        port: 7778,
        topic_id: 1,
    })
    .unwrap();
    b.produce_pcm(1, b"a").unwrap();
    b.produce_pcm(1, b"bb").unwrap();
}

async fn run_minimal_fetch_server(broker: Arc<Mutex<Broker>>, port: u16) {
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
            match msg {
                Message::Fetch(req) => {
                    let records = {
                        let mut b = broker.lock().await;
                        b.fetch_log(&req)
                    };
                    let bytes = custom_dmq::fetch_batch::encode_records(&records);
                    let _ = message::write_message(&mut writer, &Message::RFetch(bytes)).await;
                }
                Message::CommitOffset(req) => {
                    {
                        let mut b = broker.lock().await;
                        b.commit_offset(&req);
                    }
                    let _ = message::write_message(&mut writer, &Message::RCommitOffset(0)).await;
                }
                _ => {}
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
