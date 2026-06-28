//! End-to-end stress + audit against a running broker.

use custom_dmq::broker::Broker;
use custom_dmq::message::{self, Message};
use custom_dmq::topic_config::TopicConfig;
use dmq_stress::config::{StressConfig, VerifyConfig};
use dmq_stress::{audit_topic, run_stress};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[tokio::test]
#[serial]
async fn stress_produce_and_audit_roundtrip() {
    let port = pick_free_port();
    let dir = tempdir().unwrap();
    std::env::set_var("DMQ_BROKER_PORT", port.to_string());
    std::env::remove_var("DMQ_CLUSTER_CONFIG");

    let broker = Arc::new(Mutex::new(Broker::open(dir.path()).unwrap()));
    let server = tokio::spawn(run_stress_server(Arc::clone(&broker), port));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let run_id = "stress-it".to_string();
    let stress = run_stress(StressConfig {
        topic_id: 1,
        partition_id: 0,
        run_id: run_id.clone(),
        target_rps: 200,
        duration: Duration::from_secs(2),
        warmup: Duration::ZERO,
        workers: 4,
        payload_bytes: 64,
        producer_id: 10,
        ledger_path: None,
    })
    .await
    .expect("stress run");

    assert!(stress.acked > 0, "expected at least one acked produce");
    assert_eq!(stress.errors, 0);

    let audit = audit_topic(VerifyConfig {
        topic_id: 1,
        partition_id: 0,
        run_id,
        ledger_path: Some(stress.ledger_path.display().to_string()),
        group_id: 1,
    })
    .await
    .expect("audit");

    assert!(audit.ok(), "audit failed: {audit:?}");

    server.abort();
}

#[tokio::test]
#[serial]
async fn stress_survives_broker_restart() {
    let port = pick_free_port();
    let dir = tempdir().unwrap();
    std::env::set_var("DMQ_BROKER_PORT", port.to_string());
    std::env::remove_var("DMQ_CLUSTER_CONFIG");

    let run_id = "restart-it".to_string();
    let ledger_path = std::env::temp_dir().join(format!("dmq-stress-{run_id}.seq"));

    {
        let broker = Arc::new(Mutex::new(Broker::open(dir.path()).unwrap()));
        let server = tokio::spawn(run_stress_server(Arc::clone(&broker), port));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stress = run_stress(StressConfig {
            topic_id: 2,
            partition_id: 0,
            run_id: run_id.clone(),
            target_rps: 100,
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            workers: 2,
            payload_bytes: 64,
            producer_id: 20,
            ledger_path: Some(ledger_path.display().to_string()),
        })
        .await
        .expect("stress run");

        assert!(stress.acked > 0);
        server.abort();
    }

    let broker = Arc::new(Mutex::new(Broker::open(dir.path()).unwrap()));
    let server = tokio::spawn(run_stress_server(Arc::clone(&broker), port));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let audit = audit_topic(VerifyConfig {
        topic_id: 2,
        partition_id: 0,
        run_id,
        ledger_path: Some(ledger_path.display().to_string()),
        group_id: 2,
    })
    .await
    .expect("audit after restart");

    assert!(audit.ok(), "audit after restart failed: {audit:?}");
    server.abort();
}

async fn run_stress_server(broker: Arc<Mutex<Broker>>, port: u16) {
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
            loop {
                let Ok(msg) = message::read_message(&mut reader).await else {
                    break;
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
                    Message::IdempotentProduce(req) => {
                        let offset = {
                            let mut b = broker.lock().await;
                            match b.produce_idempotent(&req) {
                                Ok(offset) => offset,
                                Err(e) => {
                                    eprintln!("[stress-server] produce error: {e}");
                                    0
                                }
                            }
                        };
                        Message::RProduce(offset)
                    }
                    Message::Fetch(req) => {
                        let records = {
                            let mut b = broker.lock().await;
                            b.fetch_log(&req).unwrap_or_default()
                        };
                        let bytes = custom_dmq::fetch_batch::encode_records(&records);
                        Message::RFetch(bytes)
                    }
                    Message::CommitOffset(req) => {
                        {
                            let mut b = broker.lock().await;
                            let _ = b.commit_offset(&req);
                        }
                        Message::RCommitOffset(0)
                    }
                    _ => break,
                };
                if message::write_message(&mut writer, &response)
                    .await
                    .is_err()
                {
                    break;
                }
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
