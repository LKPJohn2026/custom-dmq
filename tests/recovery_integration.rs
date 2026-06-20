//! Crash-recovery integration: produce, reopen broker, fetch.

use custom_dmq::broker::Broker;
use custom_dmq::message::FetchRequest;
use tempfile::tempdir;

#[test]
fn produce_survives_broker_restart() {
    let dir = tempdir().unwrap();
    {
        let mut broker = Broker::open(dir.path()).unwrap();
        broker.append_log(11, 0, b"alpha").unwrap();
        broker.append_log(11, 0, b"beta").unwrap();
    }
    let mut broker = Broker::open(dir.path()).unwrap();
    let records = broker
        .fetch_log(&FetchRequest {
            topic_id: 11,
            partition_id: 0,
            offset: 0,
            max_bytes: 4096,
            max_wait_ms: 0,
        })
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, b"alpha".to_vec());
    assert_eq!(records[1].payload, b"beta".to_vec());
}
