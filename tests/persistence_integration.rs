//! Integration tests for mmap-backed broker recovery.

use custom_dmq::broker::Broker;
use custom_dmq::message::{ConsumerRegister, ProducerRegister};
use tempfile::tempdir;

#[test]
fn staged_messages_survive_broker_restart() {
    let dir = tempdir().unwrap();
    {
        let mut broker = Broker::open(dir.path()).unwrap();
        broker
            .register_producer(&ProducerRegister {
                port: 7778,
                topic_id: 3,
            })
            .unwrap();
        broker.produce_pcm(3, b"before-restart").unwrap();
    }

    let broker = Broker::open(dir.path()).unwrap();
    assert_eq!(broker.topic_staging_len(3), Some(1));
}

#[test]
fn partition_messages_survive_broker_restart() {
    let dir = tempdir().unwrap();
    {
        let mut broker = Broker::open(dir.path()).unwrap();
        broker
            .register_producer(&ProducerRegister {
                port: 7778,
                topic_id: 1,
            })
            .unwrap();
        broker
            .register_consumer(&ConsumerRegister {
                port: 7779,
                topic_id: 1,
                group_id: 7,
            })
            .unwrap();
        broker.produce_pcm(1, b"persisted").unwrap();
    }

    let mut broker = Broker::open(dir.path()).unwrap();
    let mut payload = None;
    assert!(broker
        .consume_from_partition(1, 7, 0, &mut payload)
        .unwrap());
    assert_eq!(payload.as_deref(), Some(b"persisted" as &[u8]));
}

#[test]
fn metadata_restores_topics_groups_and_partitions() {
    let dir = tempdir().unwrap();
    {
        let mut broker = Broker::open(dir.path()).unwrap();
        broker
            .register_producer(&ProducerRegister {
                port: 7778,
                topic_id: 10,
            })
            .unwrap();
        broker
            .register_consumer(&ConsumerRegister {
                port: 7779,
                topic_id: 10,
                group_id: 1,
            })
            .unwrap();
        broker
            .register_consumer(&ConsumerRegister {
                port: 7780,
                topic_id: 10,
                group_id: 1,
            })
            .unwrap();
    }

    let broker = Broker::open(dir.path()).unwrap();
    assert!(broker.has_topic(10));
    assert_eq!(broker.topic_group_count(10), Some(1));
    assert_eq!(broker.topic_group_partition_count(10, 1), Some(2));
}
