//! Integration tests for partition routing.

use custom_dmq::broker::Broker;
use custom_dmq::message::{ConsumerRegister, ProducerRegister};

#[test]
fn produce_routes_to_each_group_partition() {
    let mut broker = Broker::new();
    broker.register_producer(&ProducerRegister {
        port: 7778,
        topic_id: 1,
    });
    broker.register_consumer(&ConsumerRegister {
        port: 7779,
        topic_id: 1,
        group_id: 1,
    });
    broker.register_consumer(&ConsumerRegister {
        port: 7780,
        topic_id: 1,
        group_id: 2,
    });

    broker.produce_pcm(1, b"shared");

    let mut g1 = None;
    let mut g2 = None;
    assert!(broker.consume_from_partition(1, 1, 0, &mut g1));
    assert!(broker.consume_from_partition(1, 2, 0, &mut g2));
    assert_eq!(g1.as_deref(), Some(b"shared" as &[u8]));
    assert_eq!(g2.as_deref(), Some(b"shared" as &[u8]));
}

#[test]
fn second_consumer_in_group_gets_new_partition() {
    let mut broker = Broker::new();
    broker.register_producer(&ProducerRegister {
        port: 7778,
        topic_id: 1,
    });
    let p0 = broker.register_consumer(&ConsumerRegister {
        port: 7779,
        topic_id: 1,
        group_id: 1,
    });
    let p1 = broker.register_consumer(&ConsumerRegister {
        port: 7780,
        topic_id: 1,
        group_id: 1,
    });

    assert_eq!(p0, 0);
    assert_eq!(p1, 1);
}
