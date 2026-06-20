//! Integration tests for binary registration and consumer delivery over TCP.

use custom_dmq::broker::{broker_port, run_consumer_ready_and_send, Broker};
use custom_dmq::message::{self, ConsumerRegister, Message, ProducerRegister};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};

fn enable_legacy_dialback() {
    std::env::set_var("DMQ_LEGACY_DIALBACK", "1");
}

#[tokio::test]
async fn binary_protocol_roundtrip_on_duplex() {
    enable_legacy_dialback();
    let (client, server) = tokio::io::duplex(1024);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let (mut server_read, mut server_write) = tokio::io::split(server);

    let reg = ProducerRegister {
        port: 9001,
        topic_id: 1,
    };
    message::write_message(&mut client_write, &Message::ProducerRegister(reg))
        .await
        .unwrap();

    let msg = message::read_message(&mut server_read).await.unwrap();
    assert!(matches!(msg, Message::ProducerRegister(_)));

    message::write_message(&mut server_write, &Message::RProducerRegister(0))
        .await
        .unwrap();

    let resp = message::read_message(&mut client_read).await.unwrap();
    assert_eq!(resp, Message::RProducerRegister(0));
}

#[tokio::test]
async fn ready_initiated_delivery_over_tcp() {
    enable_legacy_dialback();
    let port = pick_free_port();
    std::env::set_var("DMQ_BROKER_PORT", port.to_string());

    let broker: Arc<Mutex<Broker>> = Arc::new(Mutex::new(Broker::new()));
    let broker_bg = Arc::clone(&broker);
    let server = tokio::spawn(async move {
        run_test_broker(broker_bg, port).await;
    });

    sleep(Duration::from_millis(50)).await;

    let producer_port = pick_free_port();
    let consumer_port = pick_free_port();
    let topic_id = 1u16;
    let group_id = 1u16;

    let consumer_task = tokio::spawn(run_test_consumer(consumer_port, topic_id, group_id));

    sleep(Duration::from_millis(50)).await;

    let producer_task = tokio::spawn(run_test_producer(
        producer_port,
        topic_id,
        b"hello-ready".to_vec(),
    ));

    let received = timeout(Duration::from_secs(5), consumer_task)
        .await
        .expect("consumer timed out")
        .expect("consumer panicked");

    producer_task
        .await
        .expect("producer panicked")
        .expect("producer failed");

    assert_eq!(received, b"hello-ready".to_vec());
    server.abort();
}

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn send_producer_registration(port: u16, topic_id: u16) {
    let stream = TcpStream::connect(format!("127.0.0.1:{}", broker_port()))
        .await
        .unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    message::write_message(
        &mut writer,
        &Message::ProducerRegister(ProducerRegister { port, topic_id }),
    )
    .await
    .unwrap();
    let _ = message::read_message(&mut reader).await.unwrap();
}

async fn send_consumer_registration(port: u16, topic_id: u16, group_id: u16) {
    let stream = TcpStream::connect(format!("127.0.0.1:{}", broker_port()))
        .await
        .unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    message::write_message(
        &mut writer,
        &Message::ConsumerRegister(ConsumerRegister {
            port,
            topic_id,
            group_id,
        }),
    )
    .await
    .unwrap();
    let _ = message::read_message(&mut reader).await.unwrap();
}

async fn run_test_producer(port: u16, topic_id: u16, payload: Vec<u8>) -> Result<(), String> {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .map_err(|e| e.to_string())?;
    send_producer_registration(port, topic_id).await;
    let (stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    message::write_message(&mut writer, &Message::Pcm(payload))
        .await
        .map_err(|e| e.to_string())?;
    match message::read_message(&mut reader)
        .await
        .map_err(|e| e.to_string())?
    {
        Message::RPcm(0) => Ok(()),
        other => Err(format!("unexpected producer ack: {other:?}")),
    }
}

async fn run_test_consumer(port: u16, topic_id: u16, group_id: u16) -> Vec<u8> {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    send_consumer_registration(port, topic_id, group_id).await;
    let (stream, _) = listener.accept().await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    message::write_message(&mut writer, &Message::RPcm(1))
        .await
        .unwrap();

    let msg = message::read_message(&mut reader).await.expect("PCM");
    match msg {
        Message::Pcm(p) => p,
        other => panic!("expected PCM, got {other:?}"),
    }
}

async fn run_test_broker(broker: Arc<Mutex<Broker>>, port: u16) {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    loop {
        let Ok((socket, _)) = listener.accept().await else {
            break;
        };
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            handle_registration(socket, broker).await;
        });
    }
}

async fn handle_registration(socket: TcpStream, broker: Arc<Mutex<Broker>>) {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);

    let Ok(message_in) = message::read_message(&mut reader).await else {
        return;
    };

    let response = match message_in {
        Message::ProducerRegister(reg) => {
            let topic_id = reg.topic_id;
            let port = reg.port;
            {
                let mut b = broker.lock().await;
                b.register_producer(&reg).unwrap();
            }
            tokio::spawn(dial_producer(port, topic_id, Arc::clone(&broker)));
            Message::RProducerRegister(0)
        }
        Message::ConsumerRegister(reg) => {
            let topic_id = reg.topic_id;
            let group_id = reg.group_id;
            let port = reg.port;
            let partition_idx = {
                let mut b = broker.lock().await;
                b.register_consumer(&reg).unwrap()
            };
            tokio::spawn(dial_consumer(
                port,
                topic_id,
                group_id,
                partition_idx,
                Arc::clone(&broker),
            ));
            Message::RConsumerRegister(0)
        }
        Message::Echo(text) => Message::REcho(format!("I have receiver: {text}")),
        _ => return,
    };

    let _ = message::write_message(&mut writer, &response).await;
}

async fn dial_producer(port: u16, topic_id: u16, broker: Arc<Mutex<Broker>>) {
    sleep(Duration::from_millis(30)).await;
    let Ok(stream) = TcpStream::connect(format!("127.0.0.1:{port}")).await else {
        return;
    };
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        let Ok(Message::Pcm(payload)) = message::read_message(&mut reader).await else {
            break;
        };
        let (code, _) = {
            let mut b = broker.lock().await;
            b.produce_pcm(topic_id, &payload).unwrap()
        };
        if message::write_message(&mut writer, &Message::RPcm(code))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn dial_consumer(
    port: u16,
    topic_id: u16,
    group_id: u16,
    partition_idx: u16,
    broker: Arc<Mutex<Broker>>,
) {
    sleep(Duration::from_millis(30)).await;
    let Ok(stream) = TcpStream::connect(format!("127.0.0.1:{port}")).await else {
        return;
    };
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    run_consumer_ready_and_send(
        broker,
        topic_id,
        group_id,
        partition_idx,
        &mut reader,
        &mut writer,
    )
    .await;
}
