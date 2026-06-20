//! Consumer client with ready-initiated delivery.
//!
//! The consumer sends R_PCM to signal readiness, then reads the PCM frame
//! the broker delivers at the group's current log offset.

use custom_dmq::broker::broker_addr;
use custom_dmq::message::{ConsumerRegister, Message};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, Duration};

pub async fn run(port: u16, topic_id: u16, group_id: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let broker_addr = broker_addr();

    sleep(Duration::from_millis(100)).await;

    let listener = TcpListener::bind(&addr)
        .await
        .expect("[consumer] Could not bind own port");
    println!("[consumer] Listening on {addr} (topic_id={topic_id}, group_id={group_id})");

    let stream = TcpStream::connect(&broker_addr)
        .await
        .expect("[consumer] Could not connect to broker");

    let (reader, mut writer) = stream.into_split();
    let mut buf = BufReader::new(reader);

    let reg = ConsumerRegister {
        port,
        topic_id,
        group_id,
    };
    custom_dmq::message::write_message(&mut writer, &Message::ConsumerRegister(reg))
        .await
        .expect("C_REG write");

    println!("[consumer] Sent C_REG port={port} topic_id={topic_id} group_id={group_id}");

    let resp = custom_dmq::message::read_message(&mut buf)
        .await
        .expect("C_REG response");
    match resp {
        Message::RConsumerRegister(code) => println!("[consumer] Broker ack: {code}"),
        other => eprintln!("[consumer] Unexpected response: {other:?}"),
    }

    let (broker_conn, broker_peer) = listener
        .accept()
        .await
        .expect("[consumer] Failed to accept broker dial-back");
    println!("[consumer] Broker dialed back from {broker_peer}");

    receive_loop(broker_conn).await;
}

async fn receive_loop(stream: TcpStream) {
    let (reader, mut writer) = stream.into_split();
    let mut buf = BufReader::new(reader);

    println!("[consumer] Ready — send R_PCM to request next message");

    loop {
        if custom_dmq::message::write_message(&mut writer, &Message::RPcm(1))
            .await
            .is_err()
        {
            break;
        }

        match custom_dmq::message::read_message(&mut buf).await {
            Ok(Message::Pcm(payload)) => {
                let text = String::from_utf8_lossy(&payload);
                println!("[consumer] Received PCM: {text}");
            }
            Ok(other) => {
                println!("[consumer] Unexpected message: {other:?}");
                break;
            }
            Err(e) => {
                eprintln!("[consumer] Connection closed: {e}");
                break;
            }
        }
    }
}
