//! Producer client using binary P_REG / PCM frames.
//!
//! Updated from text `REGISTER_PRODUCER` to the binary protocol. Startup order:
//!   1. Bind own TCP port
//!   2. Send P_REG to broker
//!   3. Accept broker dial-back
//!   4. Send PCM payloads on the persistent connection

use custom_dmq::broker::broker_addr;
use custom_dmq::message::{Message, ProducerRegister};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, Duration};

pub async fn run(port: u16, topic_id: u16, simulate: bool) {
    let addr = format!("127.0.0.1:{}", port);
    let broker_addr = broker_addr();

    sleep(Duration::from_millis(100)).await;

    let listener = TcpListener::bind(&addr)
        .await
        .expect("[producer] Could not bind own port");
    println!("[producer] Listening on {addr} (topic_id={topic_id}, waiting for broker dial-back)");

    let stream = TcpStream::connect(&broker_addr)
        .await
        .expect("[producer] Could not connect to broker");

    let (reader, mut writer) = stream.into_split();
    let mut buf = BufReader::new(reader);

    let reg = ProducerRegister { port, topic_id };
    custom_dmq::message::write_message(&mut writer, &Message::ProducerRegister(reg))
        .await
        .expect("register write");

    println!("[producer] Sent P_REG port={port} topic_id={topic_id}");

    let resp = custom_dmq::message::read_message(&mut buf)
        .await
        .expect("register response");
    match resp {
        Message::RProducerRegister(code) => println!("[producer] Broker ack: {code}"),
        other => eprintln!("[producer] Unexpected response: {other:?}"),
    }

    let (broker_conn, broker_peer) = listener
        .accept()
        .await
        .expect("[producer] Failed to accept broker dial-back");
    println!("[producer] Broker dialed back from {broker_peer}");

    if simulate {
        simulate_loop(broker_conn, port).await;
    } else {
        stdin_loop(broker_conn).await;
    }
}

async fn simulate_loop(stream: TcpStream, port: u16) {
    let (reader, mut writer) = stream.into_split();
    let mut buf = BufReader::new(reader);
    let mut n = 0u64;

    loop {
        sleep(Duration::from_secs(1)).await;
        let line = format!("Hello from producer {port} msg #{n}");
        n += 1;

        if custom_dmq::message::write_message(&mut writer, &Message::Pcm(line.into_bytes()))
            .await
            .is_err()
        {
            break;
        }

        match custom_dmq::message::read_message(&mut buf).await {
            Ok(Message::RPcm(code)) => println!("[producer] R_PCM: {code}"),
            Ok(other) => println!("[producer] Unexpected: {other:?}"),
            Err(e) => {
                eprintln!("[producer] Read error: {e}");
                break;
            }
        }
    }
}

async fn stdin_loop(stream: TcpStream) {
    let (reader, mut writer) = stream.into_split();
    let mut broker_buf = BufReader::new(reader);
    let mut stdin_buf = BufReader::new(tokio::io::stdin());
    let mut input = String::new();

    println!("[producer] Ready — type a line to send PCM. Ctrl+C to quit.");

    loop {
        input.clear();
        match stdin_buf.read_line(&mut input).await {
            Ok(0) => break,
            Ok(_) => {
                let msg = input.trim();
                if msg.is_empty() {
                    continue;
                }
                if custom_dmq::message::write_message(
                    &mut writer,
                    &Message::Pcm(msg.as_bytes().to_vec()),
                )
                .await
                .is_err()
                {
                    break;
                }
                match custom_dmq::message::read_message(&mut broker_buf).await {
                    Ok(Message::RPcm(code)) => println!("[producer] R_PCM: {code}"),
                    Ok(other) => println!("[producer] {other:?}"),
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }
}
