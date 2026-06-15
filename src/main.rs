//! CLI entry point: `server`, `producer`, and `consumer` subcommands.
//!
//! Replaces the single-process broker that embedded a demo producer and handled
//! text-line commands. Registration is one binary message per connection;
//! PCM traffic flows on separate dial-back sockets.

mod consumer_client;
mod producer;

use custom_dmq::broker::{broker_port, run_consumer_group_delivery, Broker};
use custom_dmq::cgroup::{ConsumerHandle, PushRequest};
use custom_dmq::message::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

type SharedBroker = Arc<Mutex<Broker>>;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    match args[1].as_str() {
        "server" => run_server().await,
        "producer" => {
            let port = parse_u16(&args, 2, "port");
            let topic_id = parse_u16(&args, 3, "topic_id");
            let simulate = args.get(4).map(|s| s == "--simulate").unwrap_or(false);
            producer::run(port, topic_id, simulate).await;
        }
        "consumer" => {
            let port = parse_u16(&args, 2, "port");
            let topic_id = parse_u16(&args, 3, "topic_id");
            let group_id = parse_u16(&args, 4, "group_id");
            consumer_client::run(port, topic_id, group_id).await;
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!(
        "Usage:
  custom-dmq server
  custom-dmq producer <port> <topic_id> [--simulate]
  custom-dmq consumer <port> <topic_id> <group_id>"
    );
}

fn parse_u16(args: &[String], idx: usize, name: &str) -> u16 {
    args.get(idx)
        .unwrap_or_else(|| {
            eprintln!("Missing {name}");
            print_usage();
            std::process::exit(1);
        })
        .parse()
        .unwrap_or_else(|_| {
            eprintln!("Invalid {name}");
            std::process::exit(1);
        })
}

async fn run_server() {
    let addr = format!("127.0.0.1:{}", broker_port());
    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("[broker] Listening on {addr}");

    let broker: SharedBroker = Arc::new(Mutex::new(Broker::new()));

    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                println!("[broker] Connection from {peer}");
                let broker = Arc::clone(&broker);
                tokio::spawn(handle_broker_connection(socket, broker));
            }
            Err(e) => eprintln!("[broker] Accept error: {e}"),
        }
    }
}

async fn handle_broker_connection(socket: TcpStream, broker: SharedBroker) {
    let (reader, mut writer) = socket.into_split();
    let mut buf = BufReader::new(reader);

    let message = match custom_dmq::message::read_message(&mut buf).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[broker] Read error: {e}");
            return;
        }
    };

    let response = match &message {
        Message::Echo(text) => {
            let reply = {
                let b = broker.lock().await;
                b.process_echo(text)
            };
            Message::REcho(reply)
        }
        Message::ProducerRegister(reg) => {
            let topic_id = reg.topic_id;
            let port = reg.port;
            {
                let mut b = broker.lock().await;
                b.register_producer(reg);
            }
            tokio::spawn(dial_back_to_producer(port, topic_id, Arc::clone(&broker)));
            Message::RProducerRegister(0)
        }
        Message::ConsumerRegister(reg) => {
            let topic_id = reg.topic_id;
            let group_id = reg.group_id;
            let port = reg.port;
            let is_new_group = {
                let mut b = broker.lock().await;
                let (_, is_new, _) = b.register_consumer(reg);
                is_new
            };
            if is_new_group {
                tokio::spawn(run_consumer_group_delivery(
                    Arc::clone(&broker),
                    topic_id,
                    group_id,
                ));
            }
            tokio::spawn(dial_back_to_consumer(
                port,
                topic_id,
                group_id,
                Arc::clone(&broker),
            ));
            Message::RConsumerRegister(0)
        }
        other => {
            eprintln!("[broker] Unexpected message on register port: {other:?}");
            return;
        }
    };

    if custom_dmq::message::write_message(&mut writer, &response)
        .await
        .is_err()
    {
        eprintln!("[broker] Failed to write response");
    }
}

async fn dial_back_to_producer(port: u16, topic_id: u16, broker: SharedBroker) {
    let addr = format!("127.0.0.1:{}", port);
    sleep(Duration::from_millis(50)).await;

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[broker] Could not dial producer at {addr}: {e}");
            return;
        }
    };

    println!("[broker] Connected to producer at {addr} (topic {topic_id})");

    let (reader, mut writer) = stream.into_split();
    let mut buf = BufReader::new(reader);

    loop {
        match custom_dmq::message::read_message(&mut buf).await {
            Ok(Message::Pcm(payload)) => {
                let (code, offset) = {
                    let mut b = broker.lock().await;
                    b.produce_pcm(topic_id, &payload)
                };
                println!(
                    "[broker←producer] topic={topic_id} offset={offset} len={}",
                    payload.len()
                );
                if custom_dmq::message::write_message(&mut writer, &Message::RPcm(code))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(other) => eprintln!("[broker←producer] Unexpected: {other:?}"),
            Err(e) => {
                eprintln!("[broker] Producer disconnected: {e}");
                break;
            }
        }
    }
}

async fn dial_back_to_consumer(port: u16, topic_id: u16, group_id: u16, broker: SharedBroker) {
    let addr = format!("127.0.0.1:{}", port);
    sleep(Duration::from_millis(50)).await;

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[broker] Could not dial consumer at {addr}: {e}");
            return;
        }
    };

    println!("[broker] Connected to consumer at {addr} (topic {topic_id}, group {group_id})");

    let ready = Arc::new(AtomicBool::new(true));
    let (push_tx, mut push_rx) = mpsc::channel::<PushRequest>(16);

    {
        let mut b = broker.lock().await;
        b.add_consumer_handle(
            topic_id,
            group_id,
            ConsumerHandle {
                port,
                ready: Arc::clone(&ready),
                push_tx: push_tx.clone(),
            },
        );
    }

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    while let Some(req) = push_rx.recv().await {
        ready.store(false, Ordering::SeqCst);

        if custom_dmq::message::write_message(&mut writer, &Message::Pcm(req.payload))
            .await
            .is_err()
        {
            break;
        }

        let ack_ok = match custom_dmq::message::read_message(&mut reader).await {
            Ok(Message::RPcm(_)) => true,
            Ok(other) => {
                eprintln!("[broker→consumer] Expected R_PCM, got {other:?}");
                false
            }
            Err(e) => {
                eprintln!("[broker→consumer] Read error: {e}");
                false
            }
        };

        ready.store(true, Ordering::SeqCst);
        let _ = req.ack.send(());

        if !ack_ok {
            break;
        }
    }
}
