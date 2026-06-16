//! CLI entry point: `server`, `producer`, and `consumer` subcommands.

mod consumer_client;
mod consumer_fetch;
mod producer;
mod producer_direct;

use custom_dmq::broker::{broker_port, data_dir_from_env, run_consumer_ready_and_send, Broker};
use custom_dmq::fetch_batch::encode_records;
use custom_dmq::message::Message;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
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
        "produce" => {
            let topic_id = parse_u16(&args, 2, "topic_id");
            let simulate = args.get(3).map(|s| s == "--simulate").unwrap_or(false);
            producer_direct::run(topic_id, simulate).await;
        }
        "consumer" => {
            let port = parse_u16(&args, 2, "port");
            let topic_id = parse_u16(&args, 3, "topic_id");
            let group_id = parse_u16(&args, 4, "group_id");
            consumer_client::run(port, topic_id, group_id).await;
        }
        "fetch" => {
            let topic_id = parse_u16(&args, 2, "topic_id");
            let group_id = parse_u16(&args, 3, "group_id");
            consumer_fetch::run(topic_id, group_id).await;
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
  custom-dmq consumer <port> <topic_id> <group_id>
  custom-dmq produce <topic_id> [--simulate]
  custom-dmq fetch <topic_id> <group_id>"
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

    let broker: SharedBroker = Arc::new(Mutex::new(
        Broker::open(data_dir_from_env()).expect("failed to open broker data dir"),
    ));

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
                if b.register_producer(reg).is_err() {
                    eprintln!("[broker] Failed to register producer");
                    return;
                }
            }
            tokio::spawn(dial_back_to_producer(port, topic_id, Arc::clone(&broker)));
            Message::RProducerRegister(0)
        }
        Message::ConsumerRegister(reg) => {
            let topic_id = reg.topic_id;
            let group_id = reg.group_id;
            let port = reg.port;
            let partition_idx = {
                let mut b = broker.lock().await;
                match b.register_consumer(reg) {
                    Ok(idx) => idx,
                    Err(e) => {
                        eprintln!("[broker] Failed to register consumer: {e}");
                        return;
                    }
                }
            };
            tokio::spawn(dial_back_to_consumer(
                port,
                topic_id,
                group_id,
                partition_idx,
                Arc::clone(&broker),
            ));
            Message::RConsumerRegister(0)
        }
        Message::Fetch(req) => {
            let records = {
                let mut b = broker.lock().await;
                b.fetch_log(req)
            };
            Message::RFetch(encode_records(&records))
        }
        Message::CommitOffset(req) => {
            {
                let mut b = broker.lock().await;
                if b.commit_offset(req).is_err() {
                    return;
                }
            }
            Message::RCommitOffset(0)
        }
        Message::Produce(req) => {
            let (_, offset) = {
                let mut b = broker.lock().await;
                b.produce_pcm(req.topic_id, &req.payload).unwrap_or((1, 0))
            };
            Message::RProduce(offset)
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
                    match b.produce_pcm(topic_id, &payload) {
                        Ok(result) => result,
                        Err(e) => {
                            eprintln!("[broker←producer] produce failed: {e}");
                            break;
                        }
                    }
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

async fn dial_back_to_consumer(
    port: u16,
    topic_id: u16,
    group_id: u16,
    partition_idx: u16,
    broker: SharedBroker,
) {
    let addr = format!("127.0.0.1:{}", port);
    sleep(Duration::from_millis(50)).await;

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[broker] Could not dial consumer at {addr}: {e}");
            return;
        }
    };

    println!(
        "[broker] Connected to consumer at {addr} (topic {topic_id}, group {group_id}, partition {partition_idx})"
    );

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
