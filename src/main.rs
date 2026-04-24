// ============================================================
// main.rs — Broker entry point
//
// Changes from previous version:
//   - CREATE_TOPIC command removed — topics auto-created by broker
//   - REGISTER_PRODUCER handler updated: calls broker.register_producer()
//     which handles topic creation internally
//   - dial_back_to_producer passes raw bytes to broker.produce()
// ============================================================

mod broker;
mod consumer;
mod producer;
mod topic;

use broker::Broker;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

const ADDR: &str = "127.0.0.1:7777";

type SharedBroker = Arc<Mutex<Broker>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("[broker] Listening on {}", ADDR);

    let shared: SharedBroker = Arc::new(Mutex::new(Broker::new()));

    // Producer spawned after broker binds — producer itself binds its
    // own port first before registering, so no race on either side
    tokio::spawn(producer::run("prod-1", "orders", 7778));

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("[broker] New connection from {}", addr);
                let broker = Arc::clone(&shared);
                tokio::spawn(handle_connection(socket, broker));
            }
            Err(e) => eprintln!("[broker] Accept error: {}", e),
        }
    }
}

async fn handle_connection(socket: TcpStream, broker: SharedBroker) {
    let (reader, mut writer) = socket.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    let _ = writer
        .write_all(b"[mini-kafka] Connected. Ready for commands.\n")
        .await;

    loop {
        line.clear();

        match buf_reader.read_line(&mut line).await {
            Ok(0) => {
                println!("[broker] Client disconnected.");
                break;
            }
            Ok(_) => {
                let command = line.trim().to_string();
                if command.is_empty() {
                    continue;
                }

                println!("[broker] Command: {:?}", command);

                // -----------------------------------------------
                // REGISTER_PRODUCER <id> <topic> <port>
                // Broker auto-creates topic if missing, then dials back
                // -----------------------------------------------
                if command.starts_with("REGISTER_PRODUCER") {
                    let parts: Vec<&str> = command.splitn(4, ' ').collect();

                    if parts.len() < 4 {
                        let _ = writer
                            .write_all(b"ERR usage: REGISTER_PRODUCER <id> <topic> <port>\n")
                            .await;
                        break;
                    }

                    let (id, topic_name, port_str) = (parts[1], parts[2], parts[3].trim());

                    match port_str.parse::<u16>() {
                        Err(_) => {
                            let _ = writer.write_all(b"ERR invalid port\n").await;
                            break;
                        }
                        Ok(port) => {
                            // register_producer auto-creates topic internally
                            let response = {
                                let mut b = broker.lock().unwrap();
                                b.register_producer(id, topic_name)
                            };

                            let _ = writer.write_all(response.as_bytes()).await;

                            if response.starts_with("OK") {
                                println!(
                                    "[broker] '{}' registered to '{}' — dialing back on {}",
                                    id, topic_name, port
                                );
                                let broker_clone = Arc::clone(&broker);
                                let topic_owned = topic_name.to_string();
                                tokio::spawn(dial_back_to_producer(
                                    port,
                                    topic_owned,
                                    broker_clone,
                                ));
                            }
                            break; // registration connection ends
                        }
                    }

                // -----------------------------------------------
                // CONSUME <topic> <group>
                // -----------------------------------------------
                } else if command.starts_with("CONSUME") {
                    let parts: Vec<&str> = command.splitn(3, ' ').collect();
                    let response = if parts.len() < 3 {
                        "ERR usage: CONSUME <topic> <group>\n".to_string()
                    } else {
                        let mut b = broker.lock().unwrap();
                        b.consume(parts[1], parts[2])
                    };
                    let _ = writer.write_all(response.as_bytes()).await;

                // -----------------------------------------------
                // Everything else — echo
                // -----------------------------------------------
                } else {
                    let response = format!("[echo] {}\n", command);
                    if writer.write_all(response.as_bytes()).await.is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("[broker] Read error: {}", e);
                break;
            }
        }
    }
}

/// Broker dials back into producer's port.
/// Reads PRODUCE commands, stores raw bytes in the ring buffer.
async fn dial_back_to_producer(port: u16, topic: String, broker: SharedBroker) {
    let addr = format!("127.0.0.1:{}", port);

    // Producer already has its listener bound before registering —
    // no delay needed, but a tiny yield helps task scheduling
    sleep(Duration::from_millis(50)).await;

    println!("[broker] Dialing back to producer at {}", addr);

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[broker] Could not reach producer at {}: {}", addr, e);
            return;
        }
    };

    println!("[broker] Connected to producer at {}", addr);

    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();

        match buf_reader.read_line(&mut line).await {
            Ok(0) => {
                println!("[broker] Producer at {} disconnected.", addr);
                break;
            }
            Ok(_) => {
                let msg = line.trim();
                if msg.is_empty() {
                    continue;
                }

                println!("[broker←producer] {:?}", msg);

                // Strip "PRODUCE " prefix, store raw bytes in ring buffer
                let response = if let Some(payload) = msg.strip_prefix("PRODUCE ") {
                    let mut b = broker.lock().unwrap();
                    b.produce(&topic, payload.as_bytes())
                } else {
                    format!("ERR unknown command: {}\n", msg)
                };

                if writer.write_all(response.as_bytes()).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                eprintln!("[broker] Read error from producer: {}", e);
                break;
            }
        }
    }
}
