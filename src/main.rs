// ============================================================
// main.rs — Broker entry point
//
// Startup sequence:
//   1. Broker binds TCP listener on 7777
//   2. Producer task spawned — it will register then become a server
//   3. Broker accept loop — each connection gets a dedicated task
//
// handle_connection now understands ONE special command:
//   REGISTER_PRODUCER <port>
//     → sends ACK back to producer
//     → spawns a task that dials back to producer's port
//     → reads ECHO messages from producer, responds
//
// All other input still echoes — full protocol comes in protocol.rs
// ============================================================

mod broker;
mod consumer;
mod producer;
mod topic;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

const ADDR: &str = "127.0.0.1:7777";

#[tokio::main]
async fn main() {
    // Step 1 — bind listener BEFORE spawning producer (eliminates race condition)
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("[broker] Listening on {}", ADDR);

    // Step 2 — spawn producer: it will register on 7777, then listen on 7778
    tokio::spawn(producer::run("prod-1", 7778));

    // Step 3 — accept loop
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("[broker] New connection from {}", addr);
                tokio::spawn(handle_connection(socket));
            }
            Err(e) => {
                eprintln!("[broker] Accept error: {}", e);
            }
        }
    }
}

/// Dedicated connection task — one per connected client.
/// Handles REGISTER_PRODUCER specially; everything else echoes.
async fn handle_connection(socket: TcpStream) {
    let (reader, mut writer) = socket.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    // Welcome banner
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

                println!("[broker] Received: {:?}", command);

                // -----------------------------------------------
                // REGISTER_PRODUCER <port>
                //   1. ACK the producer
                //   2. Dial back to producer's port in a new task
                //   3. This registration connection ends after ACK
                // -----------------------------------------------
                if command.starts_with("REGISTER_PRODUCER") {
                    let parts: Vec<&str> = command.splitn(2, ' ').collect();

                    if parts.len() < 2 {
                        let _ = writer.write_all(b"ERR missing port\n").await;
                        break;
                    }

                    match parts[1].trim().parse::<u16>() {
                        Err(_) => {
                            let _ = writer.write_all(b"ERR invalid port\n").await;
                            break;
                        }
                        Ok(port) => {
                            // Send ACK back to producer
                            let _ = writer.write_all(b"OK registered\n").await;
                            println!(
                                "[broker] Producer registered — dialing back on port {}",
                                port
                            );

                            // Spawn task: broker dials back to producer
                            tokio::spawn(dial_back_to_producer(port));

                            // Registration connection is done
                            break;
                        }
                    }
                } else {
                    // Everything else echoes — protocol.rs will replace this
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

/// Broker dials INTO the producer's port.
/// Mirrors the professor's goroutine inside processProducerRegisterMessage:
///   conn, _ := net.Dial("tcp", fmt.Sprintf(":%d", port))
///   for { readMessageFromStream / processBrokerMessage / writeMessageToStream }
async fn dial_back_to_producer(port: u16) {
    let addr = format!("127.0.0.1:{}", port);

    // Small delay — give producer time to reach ln.Accept()
    sleep(Duration::from_millis(150)).await;

    println!("[broker] Dialing back to producer at {}", addr);

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[broker] Could not dial back to producer: {}", e);
            return;
        }
    };

    println!("[broker] Connected to producer at {}", addr);

    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    // Read ECHO messages from producer, respond to each
    loop {
        line.clear();

        match buf_reader.read_line(&mut line).await {
            Ok(0) => {
                println!("[broker] Producer closed connection.");
                break;
            }
            Ok(_) => {
                let msg = line.trim();
                if msg.is_empty() {
                    continue;
                }

                println!("[broker→producer] Received: {:?}", msg);

                // Handle ECHO — strip prefix, wrap response
                let response = if let Some(payload) = msg.strip_prefix("ECHO ") {
                    format!("[echo] {}\n", payload)
                } else {
                    format!("[echo] {}\n", msg)
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
