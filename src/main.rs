mod broker;
mod consumer;
mod topic;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const ADDR: &str = "127.0.0.1:7777";

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("[broker] Listening on {}", ADDR);

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("[broker] New connection from {}", addr);
                tokio::spawn(handle_connection(socket));
            }
            Err(e) => {
                eprintln!("[broker] Failed to accept connection: {}", e);
            }
        }
    }
}

// TCP handler still echoes — broker wiring comes next step
async fn handle_connection(socket: TcpStream) {
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
                let command = line.trim();
                if command.is_empty() {
                    continue;
                }

                println!("[broker] Received: {:?}", command);

                let response = format!("[echo] {}\n", command);
                if writer.write_all(response.as_bytes()).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                eprintln!("[broker] Read error: {}", e);
                break;
            }
        }
    }
}
