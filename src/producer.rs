// ============================================================
// producer.rs — Producer (matches professor's architecture)
//
// The producer has TWO roles, executed in sequence:
//
//   ROLE 1 — Client (registration):
//     Connect to broker → send "REGISTER_PRODUCER <own_port>"
//     Read ACK from broker → close this connection
//
//   ROLE 2 — Server (message channel):
//     Bind own TCP port → wait for broker to dial back
//     Accept broker's connection → read stdin → send messages
//     Read broker's echo responses
//
// This mirrors the professor's Go design exactly:
//   producer.sendPortDataToBroker()  →  ROLE 1
//   producer.startProducerServer()   →  ROLE 2
// ============================================================

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

const BROKER_ADDR: &str = "127.0.0.1:7777";

pub async fn run(producer_id: &str, own_port: u16) {
    let producer_id = producer_id.to_string();

    // Small delay — give broker's TcpListener time to bind first
    sleep(Duration::from_millis(100)).await;

    // -------------------------------------------------------
    // ROLE 1: Register with broker
    //   - Connect to broker
    //   - Send: "REGISTER_PRODUCER <own_port>"
    //   - Read ACK
    //   - Connection closes (broker closes after handling)
    // -------------------------------------------------------
    println!(
        "[producer:{}] Connecting to broker for registration...",
        producer_id
    );

    let reg_stream = TcpStream::connect(BROKER_ADDR)
        .await
        .expect("[producer] Could not connect to broker for registration");

    let (reg_reader, mut reg_writer) = reg_stream.into_split();
    let mut reg_buf = BufReader::new(reg_reader);
    let mut line = String::new();

    // Read broker's welcome banner first
    reg_buf.read_line(&mut line).await.unwrap();
    println!("[producer:{}] Broker banner: {}", producer_id, line.trim());
    line.clear();

    // Send REGISTER_PRODUCER with our own port
    let reg_msg = format!("REGISTER_PRODUCER {}\n", own_port);
    reg_writer.write_all(reg_msg.as_bytes()).await.unwrap();
    println!(
        "[producer:{}] Sent: REGISTER_PRODUCER {}",
        producer_id, own_port
    );

    // Read ACK from broker
    reg_buf.read_line(&mut line).await.unwrap();
    println!("[producer:{}] Broker ACK: {}", producer_id, line.trim());
    // Registration connection ends here — broker will now dial back

    // -------------------------------------------------------
    // ROLE 2: Become a server — wait for broker to dial back
    //   Mirrors: ln, _ := net.Listen(...) / conn, _ := ln.Accept()
    // -------------------------------------------------------
    let addr = format!("127.0.0.1:{}", own_port);
    let listener = TcpListener::bind(&addr)
        .await
        .expect("[producer] Could not bind own port");

    println!(
        "[producer:{}] Listening on {} — waiting for broker to dial back...",
        producer_id, addr
    );

    // Accept exactly ONE inbound connection — from the broker
    let (broker_conn, broker_addr) = listener
        .accept()
        .await
        .expect("[producer] Failed to accept broker's inbound connection");

    println!(
        "[producer:{}] Broker connected back from {}",
        producer_id, broker_addr
    );

    // -------------------------------------------------------
    // Message loop — read from stdin, send to broker, print response
    // Mirrors: for { rd.ReadString / writeMessageToStream / readMessageFromStream }
    // -------------------------------------------------------
    message_loop(broker_conn, &producer_id).await;
}

async fn message_loop(stream: TcpStream, producer_id: &str) {
    let (broker_reader, mut broker_writer) = stream.into_split();
    let mut broker_buf = BufReader::new(broker_reader);

    // Read stdin line by line
    let stdin = tokio::io::stdin();
    let mut stdin_buf = BufReader::new(stdin);
    let mut input = String::new();
    let mut response = String::new();

    println!(
        "[producer:{}] Ready. Type messages and press Enter:",
        producer_id
    );

    loop {
        input.clear();

        // Read one line from stdin (blocks until user hits Enter)
        match stdin_buf.read_line(&mut input).await {
            Ok(0) => {
                println!("[producer:{}] EOF — shutting down.", producer_id);
                break;
            }
            Ok(_) => {
                let msg = input.trim();
                if msg.is_empty() {
                    continue;
                }

                // Send ECHO message to broker
                let outgoing = format!("ECHO {}\n", msg);
                if broker_writer.write_all(outgoing.as_bytes()).await.is_err() {
                    eprintln!("[producer:{}] Lost connection to broker.", producer_id);
                    break;
                }
                println!("[producer:{}] Sent: ECHO {}", producer_id, msg);

                // Read broker's response
                response.clear();
                match broker_buf.read_line(&mut response).await {
                    Ok(0) => {
                        println!("[producer:{}] Broker closed connection.", producer_id);
                        break;
                    }
                    Ok(_) => {
                        println!(
                            "[producer:{}] Broker says: {}",
                            producer_id,
                            response.trim()
                        );
                    }
                    Err(e) => {
                        eprintln!("[producer:{}] Read error: {}", producer_id, e);
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("[producer:{}] Stdin error: {}", producer_id, e);
                break;
            }
        }
    }
}
