// ============================================================
// producer.rs — Producer (race-condition fixed)
//
// Startup order:
//   1. Bind own TCP port FIRST (ln.Listen before sendPortDataToBroker)
//   2. Then connect to broker and send REGISTER_PRODUCER
//   3. Then ln.Accept() — broker is guaranteed to have our port ready
//
// No CREATE_TOPIC phase — broker auto-creates topic on registration.
//
// Messages are sent as raw bytes (PCM-style), matching the ring
// buffer's byte-oriented API.
// ============================================================

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

const BROKER_ADDR: &str = "127.0.0.1:7777";

pub async fn run(producer_id: &str, topic: &str, own_port: u16) {
    let producer_id = producer_id.to_string();
    let topic = topic.to_string();
    let addr = format!("127.0.0.1:{}", own_port);

    // Small delay — let broker bind 7777 first
    sleep(Duration::from_millis(100)).await;

    // -------------------------------------------------------
    // STEP 1 — Bind OUR listener first (professor's order)
    // This eliminates the race: broker can dial back immediately
    // after registration, and our Accept() will be ready.
    // -------------------------------------------------------
    let listener = TcpListener::bind(&addr)
        .await
        .expect("[producer] Could not bind own port");
    println!(
        "[producer:{}] Listening on {} (waiting for broker dial-back)",
        producer_id, addr
    );

    // -------------------------------------------------------
    // STEP 2 — Register with broker
    // Single message: REGISTER_PRODUCER <id> <topic> <port>
    // No CREATE_TOPIC — broker handles that internally
    // -------------------------------------------------------
    println!(
        "[producer:{}] Connecting to broker for registration...",
        producer_id
    );

    let stream = TcpStream::connect(BROKER_ADDR)
        .await
        .expect("[producer] Could not connect to broker");

    let (reader, mut writer) = stream.into_split();
    let mut buf = BufReader::new(reader);
    let mut line = String::new();

    // Read welcome banner
    buf.read_line(&mut line).await.unwrap();
    println!("[producer:{}] {}", producer_id, line.trim());
    line.clear();

    // Send registration
    let reg = format!("REGISTER_PRODUCER {} {} {}\n", producer_id, topic, own_port);
    writer.write_all(reg.as_bytes()).await.unwrap();
    println!(
        "[producer:{}] Sent: REGISTER_PRODUCER {} {} {}",
        producer_id, producer_id, topic, own_port
    );

    // Read ACK
    buf.read_line(&mut line).await.unwrap();
    println!("[producer:{}] Broker: {}", producer_id, line.trim());

    if line.trim().starts_with("ERR") {
        eprintln!("[producer:{}] Registration failed. Exiting.", producer_id);
        return;
    }
    // Registration connection ends here

    // -------------------------------------------------------
    // STEP 3 — Accept broker's dial-back connection
    // Broker guaranteed to dial us now — listener was ready
    // before we even sent the registration message
    // -------------------------------------------------------
    let (broker_conn, broker_addr) = listener
        .accept()
        .await
        .expect("[producer] Failed to accept broker's connection");
    println!(
        "[producer:{}] Broker dialed back from {}",
        producer_id, broker_addr
    );

    // -------------------------------------------------------
    // STEP 4 — Message loop
    // Read stdin → send as raw bytes → read offset ACK
    // -------------------------------------------------------
    message_loop(broker_conn, &producer_id, &topic).await;
}

async fn message_loop(stream: TcpStream, producer_id: &str, topic: &str) {
    let (reader, mut writer) = stream.into_split();
    let mut broker_buf = BufReader::new(reader);
    let mut stdin_buf = BufReader::new(tokio::io::stdin());

    let mut input = String::new();
    let mut response = String::new();

    println!(
        "[producer:{}] Ready — typing sends to topic '{}'. Ctrl+C to quit.",
        producer_id, topic
    );

    loop {
        input.clear();

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

                // Send as PRODUCE <payload> — broker strips prefix, stores raw bytes
                let outgoing = format!("PRODUCE {}\n", msg);
                if writer.write_all(outgoing.as_bytes()).await.is_err() {
                    eprintln!("[producer:{}] Lost connection to broker.", producer_id);
                    break;
                }
                println!("[producer:{}] Sent: PRODUCE {}", producer_id, msg);

                // Read offset confirmation: "OK offset N"
                response.clear();
                match broker_buf.read_line(&mut response).await {
                    Ok(0) => {
                        println!("[producer:{}] Broker closed connection.", producer_id);
                        break;
                    }
                    Ok(_) => {
                        println!("[producer:{}] Broker: {}", producer_id, response.trim());
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
