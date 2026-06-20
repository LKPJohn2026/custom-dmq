use custom_dmq::cluster::ClusterConfig;
use custom_dmq::message::{IdempotentProduceRequest, Message, ProduceRequest};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

fn producer_id() -> u64 {
    std::env::var("DMQ_PRODUCER_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

pub async fn run(topic_id: u16, simulate: bool, idempotent: bool, once: bool) {
    let broker_addr = ClusterConfig::resolve_leader_addr(topic_id, 0);
    let stream = TcpStream::connect(&broker_addr)
        .await
        .expect("[produce] Could not connect to broker");

    if simulate {
        simulate_loop(stream, topic_id, idempotent, once).await;
    } else {
        stdin_loop(stream, topic_id, idempotent).await;
    }
}

async fn simulate_loop(mut stream: TcpStream, topic_id: u16, idempotent: bool, once: bool) {
    let mut n = 0u64;
    let pid = producer_id();
    loop {
        if n > 0 && once {
            break;
        }
        if !once {
            sleep(Duration::from_secs(1)).await;
        }
        let line = format!("Hello from producer msg #{n}");
        n += 1;
        if !write_produce(
            &mut stream,
            topic_id,
            idempotent,
            pid,
            n - 1,
            line.into_bytes(),
        )
        .await
        {
            break;
        }
    }
}

async fn stdin_loop(mut stream: TcpStream, topic_id: u16, idempotent: bool) {
    let mut stdin_buf = BufReader::new(tokio::io::stdin());
    let mut input = String::new();
    let mut sequence = 0u64;
    let pid = producer_id();
    println!("[produce] Ready — type a line to append. Ctrl+C to quit.");

    loop {
        input.clear();
        match stdin_buf.read_line(&mut input).await {
            Ok(0) => break,
            Ok(_) => {
                let msg = input.trim();
                if msg.is_empty() {
                    continue;
                }
                if !write_produce(
                    &mut stream,
                    topic_id,
                    idempotent,
                    pid,
                    sequence,
                    msg.as_bytes().to_vec(),
                )
                .await
                {
                    break;
                }
                sequence += 1;
            }
            Err(_) => break,
        }
    }
}

async fn write_produce(
    stream: &mut TcpStream,
    topic_id: u16,
    idempotent: bool,
    producer_id: u64,
    sequence: u64,
    payload: Vec<u8>,
) -> bool {
    let message = if idempotent {
        Message::IdempotentProduce(IdempotentProduceRequest {
            topic_id,
            partition_id: 0,
            producer_id,
            sequence,
            payload,
        })
    } else {
        Message::Produce(ProduceRequest {
            topic_id,
            partition_id: 0,
            payload,
        })
    };
    if custom_dmq::message::write_message(stream, &message)
        .await
        .is_err()
    {
        return false;
    }
    let mut reader = BufReader::new(&mut *stream);
    let Ok(resp) = custom_dmq::message::read_message(&mut reader).await else {
        return false;
    };
    match resp {
        Message::RProduce(offset) => {
            println!("[produce] appended offset={offset} seq={sequence}");
            true
        }
        Message::RNotLeader(leader) => {
            eprintln!("[produce] not leader; retry on broker {leader}");
            false
        }
        other => {
            eprintln!("[produce] unexpected response: {other:?}");
            true
        }
    }
}
