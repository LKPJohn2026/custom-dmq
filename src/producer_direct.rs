use custom_dmq::cluster::ClusterConfig;
use custom_dmq::message::{Message, ProduceRequest};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

pub async fn run(topic_id: u16, simulate: bool) {
    let broker_addr = ClusterConfig::resolve_leader_addr(topic_id, 0);
    let stream = TcpStream::connect(&broker_addr)
        .await
        .expect("[produce] Could not connect to broker");

    if simulate {
        simulate_loop(stream, topic_id).await;
    } else {
        stdin_loop(stream, topic_id).await;
    }
}

async fn simulate_loop(mut stream: TcpStream, topic_id: u16) {
    let mut n = 0u64;
    loop {
        sleep(Duration::from_secs(1)).await;
        let line = format!("Hello from producer msg #{n}");
        n += 1;
        let req = ProduceRequest {
            topic_id,
            partition_id: 0,
            payload: line.into_bytes(),
        };
        if custom_dmq::message::write_message(&mut stream, &Message::Produce(req))
            .await
            .is_err()
        {
            break;
        }
        let mut reader = BufReader::new(&mut stream);
        let Ok(resp) = custom_dmq::message::read_message(&mut reader).await else {
            break;
        };
        if let Message::RProduce(offset) = resp {
            println!("[produce] appended offset={offset}");
        } else if let Message::RNotLeader(leader) = resp {
            eprintln!("[produce] not leader; retry on broker {leader}");
            break;
        }
    }
}

async fn stdin_loop(mut stream: TcpStream, topic_id: u16) {
    let mut stdin_buf = BufReader::new(tokio::io::stdin());
    let mut input = String::new();
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
                let req = ProduceRequest {
                    topic_id,
                    partition_id: 0,
                    payload: msg.as_bytes().to_vec(),
                };
                if custom_dmq::message::write_message(&mut stream, &Message::Produce(req))
                    .await
                    .is_err()
                {
                    break;
                }
                let mut reader = BufReader::new(&mut stream);
                let Ok(resp) = custom_dmq::message::read_message(&mut reader).await else {
                    break;
                };
                match resp {
                    Message::RProduce(offset) => println!("[produce] appended offset={offset}"),
                    Message::RNotLeader(leader) => {
                        eprintln!("[produce] not leader; retry on broker {leader}")
                    }
                    other => eprintln!("[produce] unexpected response: {other:?}"),
                }
            }
            Err(_) => break,
        }
    }
}
