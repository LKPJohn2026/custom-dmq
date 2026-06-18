//! Admin CLI for topic management and consumer lag.

use custom_dmq::broker::broker_addr;
use custom_dmq::message::{
    self, CreateTopicRequest, DescribeTopicRequest, GetLagRequest, Message,
};
use tokio::io::BufReader;
use tokio::net::TcpStream;

pub async fn run(args: &[String]) {
    if args.len() < 3 {
        print_usage();
        std::process::exit(1);
    }
    match args[2].as_str() {
        "create" => create_topic(args).await,
        "describe" => describe_topic(args).await,
        "list" => list_topics().await,
        "lag" => get_lag(args).await,
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!(
        "Usage:
  custom-dmq admin create <topic_id> [partition_count] [max_records]
  custom-dmq admin describe <topic_id>
  custom-dmq admin list
  custom-dmq admin lag <group_id> <topic_id>"
    );
}

async fn create_topic(args: &[String]) {
    let topic_id = parse_u16(args, 3, "topic_id");
    let partition_count = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let max_records = args
        .get(5)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let req = CreateTopicRequest {
        topic_id,
        partition_count,
        max_records,
    };
    let resp = roundtrip(Message::CreateTopic(req)).await;
    let Message::RCreateTopic(code) = resp else {
        eprintln!("unexpected response: {resp:?}");
        std::process::exit(1);
    };
    if code == 0 {
        println!(
            "created topic {topic_id} (partitions={partition_count}, max_records={max_records})"
        );
    } else {
        eprintln!("topic {topic_id} already exists");
        std::process::exit(1);
    }
}

async fn describe_topic(args: &[String]) {
    let topic_id = parse_u16(args, 3, "topic_id");
    let resp = roundtrip(Message::DescribeTopic(DescribeTopicRequest { topic_id })).await;
    let Message::RDescribeTopic(bytes) = resp else {
        eprintln!("unexpected response: {resp:?}");
        std::process::exit(1);
    };
    if bytes.len() < 2 {
        eprintln!("invalid describe response");
        std::process::exit(1);
    }
    let partition_count = u16::from_be_bytes([bytes[0], bytes[1]]);
    println!("topic {topic_id}: {partition_count} partition(s)");
    let mut offset = 2usize;
    for _ in 0..partition_count {
        if offset + 18 > bytes.len() {
            break;
        }
        let pid = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        let base = u64::from_be_bytes(bytes[offset + 2..offset + 10].try_into().unwrap());
        let next = u64::from_be_bytes(bytes[offset + 10..offset + 18].try_into().unwrap());
        let count = u32::from_be_bytes(bytes[offset + 18..offset + 22].try_into().unwrap());
        println!("  partition {pid}: base={base} next={next} records={count}");
        offset += 22;
    }
}

async fn list_topics() {
    let resp = roundtrip(Message::ListTopics).await;
    let Message::RListTopics(bytes) = resp else {
        eprintln!("unexpected response: {resp:?}");
        std::process::exit(1);
    };
    if bytes.len() < 2 {
        println!("no topics");
        return;
    }
    let count = u16::from_be_bytes([bytes[0], bytes[1]]);
    println!("{count} topic(s):");
    for i in 0..count {
        let start = 2 + usize::from(i) * 2;
        if start + 2 > bytes.len() {
            break;
        }
        let id = u16::from_be_bytes([bytes[start], bytes[start + 1]]);
        println!("  {id}");
    }
}

async fn get_lag(args: &[String]) {
    let group_id = parse_u16(args, 3, "group_id");
    let topic_id = parse_u16(args, 4, "topic_id");
    let resp = roundtrip(Message::GetLag(GetLagRequest {
        group_id,
        topic_id,
    }))
    .await;
    let Message::RGetLag(bytes) = resp else {
        eprintln!("unexpected response: {resp:?}");
        std::process::exit(1);
    };
    if bytes.len() < 2 {
        eprintln!("invalid lag response");
        std::process::exit(1);
    }
    let partition_count = u16::from_be_bytes([bytes[0], bytes[1]]);
    println!("lag for group {group_id} topic {topic_id}:");
    let mut offset = 2usize;
    for _ in 0..partition_count {
        if offset + 26 > bytes.len() {
            break;
        }
        let pid = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        let committed = u64::from_be_bytes(bytes[offset + 2..offset + 10].try_into().unwrap());
        let log_end = u64::from_be_bytes(bytes[offset + 10..offset + 18].try_into().unwrap());
        let lag = u64::from_be_bytes(bytes[offset + 18..offset + 26].try_into().unwrap());
        println!("  partition {pid}: committed={committed} end={log_end} lag={lag}");
        offset += 26;
    }
}

async fn roundtrip(request: Message) -> Message {
    let mut stream = TcpStream::connect(broker_addr())
        .await
        .unwrap_or_else(|e| {
            eprintln!("could not connect to broker: {e}");
            std::process::exit(1);
        });
    message::write_message(&mut stream, &request)
        .await
        .expect("write request");
    let mut reader = BufReader::new(stream);
    message::read_message(&mut reader)
        .await
        .expect("read response")
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
