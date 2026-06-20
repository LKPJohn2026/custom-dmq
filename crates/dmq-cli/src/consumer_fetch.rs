use custom_dmq::cluster::ClusterConfig;
use custom_dmq::fetch_batch::decode_records;
use custom_dmq::message::{CommitOffsetRequest, FetchRequest, Message};
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

pub async fn run(topic_id: u16, group_id: u16, once: bool) {
    let broker_addr = ClusterConfig::resolve_leader_addr(topic_id, 0);
    let stream = TcpStream::connect(&broker_addr)
        .await
        .expect("[fetch] Could not connect to broker");
    fetch_loop(stream, topic_id, group_id, once).await;
}

async fn fetch_loop(mut stream: TcpStream, topic_id: u16, group_id: u16, once: bool) {
    let mut offset = 0u64;
    loop {
        let req = FetchRequest {
            topic_id,
            partition_id: 0,
            offset,
            max_bytes: 64 * 1024,
            max_wait_ms: 500,
        };
        if custom_dmq::message::write_message(&mut stream, &Message::Fetch(req))
            .await
            .is_err()
        {
            break;
        }

        let mut reader = BufReader::new(&mut stream);
        let Ok(resp) = custom_dmq::message::read_message(&mut reader).await else {
            break;
        };
        let Message::RFetch(bytes) = resp else {
            break;
        };

        let batch = custom_dmq::compression::unwrap_batch(&bytes).unwrap_or(bytes);
        let records = decode_records(&batch).expect("decode fetch batch");
        if records.is_empty() {
            if once {
                eprintln!("[fetch] no records received");
                std::process::exit(1);
            }
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        for rec in &records {
            println!(
                "[fetch] offset={} payload={}",
                rec.offset,
                String::from_utf8_lossy(&rec.payload)
            );
            offset = rec.offset + 1;
        }

        let commit = CommitOffsetRequest {
            group_id,
            topic_id,
            partition_id: 0,
            offset,
        };
        if custom_dmq::message::write_message(&mut stream, &Message::CommitOffset(commit))
            .await
            .is_err()
        {
            break;
        }
        let mut reader = BufReader::new(&mut stream);
        let _ = custom_dmq::message::read_message(&mut reader).await;

        if once {
            break;
        }
    }
}
