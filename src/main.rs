//! CLI entry point: `server`, `producer`, and `consumer` subcommands.

mod admin;
mod consumer_client;
mod consumer_fetch;
mod metrics_server;
mod producer;
mod producer_direct;
mod request_log;

use custom_dmq::auth;
use custom_dmq::broker::{broker_port, data_dir_from_env, run_consumer_ready_and_send, Broker};
use custom_dmq::client;
use custom_dmq::compression;
use custom_dmq::fetch_batch::encode_records;
use custom_dmq::message::Message;
use custom_dmq::protocol::{self, Frame, WireFormat};
use custom_dmq::topic_config::TopicConfig;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

type SharedBroker = Arc<Mutex<Broker>>;

struct ConnectionSession {
    wire_format: WireFormat,
    protocol_version: u16,
    principal: String,
    handshaken: bool,
}

impl Default for ConnectionSession {
    fn default() -> Self {
        Self {
            wire_format: WireFormat::V1,
            protocol_version: protocol::PROTOCOL_V1,
            principal: "anonymous".to_string(),
            handshaken: false,
        }
    }
}

enum ProducePlan {
    NotLeader(u16),
    Ack {
        offset: u64,
        cluster: Option<custom_dmq::cluster::ClusterConfig>,
        topic_id: u16,
        partition_id: u16,
        payload: Vec<u8>,
        local_id: u16,
    },
}

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
            let idempotent = args.iter().any(|s| s == "--idempotent");
            let simulate = args.iter().any(|s| s == "--simulate");
            producer_direct::run(topic_id, simulate, idempotent).await;
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
        "admin" => admin::run(&args).await,
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
  custom-dmq produce <topic_id> [--simulate] [--idempotent]
  custom-dmq fetch <topic_id> <group_id>
  custom-dmq admin create|describe|list|lag ..."
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
    if custom_dmq::tls::tls_enabled() {
        println!("[broker] Listening on {addr} (TLS enabled)");
    } else {
        println!("[broker] Listening on {addr}");
    }

    let data_dir = data_dir_from_env();
    let broker: SharedBroker = Arc::new(Mutex::new(
        Broker::open(&data_dir).expect("failed to open broker data dir"),
    ));

    let metrics = {
        let guard = broker.lock().await;
        guard.metrics()
    };
    tokio::spawn(metrics_server::run_metrics_server(metrics, data_dir));

    if {
        let guard = broker.lock().await;
        guard.cluster_state().is_some()
    } {
        tokio::spawn(run_cluster_background(Arc::clone(&broker)));
    }

    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                println!("[broker] Connection from {peer}");
                let broker = Arc::clone(&broker);
                if custom_dmq::tls::tls_enabled() {
                    tokio::spawn(async move {
                        match custom_dmq::tls::accept(socket).await {
                            Ok(tls) => handle_broker_connection(tls, broker, peer.to_string()).await,
                            Err(e) => eprintln!("[broker] TLS accept failed: {e}"),
                        }
                    });
                } else {
                    tokio::spawn(handle_broker_connection(socket, broker, peer.to_string()));
                }
            }
            Err(e) => eprintln!("[broker] Accept error: {e}"),
        }
    }
}

async fn handle_broker_connection<S>(mut socket: S, broker: SharedBroker, peer: String)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut session = ConnectionSession::default();

    loop {
        let started = Instant::now();
        let (frame, wire_format) = match protocol::read_frame(&mut socket).await {
            Ok(v) => v,
            Err(e) => {
                request_log::log_request_error("READ", &peer, &e.to_string());
                return;
            }
        };
        session.wire_format = wire_format;
        request_log::log_request(request_log::request_name(&frame.message), &peer);

        let response = match dispatch_message(&broker, &frame, &mut session, &peer).await {
            Some(msg) => Frame {
                correlation_id: frame.correlation_id,
                message: msg,
            },
            None => return,
        };

        if protocol::write_frame(&mut socket, &response, session.wire_format)
            .await
            .is_err()
        {
            eprintln!("[broker] Failed to write response");
            return;
        }

        {
            let b = broker.lock().await;
            b.metrics().record_request_latency(started.elapsed().as_millis() as u64);
        }

        if matches!(frame.message, Message::Handshake(_)) {
            continue;
        }
        if session.wire_format == WireFormat::V2 && session.protocol_version >= protocol::PROTOCOL_V2
        {
            continue;
        }
        break;
    }
}

async fn dispatch_message(
    broker: &SharedBroker,
    frame: &Frame,
    session: &mut ConnectionSession,
    peer: &str,
) -> Option<Message> {
    match &frame.message {
        Message::Handshake(req) => {
            match client::process_handshake(req) {
                Ok(version) => {
                    session.handshaken = true;
                    session.protocol_version = version;
                    session.principal = auth::principal_from_token(&req.auth_token);
                    Some(Message::RHandshake(0, version))
                }
                Err((code, msg)) => Some(Message::RError(code, msg)),
            }
        }
        Message::Echo(text) => {
            if protocol::require_handshake() && !session.handshaken {
                return Some(Message::RError(1, "handshake required".into()));
            }
            let reply = {
                let b = broker.lock().await;
                b.process_echo(text)
            };
            Some(Message::REcho(reply))
        }
        Message::ProducerRegister(reg) => {
            if !protocol::legacy_dialback_enabled() {
                return Some(Message::RError(
                    1,
                    "dial-back disabled; use produce command".into(),
                ));
            }
            let topic_id = reg.topic_id;
            let port = reg.port;
            {
                let mut b = broker.lock().await;
                if b.register_producer(reg).is_err() {
                    eprintln!("[broker] Failed to register producer");
                    return None;
                }
            }
            tokio::spawn(dial_back_to_producer(port, topic_id, Arc::clone(broker)));
            Some(Message::RProducerRegister(0))
        }
        Message::ConsumerRegister(reg) => {
            if !protocol::legacy_dialback_enabled() {
                return Some(Message::RError(
                    1,
                    "dial-back disabled; use fetch command".into(),
                ));
            }
            let topic_id = reg.topic_id;
            let group_id = reg.group_id;
            let port = reg.port;
            let partition_idx = {
                let mut b = broker.lock().await;
                match b.register_consumer(reg) {
                    Ok(idx) => idx,
                    Err(e) => {
                        eprintln!("[broker] Failed to register consumer: {e}");
                        return None;
                    }
                }
            };
            tokio::spawn(dial_back_to_consumer(
                port,
                topic_id,
                group_id,
                partition_idx,
                Arc::clone(broker),
            ));
            Some(Message::RConsumerRegister(0))
        }
        Message::Fetch(req) => {
            if protocol::require_handshake() && !session.handshaken {
                return Some(Message::RError(1, "handshake required".into()));
            }
            {
                let b = broker.lock().await;
                if let Err(e) = b.check_fetch_acl(&session.principal, req.topic_id) {
                    eprintln!("[broker] fetch acl denied for {peer}: {e}");
                    b.metrics().record_error();
                    return Some(Message::RError(3, "acl denied".into()));
                }
                if let Some(leader) = b.fetch_redirect_leader(req.topic_id, req.partition_id) {
                    return Some(Message::RNotLeader(leader));
                }
            }
            let deadline = if req.max_wait_ms > 0 {
                Some(Instant::now() + Duration::from_millis(req.max_wait_ms as u64))
            } else {
                None
            };
            let records = loop {
                let records = {
                    let mut b = broker.lock().await;
                    match b.fetch_log(req) {
                        Ok(records) => records,
                        Err(e) => {
                            eprintln!("[broker] fetch failed: {e}");
                            b.metrics().record_error();
                            return None;
                        }
                    }
                };
                if !records.is_empty()
                    || deadline.is_none()
                    || Instant::now() >= deadline.unwrap()
                {
                    break records;
                }
                sleep(Duration::from_millis(50)).await;
            };
            let batch = encode_records(&records);
            match compression::wrap_batch(compression::preferred_codec(), &batch) {
                Ok(payload) => Some(Message::RFetch(payload)),
                Err(e) => {
                    eprintln!("[broker] fetch compress failed: {e}");
                    Some(Message::RFetch(batch))
                }
            }
        }
        Message::CommitOffset(req) => {
            {
                let mut b = broker.lock().await;
                if b.commit_offset(req).is_err() {
                    return None;
                }
            }
            Some(Message::RCommitOffset(0))
        }
        Message::Produce(req) => {
            if protocol::require_handshake() && !session.handshaken {
                return Some(Message::RError(1, "handshake required".into()));
            }
            {
                let b = broker.lock().await;
                if let Err(e) = b.check_produce_acl(&session.principal, req.topic_id) {
                    eprintln!("[broker] produce acl denied for {peer}: {e}");
                    b.metrics().record_error();
                    return Some(Message::RError(3, "acl denied".into()));
                }
            }
            let topic_id = req.topic_id;
            let partition_id = req.partition_id;
            let payload = req.payload.clone();
            let produce_plan = {
                let mut b = broker.lock().await;
                if b.cluster().is_some() && !b.is_partition_leader(topic_id, partition_id) {
                    ProducePlan::NotLeader(b.partition_leader(topic_id, partition_id))
                } else {
                    match b.append_log(topic_id, partition_id, &payload) {
                        Ok(offset) => ProducePlan::Ack {
                            offset,
                            cluster: b.cluster().cloned(),
                            topic_id,
                            partition_id,
                            payload,
                            local_id: b.broker_id(),
                        },
                        Err(e) => {
                            eprintln!("[broker] produce failed: {e}");
                            b.metrics().record_error();
                            return None;
                        }
                    }
                }
            };
            Some(match produce_plan {
                ProducePlan::NotLeader(leader) => Message::RNotLeader(leader),
                ProducePlan::Ack {
                    offset,
                    cluster,
                    topic_id,
                    partition_id,
                    payload,
                    local_id,
                } => {
                    if let Some(cluster) = cluster {
                        let acks = custom_dmq::replication::replicate_to_followers(
                            &cluster,
                            local_id,
                            topic_id,
                            partition_id,
                            offset,
                            &payload,
                        )
                        .await
                        .unwrap_or(0);
                        let required = custom_dmq::replication::min_required_acks(&cluster);
                        if custom_dmq::replication::requires_all_replicas() && acks < required {
                            eprintln!(
                                "[broker] insufficient replicas acked {acks}/{required} for topic {topic_id} partition {partition_id}"
                            );
                            let b = broker.lock().await;
                            b.metrics().record_error();
                        }
                    }
                    Message::RProduce(offset)
                }
            })
        }
        Message::IdempotentProduce(req) => {
            if protocol::require_handshake() && !session.handshaken {
                return Some(Message::RError(1, "handshake required".into()));
            }
            {
                let b = broker.lock().await;
                if let Err(e) = b.check_produce_acl(&session.principal, req.topic_id) {
                    eprintln!("[broker] produce acl denied for {peer}: {e}");
                    b.metrics().record_error();
                    return Some(Message::RError(3, "acl denied".into()));
                }
            }
            let topic_id = req.topic_id;
            let partition_id = req.partition_id;
            let payload = req.payload.clone();
            let produce_plan = {
                let mut b = broker.lock().await;
                if b.cluster().is_some() && !b.is_partition_leader(topic_id, partition_id) {
                    ProducePlan::NotLeader(b.partition_leader(topic_id, partition_id))
                } else {
                    match b.produce_idempotent(req) {
                        Ok(offset) => ProducePlan::Ack {
                            offset,
                            cluster: b.cluster().cloned(),
                            topic_id,
                            partition_id,
                            payload,
                            local_id: b.broker_id(),
                        },
                        Err(e) => {
                            eprintln!("[broker] idempotent produce failed: {e}");
                            b.metrics().record_error();
                            return None;
                        }
                    }
                }
            };
            Some(match produce_plan {
                ProducePlan::NotLeader(leader) => Message::RNotLeader(leader),
                ProducePlan::Ack {
                    offset,
                    cluster,
                    topic_id,
                    partition_id,
                    payload,
                    local_id,
                } => {
                    if let Some(cluster) = cluster {
                        let acks = custom_dmq::replication::replicate_to_followers(
                            &cluster,
                            local_id,
                            topic_id,
                            partition_id,
                            offset,
                            &payload,
                        )
                        .await
                        .unwrap_or(0);
                        let required = custom_dmq::replication::min_required_acks(&cluster);
                        if custom_dmq::replication::requires_all_replicas() && acks < required {
                            eprintln!(
                                "[broker] insufficient replicas acked {acks}/{required} for topic {topic_id} partition {partition_id}"
                            );
                            let b = broker.lock().await;
                            b.metrics().record_error();
                        }
                    }
                    Message::RProduce(offset)
                }
            })
        }
        Message::CreateTopic(req) => {
            {
                let b = broker.lock().await;
                if let Err(e) = b.check_admin_acl(&session.principal, req.topic_id) {
                    eprintln!("[broker] admin acl denied for {peer}: {e}");
                    b.metrics().record_error();
                    return Some(Message::RError(3, "acl denied".into()));
                }
            }
            let code = {
                let mut b = broker.lock().await;
                match b.create_topic(TopicConfig::new(
                    req.topic_id,
                    req.partition_count,
                    req.max_records,
                )) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("[broker] create topic failed: {e}");
                        b.metrics().record_error();
                        return None;
                    }
                }
            };
            Some(Message::RCreateTopic(code))
        }
        Message::DescribeTopic(req) => {
            let bytes = {
                let b = broker.lock().await;
                b.describe_topic(req.topic_id)
            };
            Some(Message::RDescribeTopic(bytes))
        }
        Message::ListTopics => {
            let bytes = {
                let b = broker.lock().await;
                b.list_topics()
            };
            Some(Message::RListTopics(bytes))
        }
        Message::GetLag(req) => {
            let bytes = {
                let b = broker.lock().await;
                b.get_lag(req.group_id, req.topic_id)
            };
            Some(Message::RGetLag(bytes))
        }
        Message::Replicate(req) => {
            let code = {
                let mut b = broker.lock().await;
                match b.apply_replica(
                    req.topic_id,
                    req.partition_id,
                    req.offset,
                    &req.payload,
                ) {
                    Ok(()) => 0u8,
                    Err(e) => {
                        eprintln!("[broker] replicate apply failed: {e}");
                        b.metrics().record_error();
                        1u8
                    }
                }
            };
            Some(Message::RReplicate(code))
        }
        Message::GetCluster => {
            let bytes = {
                let b = broker.lock().await;
                b.cluster_info_bytes()
            };
            Some(Message::RGetCluster(bytes))
        }
        Message::BrokerHeartbeat(req) => {
            let code = {
                let mut b = broker.lock().await;
                b.handle_broker_heartbeat(req).unwrap_or(1)
            };
            Some(Message::RBrokerHeartbeat(code))
        }
        Message::JoinGroup(req) => {
            let bytes = {
                let mut b = broker.lock().await;
                match b.join_group(req) {
                    Ok((code, member_id, generation, parts)) => {
                        custom_dmq::message::encode_join_group_response(
                            code,
                            member_id,
                            generation,
                            &parts,
                        )
                    }
                    Err(e) => {
                        eprintln!("[broker] join group failed: {e}");
                        custom_dmq::message::encode_join_group_response(1, 0, 0, &[])
                    }
                }
            };
            Some(Message::RJoinGroup(bytes))
        }
        Message::GroupHeartbeat(req) => {
            let (code, flag) = {
                let mut b = broker.lock().await;
                b.group_heartbeat(req).unwrap_or((1, 1))
            };
            Some(Message::RGroupHeartbeat(code, flag))
        }
        other => {
            eprintln!("[broker] Unexpected message on register port: {other:?}");
            None
        }
    }
}

async fn run_cluster_background(broker: SharedBroker) {
    use custom_dmq::cluster_state;

    loop {
        sleep(Duration::from_millis(cluster_state::heartbeat_interval_ms())).await;
        let plan = {
            let b = broker.lock().await;
            if b.cluster_state().is_none() {
                None
            } else if b.is_controller() {
                Some((true, None, b.broker_id()))
            } else {
                Some((false, b.controller_addr(), b.broker_id()))
            }
        };
        let Some((is_controller, controller_addr, broker_id)) = plan else {
            continue;
        };
        if is_controller {
            let mut b = broker.lock().await;
            if let Err(e) = b.controller_tick() {
                eprintln!("[controller] tick failed: {e}");
            }
        } else if let Some(addr) = controller_addr {
            if let Err(e) = send_broker_heartbeat(&addr, broker_id).await {
                eprintln!("[broker] heartbeat to controller failed: {e}");
            }
            if let Err(e) = sync_cluster_state_from_controller(&broker, &addr).await {
                eprintln!("[broker] cluster sync failed: {e}");
            }
        }
    }
}

async fn send_broker_heartbeat(addr: &str, broker_id: u16) -> std::io::Result<()> {
    use custom_dmq::message::{BrokerHeartbeatRequest, Message};
    let mut stream = TcpStream::connect(addr).await?;
    custom_dmq::message::write_message(
        &mut stream,
        &Message::BrokerHeartbeat(BrokerHeartbeatRequest { broker_id }),
    )
    .await?;
    let mut reader = BufReader::new(stream);
    let _ = custom_dmq::message::read_message(&mut reader).await?;
    Ok(())
}

async fn sync_cluster_state_from_controller(
    broker: &SharedBroker,
    addr: &str,
) -> std::io::Result<()> {
    use custom_dmq::cluster_state::ClusterState;
    use custom_dmq::message::Message;
    let mut stream = TcpStream::connect(addr).await?;
    custom_dmq::message::write_message(&mut stream, &Message::GetCluster).await?;
    let mut reader = BufReader::new(stream);
    let resp = custom_dmq::message::read_message(&mut reader).await?;
    if let Message::RGetCluster(bytes) = resp {
        let state = ClusterState::decode_cluster_info(&bytes)?;
        let mut b = broker.lock().await;
        b.apply_cluster_state(state)?;
    }
    Ok(())
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
