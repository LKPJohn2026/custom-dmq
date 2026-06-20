//! Protocol v2 handshake and correlation id integration tests.

use custom_dmq::auth;
use custom_dmq::client;
use custom_dmq::message::{self, HandshakeRequest, Message};
use custom_dmq::protocol::{self, Frame, WireFormat};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[tokio::test]
async fn v2_handshake_and_correlation_roundtrip() {
    std::env::remove_var("DMQ_AUTH_TOKEN");
    std::env::set_var("DMQ_PROTOCOL_VERSION", "2");

    let dir = tempdir().unwrap();
    let broker = Arc::new(Mutex::new(
        custom_dmq::broker::Broker::open(dir.path()).unwrap(),
    ));
    let port = pick_free_port();
    let server = tokio::spawn(run_protocol_server(Arc::clone(&broker), port));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let addr = format!("127.0.0.1:{port}");
    let (mut stream, version) = client::connect_and_handshake(&addr)
        .await
        .expect("handshake");
    assert_eq!(version, 2);

    stream
        .write_v2(99, &Message::Echo("ping".into()))
        .await
        .unwrap();
    let (frame, _) = stream.read_frame().await.unwrap();
    assert_eq!(frame.correlation_id, 99);
    match frame.message {
        Message::REcho(reply) => assert!(reply.contains("ping")),
        other => panic!("unexpected response: {other:?}"),
    }

    server.abort();
    std::env::remove_var("DMQ_PROTOCOL_VERSION");
}

#[tokio::test]
async fn handshake_rejects_bad_token() {
    std::env::set_var("DMQ_AUTH_TOKEN", "secret");
    let req = HandshakeRequest {
        protocol_version: 2,
        auth_token: b"wrong".to_vec(),
    };
    assert!(client::process_handshake(&req).is_err());
    assert!(auth::validate_token(b"secret").is_ok());
    std::env::remove_var("DMQ_AUTH_TOKEN");
}

#[tokio::test]
async fn v2_frame_preserves_correlation_on_echo() {
    let (mut client, mut server) = tokio::io::duplex(4096);
    let frame = Frame::v2(7, Message::Echo("x".into()));
    protocol::write_frame(&mut client, &frame, WireFormat::V2)
        .await
        .unwrap();
    let (decoded, fmt) = protocol::read_frame(&mut server).await.unwrap();
    assert_eq!(fmt, WireFormat::V2);
    assert_eq!(decoded.correlation_id, 7);
}

async fn run_protocol_server(broker: Arc<Mutex<custom_dmq::broker::Broker>>, port: u16) {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            break;
        };
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            let mut session_handshaken = false;
            loop {
                let Ok((frame, fmt)) = protocol::read_frame(&mut socket).await else {
                    break;
                };
                let response = match &frame.message {
                    Message::Handshake(req) => match client::process_handshake(req) {
                        Ok(version) => {
                            session_handshaken = true;
                            Message::RHandshake(0, version)
                        }
                        Err((code, msg)) => Message::RError(code, msg),
                    },
                    Message::Echo(text) if session_handshaken || !protocol::require_handshake() => {
                        let b = broker.lock().await;
                        Message::REcho(b.process_echo(text))
                    }
                    _ => Message::RError(1, "bad request".into()),
                };
                let out = Frame {
                    correlation_id: frame.correlation_id,
                    message: response,
                };
                if protocol::write_frame(&mut socket, &out, fmt).await.is_err() {
                    break;
                }
                if !matches!(frame.message, Message::Handshake(_)) {
                    break;
                }
            }
        });
    }
}

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
