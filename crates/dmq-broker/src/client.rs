//! Client connection helpers: optional TLS, protocol handshake, v2 frames.

use dmq_core::auth;
use dmq_protocol::message::{HandshakeRequest, Message};
use dmq_protocol::protocol::{self, Frame, WireFormat};
use std::io;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

pub enum ClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl ClientStream {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        if crate::tls::tls_enabled() {
            Ok(ClientStream::Tls(Box::new(
                crate::tls::connect(addr).await?,
            )))
        } else {
            Ok(ClientStream::Plain(TcpStream::connect(addr).await?))
        }
    }

    pub async fn handshake(&mut self, correlation_id: u32) -> io::Result<u16> {
        let version = protocol::negotiated_version_from_env();
        let token = std::env::var("DMQ_CLIENT_TOKEN")
            .unwrap_or_default()
            .into_bytes();
        let frame = Frame::v2(
            correlation_id,
            Message::Handshake(HandshakeRequest {
                protocol_version: version,
                auth_token: token,
            }),
        );
        self.write_frame(&frame, WireFormat::V2).await?;
        let (resp, _) = self.read_frame().await?;
        match resp.message {
            Message::RHandshake(0, negotiated) => Ok(negotiated),
            Message::RHandshake(code, _) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("handshake rejected code={code}"),
            )),
            Message::RError(code, msg) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("handshake error {code}: {msg}"),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected handshake response: {other:?}"),
            )),
        }
    }

    pub async fn write_message(&mut self, message: &Message) -> io::Result<()> {
        self.write_frame(&Frame::v1(message.clone()), WireFormat::V1)
            .await
    }

    pub async fn write_v2(&mut self, correlation_id: u32, message: &Message) -> io::Result<()> {
        self.write_frame(&Frame::v2(correlation_id, message.clone()), WireFormat::V2)
            .await
    }

    pub async fn read_message(&mut self) -> io::Result<Message> {
        let (frame, _) = self.read_frame().await?;
        Ok(frame.message)
    }

    pub async fn read_frame(&mut self) -> io::Result<(Frame, WireFormat)> {
        match self {
            ClientStream::Plain(s) => protocol::read_frame(s).await,
            ClientStream::Tls(s) => protocol::read_frame(s).await,
        }
    }

    async fn write_frame(&mut self, frame: &Frame, format: WireFormat) -> io::Result<()> {
        match self {
            ClientStream::Plain(s) => protocol::write_frame(s, frame, format).await,
            ClientStream::Tls(s) => protocol::write_frame(s, frame, format).await,
        }
    }
}

pub async fn connect_and_handshake(addr: &str) -> io::Result<(ClientStream, u16)> {
    let mut stream = ClientStream::connect(addr).await?;
    if protocol::negotiated_version_from_env() >= protocol::PROTOCOL_V2 {
        let version = stream.handshake(1).await?;
        Ok((stream, version))
    } else {
        Ok((stream, protocol::PROTOCOL_V1))
    }
}

pub fn process_handshake(req: &HandshakeRequest) -> Result<u16, (u8, String)> {
    if let Err(e) = auth::validate_token(&req.auth_token) {
        return Err((2, e.to_string()));
    }
    protocol::validate_protocol_version(req.protocol_version).map_err(|code| {
        (
            code,
            format!(
                "unsupported protocol version {}; max supported {}",
                req.protocol_version,
                protocol::MAX_PROTOCOL_VERSION
            ),
        )
    })
}
