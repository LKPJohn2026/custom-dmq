//! Minimal HTTP server for metrics and operational probes.

use custom_dmq::broker::bind_host;
use custom_dmq::metrics::BrokerMetrics;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub fn metrics_port() -> u16 {
    std::env::var("DMQ_METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9080)
}

pub struct OpsServer {
    metrics: Arc<BrokerMetrics>,
    data_dir: PathBuf,
}

impl OpsServer {
    pub fn new(metrics: Arc<BrokerMetrics>, data_dir: PathBuf) -> Self {
        OpsServer { metrics, data_dir }
    }

    pub async fn run(self) {
        let addr = format!("{}:{}", bind_host(), metrics_port());
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[ops] Could not bind {addr}: {e}");
                return;
            }
        };
        println!("[ops] Listening on {addr} (/metrics /health /ready)");

        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                continue;
            };
            let server = OpsServer {
                metrics: Arc::clone(&self.metrics),
                data_dir: self.data_dir.clone(),
            };
            tokio::spawn(async move {
                server.handle_connection(&mut socket).await;
            });
        }
    }

    async fn handle_connection(&self, socket: &mut tokio::net::TcpStream) {
        let mut buf = [0u8; 1024];
        let n = match socket.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let request = String::from_utf8_lossy(&buf[..n]);
        let path = request.split_whitespace().nth(1).unwrap_or("");
        let (status, content_type, body) = match path {
            "/metrics" => (
                "200 OK",
                "text/plain; version=0.0.4",
                self.metrics.render_prometheus(),
            ),
            "/health" => ("200 OK", "text/plain", "ok".to_string()),
            "/ready" => {
                if self.data_dir.exists() {
                    ("200 OK", "text/plain", "ready".to_string())
                } else {
                    (
                        "503 Service Unavailable",
                        "text/plain",
                        "not ready".to_string(),
                    )
                }
            }
            _ => ("404 Not Found", "text/plain", "not found".to_string()),
        };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
    }
}

pub async fn run_metrics_server(metrics: Arc<BrokerMetrics>, data_dir: PathBuf) {
    OpsServer::new(metrics, data_dir).run().await;
}
