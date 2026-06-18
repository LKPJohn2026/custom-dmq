//! Minimal HTTP server exposing Prometheus metrics.

use custom_dmq::metrics::BrokerMetrics;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub fn metrics_port() -> u16 {
    std::env::var("DMQ_METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9080)
}

pub async fn run_metrics_server(metrics: Arc<BrokerMetrics>) {
    let addr = format!("127.0.0.1:{}", metrics_port());
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[metrics] Could not bind {addr}: {e}");
            return;
        }
    };
    println!("[metrics] Listening on {addr}");

    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = match socket.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let request = String::from_utf8_lossy(&buf[..n]);
            let body = if request.starts_with("GET /metrics") {
                metrics.render_prometheus()
            } else {
                "not found".to_string()
            };
            let status = if request.starts_with("GET /metrics") {
                "200 OK"
            } else {
                "404 Not Found"
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}
