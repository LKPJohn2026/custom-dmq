//! Optional TLS for broker and inter-broker TCP connections.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub fn tls_enabled() -> bool {
    std::env::var("DMQ_TLS_CERT")
        .ok()
        .zip(std::env::var("DMQ_TLS_KEY").ok())
        .is_some()
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    keys.into_iter()
        .next()
        .map(PrivateKeyDer::Pkcs8)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no private key found"))
}

pub fn server_config() -> io::Result<Arc<ServerConfig>> {
    let cert_var = std::env::var("DMQ_TLS_CERT")
        .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "DMQ_TLS_CERT unset"))?;
    let key_var = std::env::var("DMQ_TLS_KEY")
        .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "DMQ_TLS_KEY unset"))?;
    let cert_path = Path::new(&cert_var);
    let key_path = Path::new(&key_var);
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Arc::new(config))
}

pub fn client_config() -> io::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    if let Ok(ca_path) = std::env::var("DMQ_TLS_CA") {
        for cert in load_certs(Path::new(&ca_path))? {
            roots.add(cert).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
    } else if let Ok(cert_path) = std::env::var("DMQ_TLS_CERT") {
        for cert in load_certs(Path::new(&cert_path))? {
            roots.add(cert).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

pub async fn accept(stream: TcpStream) -> io::Result<ServerTlsStream<TcpStream>> {
    let config = server_config()?;
    let acceptor = TlsAcceptor::from(config);
    acceptor
        .accept(stream)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

pub async fn connect(addr: &str) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let stream = TcpStream::connect(addr).await?;
    let config = client_config()?;
    let connector = TlsConnector::from(config);
    let host = addr.split(':').next().unwrap_or("localhost");
    let domain = rustls_pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{e:?}")))?;
    connector
        .connect(domain, stream)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}
