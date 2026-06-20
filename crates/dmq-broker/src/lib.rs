//! Broker runtime: replication, coordinator, client, TLS.

pub mod broker;
pub mod client;
pub mod coordinator;
pub mod replication;
pub mod tls;

// Re-export sub-crates for integration tests and CLI.
pub use dmq_core::*;
pub use dmq_protocol::*;
pub use dmq_storage::*;
