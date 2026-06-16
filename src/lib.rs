//! Shared broker, protocol, topic, and consumer-group modules.

pub mod broker;
pub mod cgroup;
pub mod fetch_batch;
pub mod message;
pub mod metadata;
pub mod mmap_queue;
pub mod partition;
pub mod partition_log;
pub mod storage;
pub mod topic;
