//! Structured request logging for broker operations.

pub fn log_request(request: &str, peer: &str) {
    println!("[broker] request={request} peer={peer}");
}

pub fn log_request_error(request: &str, peer: &str, error: &str) {
    eprintln!("[broker] request={request} peer={peer} error={error}");
}

pub fn request_name(message: &custom_dmq::message::Message) -> &'static str {
    use custom_dmq::message::Message;
    match message {
        Message::Echo(_) => "ECHO",
        Message::ProducerRegister(_) => "P_REG",
        Message::ConsumerRegister(_) => "C_REG",
        Message::Pcm(_) => "PCM",
        Message::Fetch(_) => "FETCH",
        Message::CommitOffset(_) => "COMMIT",
        Message::Produce(_) => "PRODUCE",
        Message::IdempotentProduce(_) => "IDEMPOTENT_PRODUCE",
        Message::CreateTopic(_) => "CREATE_TOPIC",
        Message::DescribeTopic(_) => "DESCRIBE_TOPIC",
        Message::ListTopics => "LIST_TOPICS",
        Message::GetLag(_) => "GET_LAG",
        Message::Replicate(_) => "REPLICATE",
        Message::GetCluster => "GET_CLUSTER",
        Message::REcho(_) => "R_ECHO",
        Message::RProducerRegister(_) => "R_P_REG",
        Message::RConsumerRegister(_) => "R_C_REG",
        Message::RPcm(_) => "R_PCM",
        Message::RFetch(_) => "R_FETCH",
        Message::RCommitOffset(_) => "R_COMMIT",
        Message::RProduce(_) => "R_PRODUCE",
        Message::RCreateTopic(_) => "R_CREATE_TOPIC",
        Message::RDescribeTopic(_) => "R_DESCRIBE_TOPIC",
        Message::RListTopics(_) => "R_LIST_TOPICS",
        Message::RGetLag(_) => "R_GET_LAG",
        Message::RReplicate(_) => "R_REPLICATE",
        Message::RGetCluster(_) => "R_GET_CLUSTER",
        Message::RNotLeader(_) => "R_NOT_LEADER",
    }
}
