/// Represents all commands a client can send over TCP.
///
/// Protocol format (plain text, newline-terminated):
///   CREATE_TOPIC <topic>
///   REGISTER_PRODUCER <topic>
///   PRODUCE <topic> <message>
///   CONSUME <topic> <group>
#[derive(Debug)]
pub enum Command {
    CreateTopic(String),
    RegisterProducer(String),
    Produce { topic: String, message: String },
    Consume { topic: String, group: String },
    Unknown(String),
}

impl Command {
    pub fn parse(input: &str) -> Self {
        // Split into at most 3 parts so message payloads with spaces are preserved
        let parts: Vec<&str> = input.trim().splitn(3, ' ').collect();

        match parts.as_slice() {
            ["CREATE_TOPIC", topic] => Command::CreateTopic(topic.to_string()),

            ["REGISTER_PRODUCER", topic] => Command::RegisterProducer(topic.to_string()),

            ["PRODUCE", topic, message] => Command::Produce {
                topic: topic.to_string(),
                message: message.to_string(),
            },

            ["CONSUME", topic, group] => Command::Consume {
                topic: topic.to_string(),
                group: group.to_string(),
            },

            _ => Command::Unknown(input.to_string()),
        }
    }
}
