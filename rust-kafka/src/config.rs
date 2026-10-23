#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub client_id: String,
    pub group_id: String,
    pub auto_offset_reset: String,
    pub message_timeout_ms: u64,
}

impl KafkaConfig {
    pub fn new(
        brokers: impl Into<String>,
        client_id: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            client_id: client_id.into(),
            group_id: group_id.into(),
            auto_offset_reset: "earliest".to_string(),
            message_timeout_ms: 0,
        }
    }
}
