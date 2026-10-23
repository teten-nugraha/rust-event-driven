use std::time::Duration;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub group_id: Option<String>,
    pub auto_offset_reset: String,
    pub message_timeout_ms: Duration,

    // Producer Settings
    pub producer_acks: String,
    pub enable_idempotence: bool,
    pub compression_type: String,
}

impl KafkaConfig {
    pub fn new(bootstrap_servers: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            client_id: client_id.into(),
            group_id: None,
            auto_offset_reset: "earliest".to_string(),
            message_timeout_ms: Duration::from_secs(30),
            producer_acks: "all".into(),
            enable_idempotence: true,
            compression_type: "lz4".into(),
        }
    }

    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }
}
