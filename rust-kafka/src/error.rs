use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("Kafka connection error: {0}")]
    Connection(String),

    #[error("Kafka producer error: {0}")]
    Producer(String),

    #[error("Kafka consumer error: {0}")]
    Consumer(String),

    #[error("Kafka serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Kafka configuration error: {0}")]
    Configuration(String),

    #[error("Kafka operation timeout error: {0}")]
    Timeout(String),

    #[error("Kafka retry exhaust after {attempts} attempts : {reason} ")]
    RetryExhausted { attempts: u32, reason: String },

    #[error("Kafka dead letter queue error: {0}")]
    DeadLetterQueue(String),

    #[error("Kafka handler error: {0}")]
    Handler(String),

    #[error("Kafka idempotence error: {0}")]
    Idempotence(String),

    #[error("Kafka internal timeout error: {0}")]
    Internal(String),
}

pub type KafkaResult<T> = Result<T, KafkaError>;

impl From<rdkafka::error::KafkaError> for KafkaError {
    fn from(e: rdkafka::error::KafkaError) -> Self {
        Self::Internal(e.to_string())
    }
}

impl KafkaError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Connection(_) | Self::Timeout(_) | Self::Handler(_)
        )
    }
}
