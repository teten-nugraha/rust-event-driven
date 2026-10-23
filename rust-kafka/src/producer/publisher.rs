use std::time::Duration;

use rfkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord },
};

use crate::{
    config::KafkaConfig,
    error::KafkaClientError,
};

use serde::Serialize;
use tracing::{ info};

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {

    pub fn new(
        config: &KafkaConfig
    ) -> Result<Self, KafkaClientError> {
        let producer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("client.id", &config.client_id)
        .set("message.timeout_ms", config.message_timeout_ms.to_string())
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("compression.type", "lz4")
        .create::<FutureProducer>()?;

        Ok(Self { producer })
    }

    pub async fn publish<T: Serialize>(
        &self,
        topic: &str,
        key: Option<&str>,
        event: &T,
    ) -> Result<(), KafkaClientError> {
        let payload = serde_json::to_vec(event)?;

        let record = FutureRecord::to(topic)
        .payload(&payload)
            .key(key);

        self.producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err,_)| KafkaClientError::from)?;

        info!(
            topic = topic,
            key = ?key,
            "Kafka message delivered successfully"
        );

        Ok(())
    }
}