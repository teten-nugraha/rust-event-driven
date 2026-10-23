use std::time::Duration;

use crate::{
    config::KafkaConfig,
    error::{KafkaError, KafkaResult},
};
use log::debug;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
    queue_timeout: Duration,
}

impl KafkaProducer {
    pub fn new(config: &KafkaConfig) -> KafkaResult<Self> {
        if config.bootstrap_servers.trim().is_empty() {
            return Err(KafkaError::Configuration(
                "bootstrap_servers cannot be empty".into(),
            ));
        }
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("client.id", &config.client_id)
            .set("acks", &config.producer_acks)
            .set("enable.idempotence", config.enable_idempotence.to_string())
            .set("compression.type", &config.compression_type.to_string())
            .set(
                "message.timeout_ms",
                config.message_timeout_ms.as_millis().to_string(),
            )
            .create()
            .map_err(|e| KafkaError::Producer(e.to_string()))?;

        info!("Kafka producer initialized: {}", config.client_id);

        Ok(Self {
            producer,
            queue_timeout: Duration::from_secs(5),
        })
    }

    pub async fn publish<T: Serialize>(
        &self,
        topic: &str,
        key: &str,
        payload: &T,
    ) -> KafkaResult<()>
    where
        T: Serialize,
    {
        let serialized = serde_json::to_vec(payload)?;

        let record = FutureRecord::to(topic).key(key).payload(&serialized);

        match self.producer.send(record, self.queue_timeout).await {
            Ok(delivery) => {
                debug!(
                    topic = topic,
                    key = key,
                    delivery = ?delivery,
                    "Kafka message delivered"
                );

                Ok(())
            }
            Err((err, message)) => {
                error!(
                    topic = topic,
                    key = key,
                    error = ?err,
                    "Kafka message delivery failed"
                );

                Err(KafkaError::Producer(err.to_string()))
            }
        }
    }
}
