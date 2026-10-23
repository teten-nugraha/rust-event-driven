use std::future::Future;

use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
};
use serde::de::DeserializeOwned;
use tokio::signal;
use tracing::{debug, error, info};

use crate::{
    config::KafkaConfig,
    error::{KafkaError, KafkaResult},
};

use crate::retry::{RetryPolicy, execute_with_retry};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    retry_policy: RetryPolicy,
}

impl KafkaConsumer {
    pub fn new(config: KafkaConfig) -> KafkaResult<Self> {
        let group_id = config
            .group_id
            .ok_or_else(|| KafkaError::Configuration("group id must be provided".into()))?;

        if config.bootstrap_servers.is_empty() {
            return Err(KafkaError::Configuration(
                "bootstrap servers must be provided".into(),
            ));
        }

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("client.id", &config.client_id)
            .set("group.id", &group_id)
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .create()
            .map_err(|err| KafkaError::Consumer(err.to_string()))?;

        info!(
            client_id = %config.client_id,
            group_id = %group_id,
            "Kafka consumer initialized"
        );

        Ok(Self {
            consumer,
            retry_policy: RetryPolicy::default(),
        })
    }

    pub async fn subscribe<T, F, Fut>(&self, topics: &[&str], mut handler: F) -> KafkaResult<()>
    where
        T: DeserializeOwned + Clone,
        F: FnMut(T) -> Fut,
        Fut: Future<Output = KafkaResult<()>>,
    {
        if topics.is_empty() {
            return Err(KafkaError::Configuration("topics must be provided".into()));
        }

        self.consumer
            .subscribe(topics)
            .map_err(|err| KafkaError::Consumer(err.to_string()))?;

        info!(?topics, "Kafka consumer subscribed to topics");

        loop {
            tokio::select! {
                result = self.consumer.recv() => {
                    let message = result.map_err(|e| {
                        KafkaError::Consumer(e.to_string())
                    })?;

                    let topic = message.topic();
                    let partition = message.partition();
                    let offset = message.offset();

                    debug!(
                        topic,
                        partition,
                        offset,
                        "Kafka message received"
                    );

                    let payload = message.payload().ok_or_else(|| {
                        KafkaError::Consumer(format!("Empty payload at {topic}:{partition}:{offset}"))
                    })?;

                    let event: T = serde_json::from_slice(payload)?;

                    // Business logic is executed by client service
                    // Clone event so retry closure can reuse it if first attempt fails
                    let event_clone = event.clone();
                    let result = execute_with_retry(&self.retry_policy, || {
                        handler(event_clone.clone())
                    })
                    .await;

                    if let Err(err) = result {
                        error!(
                            topic,
                            partition,
                            offset,
                            error = %err,
                            "Kafka processing failed after retry"
                        );

                        // Don't commit offset — DLQ will be added later
                        return Err(err);
                    }


                    // Commit only after successfully processing
                    self.consumer
                        .commit_message(&message, CommitMode::Sync)
                        .map_err(|err| KafkaError::Consumer(err.to_string()))?;

                    debug!(
                        topic,
                        partition,
                        offset,
                        "Kafka offset committed"
                    );
                }

                _ = signal::ctrl_c() => {
                    info!("Kafka consumer shutting down");
                    break;
                }
            }
        } // end loop

        Ok(())
    }
}
