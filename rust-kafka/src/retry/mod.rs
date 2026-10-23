pub mod policy;
pub use policy::RetryPolicy;

use std::future::Future;

use tokio::time::sleep;
use tracing::{info, warn};

use crate::error::{KafkaError, KafkaResult};

pub async fn execute_with_retry<F, Fut, T>(policy: &RetryPolicy, mut operation: F) -> KafkaResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = KafkaResult<T>>,
{
    policy.validate().map_err(KafkaError::Configuration)?;

    for attempt in 1..=policy.max_attempts {
        match operation().await {
            Ok(value) => {
                if attempt > 1 {
                    info!(attempt, "Operation succeeded after retry");
                }
                return Ok(value);
            }
            Err(err) => {
                if !err.is_retryable() {
                    warn!(
                        attempt,
                        error = %err,
                        "Non-retryable error"
                    );
                    return Err(err);
                }

                if attempt == policy.max_attempts {
                    return Err(KafkaError::RetryExhausted {
                        attempts: attempt,
                        reason: err.to_string(),
                    });
                }

                let delay = policy.delay_for(attempt);
                warn!(
                    attempt,
                    max_attempts = policy.max_attempts,
                    delay_ms = delay.as_millis() as u64,
                    error = %err,
                    "Retrying operation"
                );

                sleep(delay).await;
            }
        }
    }

    // This point is unreachable because the loop always returns inside,
    // but the compiler needs a value here.
    unreachable!("retry loop should always return before exhausting iterations")
}
