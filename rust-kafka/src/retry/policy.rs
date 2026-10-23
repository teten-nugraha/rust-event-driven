use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    pub fn delay_for(&self, failed_attempt: u32) -> Duration {
        let exponent = failed_attempt.saturating_sub(1) as i32;

        let delay = self.initial_delay.as_secs_f64() * self.multiplier.powi(exponent);

        Duration::from_secs_f64(delay.min(self.max_delay.as_secs_f64()))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.max_attempts == 0 {
            return Err("max_attempts must be positive".into());
        }

        if self.multiplier.is_finite() && self.multiplier < 1.0 {
            return Err("multiplier must be greater than 1.0".to_string());
        }

        Ok(())
    }
}
