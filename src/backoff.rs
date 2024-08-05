use std::time::{Duration, Instant};
use crate::config::MAX_RETRY_DELAY;

/// Represents the state of backoff attempts.
#[derive(Clone, Copy)]
pub(crate) struct BackOff {
    consecutive_failures: u32,
    last_success: Instant,
}

impl BackOff {
    /// Creates a new `BackOff` with no failures and the current time as the last success.
    pub(crate) fn new() -> Self {
        Self {
            consecutive_failures: 0,
            last_success: Instant::now(),
        }
    } 

    /// Increments the count of consecutive failures and returns the new count.
    pub(crate) fn inc_failure(&mut self) -> u32 {
        self.consecutive_failures += 1;
        self.consecutive_failures
    }

    /// Calculates the delay before the next attempt based on the number of consecutive failures.
    pub(crate) fn calculate_delay(&self) -> Duration {
        let base_delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(MAX_RETRY_DELAY);
        std::cmp::min(base_delay * 2u32.pow(self.consecutive_failures), max_delay)
    }

    /// Resets the backoff state to its initial values.
    pub(crate) fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.last_success = Instant::now();
    }

    /// Returns the time elapsed since the last successful attempt.
    pub(crate) fn time_since_last_success(&self) -> Duration {
        Instant::now().duration_since(self.last_success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff() {
        let mut backoff = BackOff::new();

        // Initial state
        assert_eq!(backoff.consecutive_failures, 0);
        assert!(backoff.time_since_last_success().as_secs() < 1);

        // Record a failure
        backoff.inc_failure();
        assert_eq!(backoff.consecutive_failures, 1);

        // Calculate delay
        let delay = backoff.calculate_delay();
        assert_eq!(delay, Duration::from_secs(2));

        // Record another failure
        backoff.inc_failure();
        assert_eq!(backoff.consecutive_failures, 2);

        // Calculate delay
        let delay = backoff.calculate_delay();
        assert_eq!(delay, Duration::from_secs(4));

        // Reset state
        backoff.reset();
        assert_eq!(backoff.consecutive_failures, 0);
    }
}