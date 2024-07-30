use std::time::{Duration, Instant};
use crate::config::MAX_RETRY_DELAY;

/// Represents the state of retry attempts.
#[derive(Clone, Copy)]
pub(crate) struct RetryState {
    consecutive_failures: u32,
    last_success: Instant,
}

impl RetryState {
    /// Creates a new `RetryState` with no failures and the current time as the last success.
    pub(crate) fn new() -> Self {
        Self {
            consecutive_failures: 0,
            last_success: Instant::now(),
        }
    }

    /// Increments the count of consecutive failures and returns the new count.
    pub(crate) fn record_failure(&mut self) -> u32 {
        self.consecutive_failures += 1;
        self.consecutive_failures
    }

    /// Records a successful attempt, resetting the failure count and updating the last success time.
    pub(crate) fn record_success(&mut self) {
        self.reset()
    }

    /// Calculates the delay before the next retry attempt based on the number of consecutive failures.
    pub(crate) fn calculate_delay(&self) -> Duration {
        let base_delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(MAX_RETRY_DELAY);
        std::cmp::min(base_delay * 2u32.pow(self.consecutive_failures), max_delay)
    }

    /// Resets the retry state to its initial values.
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
    fn test_retry_state() {
        let mut retry_state = RetryState::new();

        // Initial state
        assert_eq!(retry_state.consecutive_failures, 0);
        assert!(retry_state.time_since_last_success().as_secs() < 1);

        // Record a failure
        retry_state.record_failure();
        assert_eq!(retry_state.consecutive_failures, 1);

        // Calculate delay
        let delay = retry_state.calculate_delay();
        assert_eq!(delay, Duration::from_secs(2));

        // Record another failure
        retry_state.record_failure();
        assert_eq!(retry_state.consecutive_failures, 2);

        // Calculate delay
        let delay = retry_state.calculate_delay();
        assert_eq!(delay, Duration::from_secs(4));

        // Record success
        retry_state.record_success();
        assert_eq!(retry_state.consecutive_failures, 0);
        assert!(retry_state.time_since_last_success().as_secs() < 1);

        // Reset state
        retry_state.reset();
        assert_eq!(retry_state.consecutive_failures, 0);
    }
}
