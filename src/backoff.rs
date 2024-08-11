use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use anyhow::{anyhow, Result};

use crate::config::MAX_RETRY_DELAY;

const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
const CIRCUIT_BREAKER_RESET_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes


/// A circuit breaker acts as a proxy for operations that might fail. 
/// The proxy should monitor the number of recent or consecutives failures that have occurred, 
/// and use this information to decide whether to allow the operation to proceed, or 
/// simply return an exception immediately.
// Refences: https://learn.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker
pub(crate) struct BackOff {
    consecutive_failures: AtomicU32,
    last_success: std::sync::Mutex<Instant>,
    circuit_open: AtomicBool,
    last_failure: std::sync::Mutex<Option<Instant>>,
    pub(crate) reset_timeout: Duration,
}

impl BackOff {
    /// Creates a new `BackOff` with no failures and the current time as the last success.
    pub(crate) fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            last_success: std::sync::Mutex::new(Instant::now()),
            circuit_open: AtomicBool::new(false),
            last_failure: std::sync::Mutex::new(None),
            reset_timeout: CIRCUIT_BREAKER_RESET_TIMEOUT,
        }
    }

    /// Increments the count of consecutive failures and returns the new count.
    /// Also checks if the circuit breaker should be opened.
    pub(crate) fn record_failure(&self) -> (u32, bool) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        let mut last_failure = self.last_failure.lock().unwrap();
        *last_failure = Some(Instant::now());

        let circuit_opened = if failures >= CIRCUIT_BREAKER_THRESHOLD {
            self.circuit_open.store(true, Ordering::SeqCst);
            true
        } else {
            false
        };

        (failures, circuit_opened)
    }

    /// Calculates the delay before the next attempt based on the number of consecutive failures.
    pub(crate) fn calculate_delay(&self) -> Duration {
        let failures = self.consecutive_failures.load(Ordering::SeqCst);
        let base_delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(MAX_RETRY_DELAY);
        std::cmp::min(base_delay * 2u32.pow(failures), max_delay)
    }

    /// Resets the backoff state to its initial values.
    pub(crate) fn record_success(&self) -> Result<()>{
        self.consecutive_failures.store(0, Ordering::SeqCst);
        let mut last_success = self.last_success.lock().map_err(|e| anyhow!("{}", e))?;
        *last_success = Instant::now();
        self.circuit_open.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Checks if the circuit is open.
    /// If the circuit has been open for longer than the reset timeout, it will attempt to close it.
    pub(crate) fn is_circuit_open(&self) -> bool {
        if self.circuit_open.load(Ordering::SeqCst) {
            let last_failure = self.last_failure.lock().unwrap();
            if let Some(time) = *last_failure {
                if time.elapsed() > self.reset_timeout {
                    self.circuit_open.store(false, Ordering::SeqCst);
                    self.consecutive_failures.store(0, Ordering::SeqCst);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    /// Calculates the time until the circuit breaker will open based on the last failure time
    pub(crate) fn time_until_open(&self) -> Result<Duration> {
        let last_failure = self.last_failure.lock().map_err(|e| anyhow!("unable to lock failure state: {}", e))?;
        if let Some(failure_time) = *last_failure {
            let now = Instant::now();
            let elapsed = now.duration_since(failure_time);
            
            if elapsed >= self.reset_timeout {
                Ok(Duration::from_secs(0))
            } else {
                Ok(self.reset_timeout - elapsed)
            }
        } else {
            Err(anyhow!("No last failure recorded"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_backoff_and_circuit_breaker() {
        let mut backoff = BackOff::new();
        backoff.reset_timeout = Duration::from_millis(300); 

        // Initial state
        assert_eq!(backoff.consecutive_failures.load(Ordering::SeqCst), 0);
        assert!(!backoff.is_circuit_open());

        // Record failures until circuit opens
        for i in 1..=CIRCUIT_BREAKER_THRESHOLD {
            let (failures, circuit_opened) = backoff.record_failure();
            assert_eq!(failures, i);
            assert_eq!(circuit_opened, i == CIRCUIT_BREAKER_THRESHOLD);
        }

        // Check circuit is open
        assert!(backoff.is_circuit_open());

        // Calculate delay
        let delay = backoff.calculate_delay();
        assert_eq!(delay, Duration::from_secs(32)); // 2^5 = 32

        // Record success
        let _ = backoff.record_success();
        assert_eq!(backoff.consecutive_failures.load(Ordering::SeqCst), 0);
        assert!(!backoff.is_circuit_open());

        // Test circuit breaker reset timeout
        for _ in 1..=CIRCUIT_BREAKER_THRESHOLD {
            let _ = backoff.record_failure();
        }
        assert!(backoff.is_circuit_open());
        thread::sleep(Duration::from_millis(150));
        assert!(backoff.is_circuit_open()); // Circuit should still be open
        thread::sleep(Duration::from_millis(151));
        
        assert!(!backoff.is_circuit_open()); // Circuit should auto-close after timeout
        assert_eq!(backoff.consecutive_failures.load(Ordering::SeqCst), 0); // Failures should be reset
    }

    #[test]
    fn test_backoff_without_opening_circuit() {
        let backoff = BackOff::new();

        // Record failures, but not enough to open circuit
        for i in 1..CIRCUIT_BREAKER_THRESHOLD {
            let (failures, circuit_opened) = backoff.record_failure();
            assert_eq!(failures, i);
            assert!(!circuit_opened);
            assert!(!backoff.is_circuit_open());
        }

        // Check delay increases
        let delay = backoff.calculate_delay();
        assert_eq!(delay, Duration::from_secs(16)); // 2^4 = 16

        // Record success
        let _ = backoff.record_success();
        assert_eq!(backoff.consecutive_failures.load(Ordering::SeqCst), 0);
        assert!(!backoff.is_circuit_open());
    }
}