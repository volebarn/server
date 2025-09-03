//! Retry logic and circuit breaker implementation
//! 
//! This module provides comprehensive retry logic with exponential backoff, jitter,
//! and circuit breaker pattern using atomic operations for lock-free performance.

use crate::config::Config;
use crate::error::ClientError;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{debug, warn, error};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed = 0,   // Normal operation
    Open = 1,     // Failing fast
    HalfOpen = 2, // Testing recovery
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Circuit breaker for handling cascading failures
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Current state of the circuit breaker
    state: AtomicU8,
    /// Number of consecutive failures
    failure_count: AtomicU32,
    /// Timestamp of last failure (seconds since epoch)
    last_failure_time: AtomicU64,
    /// Configuration reference
    config: Arc<Config>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU32::new(0),
            last_failure_time: AtomicU64::new(0),
            config,
        }
    }

    /// Check if a request can proceed
    pub async fn can_proceed(&self) -> Result<(), ClientError> {
        let current_state = CircuitState::from(self.state.load(Ordering::Acquire));
        
        match current_state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open => {
                // Check if enough time has passed to attempt recovery
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let last_failure = self.last_failure_time.load(Ordering::Acquire);
                let recovery_timeout = self.config.circuit_breaker_timeout().as_secs();
                
                if now.saturating_sub(last_failure) >= recovery_timeout {
                    // Transition to half-open for testing
                    self.state.store(CircuitState::HalfOpen as u8, Ordering::Release);
                    debug!("Circuit breaker transitioning to half-open state");
                    Ok(())
                } else {
                    Err(ClientError::CircuitBreakerOpen)
                }
            }
            CircuitState::HalfOpen => Ok(()), // Allow single test request
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let current_state = CircuitState::from(self.state.load(Ordering::Acquire));
        
        match current_state {
            CircuitState::HalfOpen => {
                // Recovery successful, transition to closed
                self.state.store(CircuitState::Closed as u8, Ordering::Release);
                self.failure_count.store(0, Ordering::Release);
                debug!("Circuit breaker recovered, transitioning to closed state");
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Release);
            }
            CircuitState::Open => {
                // Should not happen, but reset if it does
                self.failure_count.store(0, Ordering::Release);
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self, error: &ClientError) {
        // Only count failures that should trigger circuit breaker
        if !error.should_trigger_circuit_breaker() {
            return;
        }

        let current_state = CircuitState::from(self.state.load(Ordering::Acquire));
        let failure_count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
        let threshold = self.config.circuit_breaker_threshold();
        
        // Update last failure time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_failure_time.store(now, Ordering::Release);

        match current_state {
            CircuitState::Closed => {
                if failure_count >= threshold {
                    self.state.store(CircuitState::Open as u8, Ordering::Release);
                    warn!("Circuit breaker opened after {} failures", failure_count);
                }
            }
            CircuitState::HalfOpen => {
                // Test failed, go back to open
                self.state.store(CircuitState::Open as u8, Ordering::Release);
                warn!("Circuit breaker test failed, returning to open state");
            }
            CircuitState::Open => {
                // Already open, just update failure time
            }
        }
    }

    /// Get current state
    pub fn state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::Acquire))
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Acquire)
    }

    /// Reset the circuit breaker to closed state
    pub fn reset(&self) {
        self.state.store(CircuitState::Closed as u8, Ordering::Release);
        self.failure_count.store(0, Ordering::Release);
        self.last_failure_time.store(0, Ordering::Release);
        debug!("Circuit breaker manually reset");
    }
}

/// Retry policy for handling transient failures
#[derive(Debug)]
pub struct RetryPolicy {
    config: Arc<Config>,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new(config: Arc<Config>) -> Self {
        let circuit_breaker = Arc::new(CircuitBreaker::new(config.clone()));
        Self {
            config,
            circuit_breaker,
        }
    }

    /// Execute an operation with retry logic
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, ClientError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        let max_attempts = self.config.max_retries() + 1; // +1 for initial attempt
        let mut last_error = None;

        for attempt in 0..max_attempts {
            // Check circuit breaker before each attempt
            if let Err(e) = self.circuit_breaker.can_proceed().await {
                return Err(e);
            }

            // Execute the operation
            match operation().await {
                Ok(result) => {
                    // Success - record it and return
                    self.circuit_breaker.record_success();
                    if attempt > 0 {
                        debug!("Operation succeeded after {} retries", attempt);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    // Record failure for circuit breaker
                    self.circuit_breaker.record_failure(&error);
                    
                    // Check if this error is retryable
                    if !error.is_retryable() {
                        debug!("Non-retryable error: {}", error);
                        return Err(error);
                    }

                    last_error = Some(error.clone());

                    // Don't sleep after the last attempt
                    if attempt < max_attempts - 1 {
                        let delay = self.calculate_delay(attempt, &error);
                        debug!("Attempt {} failed, retrying in {:?}: {}", attempt + 1, delay, error);
                        sleep(delay).await;
                    } else {
                        error!("All {} attempts failed, giving up", max_attempts);
                    }
                }
            }
        }

        // All attempts failed
        Err(last_error.unwrap_or(ClientError::RetryLimitExceeded { 
            attempts: max_attempts 
        }))
    }

    /// Calculate delay for exponential backoff with jitter
    fn calculate_delay(&self, attempt: u32, error: &ClientError) -> Duration {
        let base_delay = self.config.initial_retry_delay();
        let max_delay = self.config.max_retry_delay();
        let multiplier = self.config.retry_multiplier();
        let jitter_enabled = self.config.retry_jitter();

        // Use error-specific delay if available
        let error_delay = Duration::from_secs(error.retry_delay(attempt));
        if error_delay > Duration::ZERO {
            return error_delay.min(max_delay);
        }

        // Calculate exponential backoff
        let exponential_delay = base_delay.as_millis() as f64 * multiplier.powi(attempt as i32);
        let mut delay = Duration::from_millis(exponential_delay as u64).min(max_delay);

        // Add jitter to prevent thundering herd
        if jitter_enabled {
            let jitter_range = delay.as_millis() as u64 / 4; // 25% jitter
            if jitter_range > 0 {
                let jitter = fastrand::u64(0..=jitter_range);
                delay = delay.saturating_add(Duration::from_millis(jitter));
            }
        }

        delay.min(max_delay)
    }

    /// Get circuit breaker reference
    pub fn circuit_breaker(&self) -> Arc<CircuitBreaker> {
        self.circuit_breaker.clone()
    }

    /// Execute with custom retry configuration
    pub async fn execute_with_config<F, Fut, T>(
        &self,
        operation: F,
        max_attempts: u32,
        base_delay: Duration,
    ) -> Result<T, ClientError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        let mut last_error = None;

        for attempt in 0..max_attempts {
            // Check circuit breaker before each attempt
            if let Err(e) = self.circuit_breaker.can_proceed().await {
                return Err(e);
            }

            match operation().await {
                Ok(result) => {
                    self.circuit_breaker.record_success();
                    if attempt > 0 {
                        debug!("Custom retry operation succeeded after {} attempts", attempt);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    self.circuit_breaker.record_failure(&error);
                    
                    if !error.is_retryable() {
                        return Err(error);
                    }

                    last_error = Some(error.clone());

                    if attempt < max_attempts - 1 {
                        let delay = base_delay * (2_u32.pow(attempt));
                        debug!("Custom retry attempt {} failed, retrying in {:?}", attempt + 1, delay);
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or(ClientError::RetryLimitExceeded { 
            attempts: max_attempts 
        }))
    }
}

/// Health monitor for tracking service availability
#[derive(Debug)]
pub struct HealthMonitor {
    /// Number of consecutive successful health checks
    success_count: AtomicU32,
    /// Number of consecutive failed health checks
    failure_count: AtomicU32,
    /// Timestamp of last health check
    last_check_time: AtomicU64,
    /// Whether the service is currently healthy
    is_healthy: AtomicU8, // 0 = unknown, 1 = healthy, 2 = unhealthy
    /// Configuration reference
    config: Arc<Config>,
}

impl HealthMonitor {
    /// Create a new health monitor
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            success_count: AtomicU32::new(0),
            failure_count: AtomicU32::new(0),
            last_check_time: AtomicU64::new(0),
            is_healthy: AtomicU8::new(0), // Unknown initially
            config,
        }
    }

    /// Record a successful health check
    pub fn record_success(&self) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.failure_count.store(0, Ordering::Relaxed);
        self.is_healthy.store(1, Ordering::Release);
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_check_time.store(now, Ordering::Release);
    }

    /// Record a failed health check
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
        self.is_healthy.store(2, Ordering::Release);
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_check_time.store(now, Ordering::Release);
    }

    /// Check if the service is healthy
    pub fn is_healthy(&self) -> Option<bool> {
        match self.is_healthy.load(Ordering::Acquire) {
            1 => Some(true),
            2 => Some(false),
            _ => None, // Unknown
        }
    }

    /// Get time since last health check
    pub fn time_since_last_check(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last_check = self.last_check_time.load(Ordering::Acquire);
        Duration::from_secs(now.saturating_sub(last_check))
    }

    /// Check if health check is overdue
    pub fn is_check_overdue(&self) -> bool {
        self.time_since_last_check() > self.config.health_check_interval() * 2
    }

    /// Get current success count
    pub fn success_count(&self) -> u32 {
        self.success_count.load(Ordering::Relaxed)
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Reset health status
    pub fn reset(&self) {
        self.success_count.store(0, Ordering::Relaxed);
        self.failure_count.store(0, Ordering::Relaxed);
        self.is_healthy.store(0, Ordering::Release);
        self.last_check_time.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_circuit_breaker_states() {
        let config = Arc::new(Config::default());
        config.set_circuit_breaker_threshold(2);
        let cb = CircuitBreaker::new(config);

        // Initially closed
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.can_proceed().await.is_ok());

        // Record failures
        let error = ClientError::Connection { 
            error: "Connection refused".to_string() 
        };
        
        cb.record_failure(&error);
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.failure_count(), 1);

        cb.record_failure(&error);
        assert_eq!(cb.state(), CircuitState::Open);
        assert_eq!(cb.failure_count(), 2);

        // Should fail fast when open
        assert!(matches!(cb.can_proceed().await, Err(ClientError::CircuitBreakerOpen)));
    }

    #[tokio::test]
    async fn test_retry_policy_success() {
        let config = Arc::new(Config::default());
        config.set_max_retries(3);
        let policy = RetryPolicy::new(config);

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result = policy.execute(|| {
            let count = call_count_clone.fetch_add(1, Ordering::Relaxed) + 1;
            async move {
                if count < 3 {
                    Err(ClientError::Connection { 
                        error: "Connection refused".to_string() 
                    })
                } else {
                    Ok("success")
                }
            }
        }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(call_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_policy_failure() {
        let config = Arc::new(Config::default());
        config.set_max_retries(2);
        config.set_initial_retry_delay(Duration::from_millis(1)); // Fast test
        let policy = RetryPolicy::new(config);

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<&str, ClientError> = policy.execute(|| {
            call_count_clone.fetch_add(1, Ordering::Relaxed);
            async move {
                Err(ClientError::Connection { 
                    error: "Connection refused".to_string() 
                })
            }
        }).await;

        assert!(result.is_err());
        assert_eq!(call_count.load(Ordering::Relaxed), 3); // 1 initial + 2 retries
    }

    #[tokio::test]
    async fn test_health_monitor() {
        let config = Arc::new(Config::default());
        let monitor = HealthMonitor::new(config);

        // Initially unknown
        assert_eq!(monitor.is_healthy(), None);

        // Record success
        monitor.record_success();
        assert_eq!(monitor.is_healthy(), Some(true));
        assert_eq!(monitor.success_count(), 1);
        assert_eq!(monitor.failure_count(), 0);

        // Record failure
        monitor.record_failure();
        assert_eq!(monitor.is_healthy(), Some(false));
        assert_eq!(monitor.success_count(), 0);
        assert_eq!(monitor.failure_count(), 1);
    }

    #[test]
    fn test_delay_calculation() {
        let config = Arc::new(Config::default());
        config.set_initial_retry_delay(Duration::from_millis(100));
        config.set_retry_multiplier(2.0);
        config.set_retry_jitter(false); // Disable for predictable testing
        
        let policy = RetryPolicy::new(config);
        
        // Use a config error which has a base delay of 1 second in retry_delay
        let error = ClientError::Config { 
            field: "test".to_string(),
            error: "test error".to_string()
        };

        let delay0 = policy.calculate_delay(0, &error);
        let delay1 = policy.calculate_delay(1, &error);
        let delay2 = policy.calculate_delay(2, &error);

        // Config errors have base delay of 1 second from retry_delay method
        // So expect 1s, 2s, 4s (with potential jitter)
        assert!(delay0 >= Duration::from_secs(1) && delay0 <= Duration::from_secs(2));
        assert!(delay1 >= Duration::from_secs(2) && delay1 <= Duration::from_secs(3));
        assert!(delay2 >= Duration::from_secs(4) && delay2 <= Duration::from_secs(5));
    }
}