//! Client configuration with atomic state management
//! 
//! This module provides lock-free configuration management using atomic types
//! for high-performance concurrent access.

use crate::error::ClientError;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Client configuration with atomic state management
#[derive(Debug)]
pub struct Config {
    /// Server base URL (e.g., "https://localhost:8080")
    pub server_url: Arc<RwLock<String>>,
    
    /// Connection timeout in seconds
    pub connect_timeout: AtomicU64,
    
    /// Request timeout in seconds
    pub request_timeout: AtomicU64,
    
    /// Maximum number of retry attempts
    pub max_retries: AtomicU32,
    
    /// Initial retry delay in milliseconds
    pub initial_retry_delay: AtomicU64,
    
    /// Maximum retry delay in milliseconds
    pub max_retry_delay: AtomicU64,
    
    /// Retry backoff multiplier (as integer, divide by 100 for actual value)
    pub retry_multiplier: AtomicU32,
    
    /// Whether to add jitter to retry delays
    pub retry_jitter: AtomicBool,
    
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: AtomicU32,
    
    /// Circuit breaker recovery timeout in seconds
    pub circuit_breaker_timeout: AtomicU64,
    
    /// Connection pool size
    pub pool_size: AtomicU32,
    
    /// Pool idle timeout in seconds
    pub pool_idle_timeout: AtomicU64,
    
    /// Whether to verify TLS certificates
    pub tls_verify: AtomicBool,
    
    /// Maximum file size for single operations (bytes)
    pub max_file_size: AtomicU64,
    
    /// Chunk size for large file operations (bytes)
    pub chunk_size: AtomicU64,
    
    /// Maximum concurrent operations
    pub max_concurrent: AtomicU32,
    
    /// Health check interval in seconds
    pub health_check_interval: AtomicU64,
    
    /// Offline queue maximum size
    pub offline_queue_size: AtomicU32,
    
    /// Whether offline mode is enabled
    pub offline_mode_enabled: AtomicBool,
}

impl Config {
    /// Create a new configuration with default values
    pub fn new(server_url: String) -> Self {
        Self {
            server_url: Arc::new(RwLock::new(server_url)),
            connect_timeout: AtomicU64::new(30),
            request_timeout: AtomicU64::new(300),
            max_retries: AtomicU32::new(5),
            initial_retry_delay: AtomicU64::new(1000),
            max_retry_delay: AtomicU64::new(300000),
            retry_multiplier: AtomicU32::new(200), // 2.0 * 100
            retry_jitter: AtomicBool::new(true),
            circuit_breaker_threshold: AtomicU32::new(5),
            circuit_breaker_timeout: AtomicU64::new(30),
            pool_size: AtomicU32::new(10),
            pool_idle_timeout: AtomicU64::new(90),
            tls_verify: AtomicBool::new(true),
            max_file_size: AtomicU64::new(100 * 1024 * 1024), // 100MB
            chunk_size: AtomicU64::new(1024 * 1024), // 1MB
            max_concurrent: AtomicU32::new(10),
            health_check_interval: AtomicU64::new(30),
            offline_queue_size: AtomicU32::new(1000),
            offline_mode_enabled: AtomicBool::new(true),
        }
    }

    /// Get server URL
    pub async fn server_url(&self) -> String {
        self.server_url.read().await.clone()
    }

    /// Set server URL
    pub async fn set_server_url(&self, url: String) -> Result<(), ClientError> {
        if url.is_empty() {
            return Err(ClientError::Config {
                field: "server_url".to_string(),
                error: "URL cannot be empty".to_string(),
            });
        }
        
        // Validate URL format
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(ClientError::Config {
                field: "server_url".to_string(),
                error: "URL must start with http:// or https://".to_string(),
            });
        }
        
        *self.server_url.write().await = url;
        Ok(())
    }

    /// Get connect timeout as Duration
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout.load(Ordering::Relaxed))
    }

    /// Set connect timeout
    pub fn set_connect_timeout(&self, timeout: Duration) {
        self.connect_timeout.store(timeout.as_secs(), Ordering::Relaxed);
    }

    /// Get request timeout as Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout.load(Ordering::Relaxed))
    }

    /// Set request timeout
    pub fn set_request_timeout(&self, timeout: Duration) {
        self.request_timeout.store(timeout.as_secs(), Ordering::Relaxed);
    }

    /// Get maximum retry attempts
    pub fn max_retries(&self) -> u32 {
        self.max_retries.load(Ordering::Relaxed)
    }

    /// Set maximum retry attempts
    pub fn set_max_retries(&self, retries: u32) {
        self.max_retries.store(retries, Ordering::Relaxed);
    }

    /// Get initial retry delay as Duration
    pub fn initial_retry_delay(&self) -> Duration {
        Duration::from_millis(self.initial_retry_delay.load(Ordering::Relaxed))
    }

    /// Set initial retry delay
    pub fn set_initial_retry_delay(&self, delay: Duration) {
        self.initial_retry_delay.store(delay.as_millis() as u64, Ordering::Relaxed);
    }

    /// Get maximum retry delay as Duration
    pub fn max_retry_delay(&self) -> Duration {
        Duration::from_millis(self.max_retry_delay.load(Ordering::Relaxed))
    }

    /// Set maximum retry delay
    pub fn set_max_retry_delay(&self, delay: Duration) {
        self.max_retry_delay.store(delay.as_millis() as u64, Ordering::Relaxed);
    }

    /// Get retry multiplier as f64
    pub fn retry_multiplier(&self) -> f64 {
        self.retry_multiplier.load(Ordering::Relaxed) as f64 / 100.0
    }

    /// Set retry multiplier
    pub fn set_retry_multiplier(&self, multiplier: f64) {
        self.retry_multiplier.store((multiplier * 100.0) as u32, Ordering::Relaxed);
    }

    /// Check if retry jitter is enabled
    pub fn retry_jitter(&self) -> bool {
        self.retry_jitter.load(Ordering::Relaxed)
    }

    /// Set retry jitter
    pub fn set_retry_jitter(&self, enabled: bool) {
        self.retry_jitter.store(enabled, Ordering::Relaxed);
    }

    /// Get circuit breaker failure threshold
    pub fn circuit_breaker_threshold(&self) -> u32 {
        self.circuit_breaker_threshold.load(Ordering::Relaxed)
    }

    /// Set circuit breaker failure threshold
    pub fn set_circuit_breaker_threshold(&self, threshold: u32) {
        self.circuit_breaker_threshold.store(threshold, Ordering::Relaxed);
    }

    /// Get circuit breaker recovery timeout as Duration
    pub fn circuit_breaker_timeout(&self) -> Duration {
        Duration::from_secs(self.circuit_breaker_timeout.load(Ordering::Relaxed))
    }

    /// Set circuit breaker recovery timeout
    pub fn set_circuit_breaker_timeout(&self, timeout: Duration) {
        self.circuit_breaker_timeout.store(timeout.as_secs(), Ordering::Relaxed);
    }

    /// Get connection pool size
    pub fn pool_size(&self) -> u32 {
        self.pool_size.load(Ordering::Relaxed)
    }

    /// Set connection pool size
    pub fn set_pool_size(&self, size: u32) {
        self.pool_size.store(size, Ordering::Relaxed);
    }

    /// Get pool idle timeout as Duration
    pub fn pool_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.pool_idle_timeout.load(Ordering::Relaxed))
    }

    /// Set pool idle timeout
    pub fn set_pool_idle_timeout(&self, timeout: Duration) {
        self.pool_idle_timeout.store(timeout.as_secs(), Ordering::Relaxed);
    }

    /// Check if TLS verification is enabled
    pub fn tls_verify(&self) -> bool {
        self.tls_verify.load(Ordering::Relaxed)
    }

    /// Set TLS verification
    pub fn set_tls_verify(&self, verify: bool) {
        self.tls_verify.store(verify, Ordering::Relaxed);
    }

    /// Get maximum file size
    pub fn max_file_size(&self) -> u64 {
        self.max_file_size.load(Ordering::Relaxed)
    }

    /// Set maximum file size
    pub fn set_max_file_size(&self, size: u64) {
        self.max_file_size.store(size, Ordering::Relaxed);
    }

    /// Get chunk size for large operations
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size.load(Ordering::Relaxed)
    }

    /// Set chunk size for large operations
    pub fn set_chunk_size(&self, size: u64) {
        self.chunk_size.store(size, Ordering::Relaxed);
    }

    /// Get maximum concurrent operations
    pub fn max_concurrent(&self) -> u32 {
        self.max_concurrent.load(Ordering::Relaxed)
    }

    /// Set maximum concurrent operations
    pub fn set_max_concurrent(&self, concurrent: u32) {
        self.max_concurrent.store(concurrent, Ordering::Relaxed);
    }

    /// Get health check interval as Duration
    pub fn health_check_interval(&self) -> Duration {
        Duration::from_secs(self.health_check_interval.load(Ordering::Relaxed))
    }

    /// Set health check interval
    pub fn set_health_check_interval(&self, interval: Duration) {
        self.health_check_interval.store(interval.as_secs(), Ordering::Relaxed);
    }

    /// Get offline queue maximum size
    pub fn offline_queue_size(&self) -> u32 {
        self.offline_queue_size.load(Ordering::Relaxed)
    }

    /// Set offline queue maximum size
    pub fn set_offline_queue_size(&self, size: u32) {
        self.offline_queue_size.store(size, Ordering::Relaxed);
    }

    /// Check if offline mode is enabled
    pub fn offline_mode_enabled(&self) -> bool {
        self.offline_mode_enabled.load(Ordering::Relaxed)
    }

    /// Set offline mode
    pub fn set_offline_mode_enabled(&self, enabled: bool) {
        self.offline_mode_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Create configuration from environment variables
    pub async fn from_env() -> Result<Self, ClientError> {
        let server_url = std::env::var("VOLEBARN_SERVER_URL")
            .unwrap_or_else(|_| "https://localhost:8080".to_string());

        let config = Self::new(server_url);

        // Load optional environment overrides
        if let Ok(timeout) = std::env::var("VOLEBARN_CONNECT_TIMEOUT") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.set_connect_timeout(Duration::from_secs(secs));
            }
        }

        if let Ok(timeout) = std::env::var("VOLEBARN_REQUEST_TIMEOUT") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.set_request_timeout(Duration::from_secs(secs));
            }
        }

        if let Ok(retries) = std::env::var("VOLEBARN_MAX_RETRIES") {
            if let Ok(count) = retries.parse::<u32>() {
                config.set_max_retries(count);
            }
        }

        if let Ok(verify) = std::env::var("VOLEBARN_TLS_VERIFY") {
            config.set_tls_verify(verify.to_lowercase() != "false");
        }

        if let Ok(size) = std::env::var("VOLEBARN_MAX_FILE_SIZE") {
            if let Ok(bytes) = size.parse::<u64>() {
                config.set_max_file_size(bytes);
            }
        }

        Ok(config)
    }

    /// Validate configuration
    pub async fn validate(&self) -> Result<(), ClientError> {
        let url = self.server_url().await;
        if url.is_empty() {
            return Err(ClientError::Config {
                field: "server_url".to_string(),
                error: "Server URL cannot be empty".to_string(),
            });
        }

        if self.max_retries() > 10 {
            return Err(ClientError::Config {
                field: "max_retries".to_string(),
                error: "Maximum retries cannot exceed 10".to_string(),
            });
        }

        if self.pool_size() == 0 {
            return Err(ClientError::Config {
                field: "pool_size".to_string(),
                error: "Pool size must be greater than 0".to_string(),
            });
        }

        if self.chunk_size() == 0 {
            return Err(ClientError::Config {
                field: "chunk_size".to_string(),
                error: "Chunk size must be greater than 0".to_string(),
            });
        }

        Ok(())
    }
}

impl Clone for Config {
    fn clone(&self) -> Self {
        Self {
            server_url: Arc::new(RwLock::new(
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        self.server_url.read().await.clone()
                    })
                })
            )),
            connect_timeout: AtomicU64::new(self.connect_timeout.load(Ordering::Relaxed)),
            request_timeout: AtomicU64::new(self.request_timeout.load(Ordering::Relaxed)),
            max_retries: AtomicU32::new(self.max_retries.load(Ordering::Relaxed)),
            initial_retry_delay: AtomicU64::new(self.initial_retry_delay.load(Ordering::Relaxed)),
            max_retry_delay: AtomicU64::new(self.max_retry_delay.load(Ordering::Relaxed)),
            retry_multiplier: AtomicU32::new(self.retry_multiplier.load(Ordering::Relaxed)),
            retry_jitter: AtomicBool::new(self.retry_jitter.load(Ordering::Relaxed)),
            circuit_breaker_threshold: AtomicU32::new(self.circuit_breaker_threshold.load(Ordering::Relaxed)),
            circuit_breaker_timeout: AtomicU64::new(self.circuit_breaker_timeout.load(Ordering::Relaxed)),
            pool_size: AtomicU32::new(self.pool_size.load(Ordering::Relaxed)),
            pool_idle_timeout: AtomicU64::new(self.pool_idle_timeout.load(Ordering::Relaxed)),
            tls_verify: AtomicBool::new(self.tls_verify.load(Ordering::Relaxed)),
            max_file_size: AtomicU64::new(self.max_file_size.load(Ordering::Relaxed)),
            chunk_size: AtomicU64::new(self.chunk_size.load(Ordering::Relaxed)),
            max_concurrent: AtomicU32::new(self.max_concurrent.load(Ordering::Relaxed)),
            health_check_interval: AtomicU64::new(self.health_check_interval.load(Ordering::Relaxed)),
            offline_queue_size: AtomicU32::new(self.offline_queue_size.load(Ordering::Relaxed)),
            offline_mode_enabled: AtomicBool::new(self.offline_mode_enabled.load(Ordering::Relaxed)),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new("https://localhost:8080".to_string())
    }
}