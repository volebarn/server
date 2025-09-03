//! Main client implementation with TLS support and resilience
//! 
//! This module provides the main Client struct with comprehensive retry logic,
//! circuit breaker pattern, connection pooling, and TLS 1.3 support.

use crate::config::Config;
use crate::error::{ClientError, ClientResult};
use crate::hash::HashManager;
use crate::retry::{RetryPolicy, HealthMonitor};
use bytes::Bytes;
use reqwest::{ClientBuilder, Method, Response};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::time::{interval, timeout, Instant};
use tracing::{debug, info, warn, instrument};


/// Offline operation for queuing when server is unavailable
#[derive(Debug, Clone)]
pub struct OfflineOperation {
    pub id: String,
    pub operation_type: String,
    pub path: String,
    pub data: Option<Bytes>,
    pub timestamp: SystemTime,
    pub retry_count: u32,
}

/// Client statistics for monitoring
#[derive(Debug, Clone)]
pub struct ClientStats {
    pub requests_sent: u64,
    pub requests_successful: u64,
    pub requests_failed: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub offline_operations_queued: u32,
    pub circuit_breaker_trips: u32,
    pub average_response_time_ms: u64,
}

/// Main client for Volebarn file synchronization
#[derive(Debug)]
pub struct Client {
    /// HTTP client with connection pooling and TLS
    http_client: reqwest::Client,
    /// Client configuration with atomic state
    config: Arc<Config>,
    /// Retry policy with circuit breaker
    retry_policy: Arc<RetryPolicy>,
    /// Health monitoring
    health_monitor: Arc<HealthMonitor>,
    /// Hash manager for integrity verification
    hash_manager: HashManager,
    /// Offline operation queue
    offline_queue: Arc<RwLock<VecDeque<OfflineOperation>>>,
    /// Semaphore for limiting concurrent operations
    concurrency_limiter: Arc<Semaphore>,
    /// Client statistics
    stats: Arc<ClientStats>,
    /// Atomic counters for statistics
    requests_sent: Arc<AtomicU64>,
    requests_successful: Arc<AtomicU64>,
    requests_failed: Arc<AtomicU64>,
    bytes_uploaded: Arc<AtomicU64>,
    bytes_downloaded: Arc<AtomicU64>,
    offline_operations_queued: Arc<AtomicU32>,
    circuit_breaker_trips: Arc<AtomicU32>,
    /// Response time tracking
    total_response_time_ms: Arc<AtomicU64>,
    /// Background task handles
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    /// Shutdown signal
    shutdown_tx: Arc<RwLock<Option<mpsc::Sender<()>>>>,
    /// Whether client is running
    is_running: Arc<AtomicBool>,
}

impl Client {
    /// Create a new client with the given configuration
    pub async fn new(config: Config) -> ClientResult<Self> {
        let config = Arc::new(config);
        config.validate().await?;

        // Build HTTP client with TLS configuration
        let http_client = Self::build_http_client(&config).await?;
        
        // Initialize components
        let retry_policy = Arc::new(RetryPolicy::new(config.clone()));
        let health_monitor = Arc::new(HealthMonitor::new(config.clone()));
        let hash_manager = HashManager::new();
        let offline_queue = Arc::new(RwLock::new(VecDeque::new()));
        let concurrency_limiter = Arc::new(Semaphore::new(config.max_concurrent() as usize));
        
        // Initialize atomic counters
        let requests_sent = Arc::new(AtomicU64::new(0));
        let requests_successful = Arc::new(AtomicU64::new(0));
        let requests_failed = Arc::new(AtomicU64::new(0));
        let bytes_uploaded = Arc::new(AtomicU64::new(0));
        let bytes_downloaded = Arc::new(AtomicU64::new(0));
        let offline_operations_queued = Arc::new(AtomicU32::new(0));
        let circuit_breaker_trips = Arc::new(AtomicU32::new(0));
        let total_response_time_ms = Arc::new(AtomicU64::new(0));

        // Create stats view
        let stats = Arc::new(ClientStats {
            requests_sent: 0,
            requests_successful: 0,
            requests_failed: 0,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            offline_operations_queued: 0,
            circuit_breaker_trips: 0,
            average_response_time_ms: 0,
        });

        let client = Self {
            http_client,
            config,
            retry_policy,
            health_monitor,
            hash_manager,
            offline_queue,
            concurrency_limiter,
            stats,
            requests_sent,
            requests_successful,
            requests_failed,
            bytes_uploaded,
            bytes_downloaded,
            offline_operations_queued,
            circuit_breaker_trips,
            total_response_time_ms,
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown_tx: Arc::new(RwLock::new(None)),
            is_running: Arc::new(AtomicBool::new(false)),
        };

        Ok(client)
    }

    /// Create a client with default configuration
    pub async fn with_defaults(server_url: String) -> ClientResult<Self> {
        let config = Config::new(server_url);
        Self::new(config).await
    }

    /// Create a client from environment variables
    pub async fn from_env() -> ClientResult<Self> {
        let config = Config::from_env().await?;
        Self::new(config).await
    }

    /// Start background tasks (health monitoring, offline queue processing)
    pub async fn start(&self) -> ClientResult<()> {
        if self.is_running.load(Ordering::Acquire) {
            return Ok(());
        }

        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        *self.shutdown_tx.write().await = Some(mpsc::channel(1).0); // Store a dummy sender for compatibility
        self.is_running.store(true, Ordering::Release);

        let mut tasks = self.background_tasks.write().await;

        // Health monitoring task
        let health_task = {
            let client = self.clone_for_background();
            let shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(async move {
                client.health_monitoring_task(shutdown_rx).await;
            })
        };
        tasks.push(health_task);

        // Offline queue processing task
        let offline_task = {
            let client = self.clone_for_background();
            let shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(async move {
                client.offline_queue_processing_task(shutdown_rx).await;
            })
        };
        tasks.push(offline_task);

        info!("Client background tasks started");
        Ok(())
    }

    /// Stop background tasks and cleanup
    pub async fn stop(&self) -> ClientResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            return Ok(());
        }

        self.is_running.store(false, Ordering::Release);

        // Send shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.write().await.take() {
            let _ = shutdown_tx.send(()).await;
        }

        // Wait for background tasks to complete
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            let _ = task.await;
        }

        info!("Client stopped");
        Ok(())
    }

    /// Get current client statistics
    pub async fn stats(&self) -> ClientStats {
        let request_count = self.requests_sent.load(Ordering::Relaxed);
        let total_time = self.total_response_time_ms.load(Ordering::Relaxed);
        let avg_response_time = if request_count > 0 {
            total_time / request_count
        } else {
            0
        };

        ClientStats {
            requests_sent: self.requests_sent.load(Ordering::Relaxed),
            requests_successful: self.requests_successful.load(Ordering::Relaxed),
            requests_failed: self.requests_failed.load(Ordering::Relaxed),
            bytes_uploaded: self.bytes_uploaded.load(Ordering::Relaxed),
            bytes_downloaded: self.bytes_downloaded.load(Ordering::Relaxed),
            offline_operations_queued: self.offline_operations_queued.load(Ordering::Relaxed),
            circuit_breaker_trips: self.circuit_breaker_trips.load(Ordering::Relaxed),
            average_response_time_ms: avg_response_time,
        }
    }

    /// Check server health
    #[instrument(skip(self))]
    pub async fn health_check(&self) -> ClientResult<bool> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        let start_time = Instant::now();
        let result = self.retry_policy.execute(|| async {
            let url = format!("{}/health", self.config.server_url().await);
            let response = timeout(
                self.config.request_timeout(),
                self.http_client.get(&url).send()
            ).await
            .map_err(|_| ClientError::Timeout { 
                duration: self.config.request_timeout().as_secs(),
                operation: "health_check".to_string()
            })?
            .map_err(ClientError::Network)?;

            if response.status().is_success() {
                Ok(true)
            } else {
                Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: "Health check failed".to_string(),
                })
            }
        }).await;

        // Update statistics
        let elapsed = start_time.elapsed().as_millis() as u64;
        self.requests_sent.fetch_add(1, Ordering::Relaxed);
        self.total_response_time_ms.fetch_add(elapsed, Ordering::Relaxed);

        match &result {
            Ok(_) => {
                self.requests_successful.fetch_add(1, Ordering::Relaxed);
                self.health_monitor.record_success();
                debug!("Health check successful");
            }
            Err(e) => {
                self.requests_failed.fetch_add(1, Ordering::Relaxed);
                self.health_monitor.record_failure();
                if matches!(e, ClientError::CircuitBreakerOpen) {
                    self.circuit_breaker_trips.fetch_add(1, Ordering::Relaxed);
                }
                debug!("Health check failed: {}", e);
            }
        }

        result
    }

    /// Test connectivity to server
    pub async fn test_connection(&self) -> ClientResult<Duration> {
        let start = Instant::now();
        self.health_check().await?;
        Ok(start.elapsed())
    }

    /// Get server information
    pub async fn server_info(&self) -> ClientResult<serde_json::Value> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/info", self.config.server_url().await);
            let response = self.make_request(Method::GET, &url, None).await?;
            let info: serde_json::Value = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "server_info".to_string(),
                    error: e.to_string(),
                })?;
            Ok(info)
        }).await
    }

    /// Build HTTP client with TLS configuration and connection pooling
    async fn build_http_client(config: &Config) -> ClientResult<reqwest::Client> {
        let mut builder = ClientBuilder::new()
            .connect_timeout(config.connect_timeout())
            .timeout(config.request_timeout())
            .pool_max_idle_per_host(config.pool_size() as usize)
            .pool_idle_timeout(Some(config.pool_idle_timeout()))
            .tcp_keepalive(Some(Duration::from_secs(60)));

        // Configure TLS
        if config.tls_verify() {
            // Use system root certificates
            builder = builder.use_rustls_tls();
        } else {
            // Disable certificate verification (for development/testing)
            builder = builder
                .use_rustls_tls()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }

        // Build client
        builder.build()
            .map_err(|e| ClientError::Config {
                field: "http_client".to_string(),
                error: e.to_string(),
            })
    }

    /// Make an HTTP request with error handling and statistics tracking
    #[instrument(skip(self, body))]
    async fn make_request(
        &self,
        method: Method,
        url: &str,
        body: Option<Bytes>,
    ) -> ClientResult<Response> {
        let start_time = Instant::now();
        self.requests_sent.fetch_add(1, Ordering::Relaxed);

        let mut request = self.http_client.request(method, url);

        if let Some(body_data) = body {
            self.bytes_uploaded.fetch_add(body_data.len() as u64, Ordering::Relaxed);
            request = request.body(body_data);
        }

        let response = timeout(
            self.config.request_timeout(),
            request.send()
        ).await
        .map_err(|_| ClientError::Timeout {
            duration: self.config.request_timeout().as_secs(),
            operation: "http_request".to_string(),
        })?
        .map_err(ClientError::Network)?;

        // Update statistics
        let elapsed = start_time.elapsed().as_millis() as u64;
        self.total_response_time_ms.fetch_add(elapsed, Ordering::Relaxed);

        if response.status().is_success() {
            self.requests_successful.fetch_add(1, Ordering::Relaxed);
        } else {
            self.requests_failed.fetch_add(1, Ordering::Relaxed);
            return Err(ClientError::Server {
                status: response.status().as_u16(),
                message: response.status().canonical_reason()
                    .unwrap_or("Unknown error").to_string(),
            });
        }

        Ok(response)
    }

    /// Add operation to offline queue
    async fn queue_offline_operation(&self, operation: OfflineOperation) -> ClientResult<()> {
        if !self.config.offline_mode_enabled() {
            return Err(ClientError::Connection {
                error: "Offline mode disabled".to_string(),
            });
        }

        let mut queue = self.offline_queue.write().await;
        let max_size = self.config.offline_queue_size() as usize;

        if queue.len() >= max_size {
            // Remove oldest operation to make space
            queue.pop_front();
            warn!("Offline queue full, removing oldest operation");
        }

        queue.push_back(operation);
        self.offline_operations_queued.fetch_add(1, Ordering::Relaxed);
        debug!("Operation queued for offline processing");
        Ok(())
    }

    /// Health monitoring background task
    async fn health_monitoring_task(&self, mut shutdown_rx: broadcast::Receiver<()>) {
        let mut interval = interval(self.config.health_check_interval());
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.health_check().await {
                        debug!("Background health check failed: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    debug!("Health monitoring task shutting down");
                    break;
                }
            }
        }
    }

    /// Offline queue processing background task
    async fn offline_queue_processing_task(&self, mut shutdown_rx: broadcast::Receiver<()>) {
        let mut interval = interval(Duration::from_secs(30)); // Process every 30 seconds
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.process_offline_queue().await;
                }
                _ = shutdown_rx.recv() => {
                    debug!("Offline queue processing task shutting down");
                    break;
                }
            }
        }
    }

    /// Process queued offline operations
    async fn process_offline_queue(&self) {
        // Check if server is healthy before processing
        if self.health_monitor.is_healthy() != Some(true) {
            return;
        }

        let mut queue = self.offline_queue.write().await;
        let mut operations_to_retry = Vec::new();
        let mut successful_operations = 0;

        // Process operations in batches
        while let Some(operation) = queue.pop_front() {
            // Skip operations that have exceeded retry limit
            if operation.retry_count >= self.config.max_retries() {
                warn!("Dropping offline operation after {} retries: {}", 
                      operation.retry_count, operation.path);
                continue;
            }

            // Try to execute the operation
            let result = match operation.operation_type.as_str() {
                "upload" => {
                    if let Some(_data) = &operation.data {
                        // This would call the actual upload method
                        // For now, just simulate success/failure
                        Ok(())
                    } else {
                        Err(ClientError::LocalFile {
                            path: operation.path.clone(),
                            error: "No data for upload operation".to_string(),
                        })
                    }
                }
                "delete" => {
                    // This would call the actual delete method
                    Ok(())
                }
                _ => {
                    Err(ClientError::InvalidPath {
                        path: operation.path.clone(),
                        reason: format!("Unknown operation type: {}", operation.operation_type),
                    })
                }
            };

            match result {
                Ok(_) => {
                    successful_operations += 1;
                    self.offline_operations_queued.fetch_sub(1, Ordering::Relaxed);
                }
                Err(_) => {
                    // Retry the operation
                    let mut retry_operation = operation;
                    retry_operation.retry_count += 1;
                    operations_to_retry.push(retry_operation);
                }
            }
        }

        // Re-queue failed operations for retry
        for operation in operations_to_retry {
            queue.push_back(operation);
        }

        if successful_operations > 0 {
            info!("Processed {} offline operations successfully", successful_operations);
        }
    }

    /// Clone client for background tasks (lightweight clone)
    fn clone_for_background(&self) -> Self {
        Self {
            http_client: self.http_client.clone(),
            config: self.config.clone(),
            retry_policy: self.retry_policy.clone(),
            health_monitor: self.health_monitor.clone(),
            hash_manager: self.hash_manager.clone(),
            offline_queue: self.offline_queue.clone(),
            concurrency_limiter: self.concurrency_limiter.clone(),
            stats: self.stats.clone(),
            requests_sent: self.requests_sent.clone(),
            requests_successful: self.requests_successful.clone(),
            requests_failed: self.requests_failed.clone(),
            bytes_uploaded: self.bytes_uploaded.clone(),
            bytes_downloaded: self.bytes_downloaded.clone(),
            offline_operations_queued: self.offline_operations_queued.clone(),
            circuit_breaker_trips: self.circuit_breaker_trips.clone(),
            total_response_time_ms: self.total_response_time_ms.clone(),
            background_tasks: self.background_tasks.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Ensure background tasks are stopped when client is dropped
        if self.is_running.load(Ordering::Acquire) {
            // We can't await in Drop, so we just send the shutdown signal
            if let Ok(shutdown_tx) = self.shutdown_tx.try_read() {
                if let Some(tx) = shutdown_tx.as_ref() {
                    let _ = tx.try_send(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_client_creation() {
        let config = Config::new("https://localhost:8080".to_string());
        let client = Client::new(config).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_client_with_defaults() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_client_from_env() {
        // Set environment variable for test
        std::env::set_var("VOLEBARN_SERVER_URL", "https://test.example.com");
        
        let client = Client::from_env().await;
        assert!(client.is_ok());
        
        let client = client.unwrap();
        let server_url = client.config.server_url().await;
        assert_eq!(server_url, "https://test.example.com");
        
        // Clean up
        std::env::remove_var("VOLEBARN_SERVER_URL");
    }

    #[tokio::test]
    async fn test_client_stats() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        let stats = client.stats().await;
        
        assert_eq!(stats.requests_sent, 0);
        assert_eq!(stats.requests_successful, 0);
        assert_eq!(stats.requests_failed, 0);
        assert_eq!(stats.bytes_uploaded, 0);
        assert_eq!(stats.bytes_downloaded, 0);
        assert_eq!(stats.offline_operations_queued, 0);
        assert_eq!(stats.circuit_breaker_trips, 0);
        assert_eq!(stats.average_response_time_ms, 0);
    }

    #[tokio::test]
    async fn test_offline_operation_queue() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        let operation = OfflineOperation {
            id: Uuid::new_v4().to_string(),
            operation_type: "upload".to_string(),
            path: "/test/file.txt".to_string(),
            data: Some(Bytes::from("test data")),
            timestamp: SystemTime::now(),
            retry_count: 0,
        };

        let result = client.queue_offline_operation(operation).await;
        assert!(result.is_ok());
        
        let stats = client.stats().await;
        assert_eq!(stats.offline_operations_queued, 1);
    }

    #[tokio::test]
    async fn test_offline_queue_size_limit() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Set a small queue size for testing
        client.config.set_offline_queue_size(2);
        
        // Add operations up to the limit
        for i in 0..3 {
            let operation = OfflineOperation {
                id: Uuid::new_v4().to_string(),
                operation_type: "upload".to_string(),
                path: format!("/test/file{}.txt", i),
                data: Some(Bytes::from("test data")),
                timestamp: SystemTime::now(),
                retry_count: 0,
            };
            
            let result = client.queue_offline_operation(operation).await;
            assert!(result.is_ok());
        }
        
        // Queue should be limited to 2 items (oldest removed)
        let queue = client.offline_queue.read().await;
        assert_eq!(queue.len(), 2);
    }

    #[tokio::test]
    async fn test_client_start_stop() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        assert!(!client.is_running.load(Ordering::Acquire));
        
        client.start().await.unwrap();
        assert!(client.is_running.load(Ordering::Acquire));
        
        // Wait a bit to ensure tasks are running
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        client.stop().await.unwrap();
        assert!(!client.is_running.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_client_double_start() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // First start should succeed
        let result1 = client.start().await;
        assert!(result1.is_ok());
        assert!(client.is_running.load(Ordering::Acquire));
        
        // Second start should also succeed (no-op)
        let result2 = client.start().await;
        assert!(result2.is_ok());
        assert!(client.is_running.load(Ordering::Acquire));
        
        client.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_stop_without_start() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Stop without start should succeed (no-op)
        let result = client.stop().await;
        assert!(result.is_ok());
        assert!(!client.is_running.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_config_validation() {
        // Test valid config
        let config = Config::new("https://localhost:8080".to_string());
        assert!(config.validate().await.is_ok());
        
        // Test invalid config - empty URL
        let config = Config::new("".to_string());
        assert!(config.validate().await.is_err());
        
        // Test invalid config - too many retries
        let config = Config::new("https://localhost:8080".to_string());
        config.set_max_retries(15);
        assert!(config.validate().await.is_err());
        
        // Test invalid config - zero pool size
        let config = Config::new("https://localhost:8080".to_string());
        config.set_pool_size(0);
        assert!(config.validate().await.is_err());
    }

    #[tokio::test]
    async fn test_config_atomic_operations() {
        let config = Config::new("https://localhost:8080".to_string());
        
        // Test timeout operations
        config.set_connect_timeout(Duration::from_secs(60));
        assert_eq!(config.connect_timeout(), Duration::from_secs(60));
        
        config.set_request_timeout(Duration::from_secs(120));
        assert_eq!(config.request_timeout(), Duration::from_secs(120));
        
        // Test retry operations
        config.set_max_retries(3);
        assert_eq!(config.max_retries(), 3);
        
        config.set_retry_multiplier(1.5);
        assert_eq!(config.retry_multiplier(), 1.5);
        
        config.set_retry_jitter(false);
        assert!(!config.retry_jitter());
        
        // Test TLS operations
        config.set_tls_verify(false);
        assert!(!config.tls_verify());
        
        // Test file size operations
        config.set_max_file_size(50 * 1024 * 1024);
        assert_eq!(config.max_file_size(), 50 * 1024 * 1024);
        
        config.set_chunk_size(512 * 1024);
        assert_eq!(config.chunk_size(), 512 * 1024);
    }

    #[tokio::test]
    async fn test_config_server_url_validation() {
        let config = Config::new("https://localhost:8080".to_string());
        
        // Test valid URLs
        assert!(config.set_server_url("https://example.com".to_string()).await.is_ok());
        assert!(config.set_server_url("http://localhost:3000".to_string()).await.is_ok());
        
        // Test invalid URLs
        assert!(config.set_server_url("".to_string()).await.is_err());
        assert!(config.set_server_url("ftp://example.com".to_string()).await.is_err());
        assert!(config.set_server_url("not-a-url".to_string()).await.is_err());
    }

    #[tokio::test]
    async fn test_http_client_building() {
        let config = Config::new("https://localhost:8080".to_string());
        
        // Test with TLS verification enabled
        config.set_tls_verify(true);
        let client_result = Client::build_http_client(&config).await;
        assert!(client_result.is_ok());
        
        // Test with TLS verification disabled
        config.set_tls_verify(false);
        let client_result = Client::build_http_client(&config).await;
        assert!(client_result.is_ok());
    }

    #[tokio::test]
    async fn test_client_statistics_tracking() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Simulate some statistics
        client.requests_sent.store(10, Ordering::Relaxed);
        client.requests_successful.store(8, Ordering::Relaxed);
        client.requests_failed.store(2, Ordering::Relaxed);
        client.bytes_uploaded.store(1024, Ordering::Relaxed);
        client.bytes_downloaded.store(2048, Ordering::Relaxed);
        client.total_response_time_ms.store(5000, Ordering::Relaxed);
        
        let stats = client.stats().await;
        assert_eq!(stats.requests_sent, 10);
        assert_eq!(stats.requests_successful, 8);
        assert_eq!(stats.requests_failed, 2);
        assert_eq!(stats.bytes_uploaded, 1024);
        assert_eq!(stats.bytes_downloaded, 2048);
        assert_eq!(stats.average_response_time_ms, 500); // 5000 / 10
    }

    #[tokio::test]
    async fn test_offline_mode_disabled() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Disable offline mode
        client.config.set_offline_mode_enabled(false);
        
        let operation = OfflineOperation {
            id: Uuid::new_v4().to_string(),
            operation_type: "upload".to_string(),
            path: "/test/file.txt".to_string(),
            data: Some(Bytes::from("test data")),
            timestamp: SystemTime::now(),
            retry_count: 0,
        };

        let result = client.queue_offline_operation(operation).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ClientError::Connection { .. }));
    }

    #[tokio::test]
    async fn test_concurrency_limiting() {
        let config = Config::new("https://localhost:8080".to_string());
        config.set_max_concurrent(1);
        let client = Client::new(config).await.unwrap();
        
        // The semaphore should have 1 permit
        let permit1 = client.concurrency_limiter.try_acquire();
        assert!(permit1.is_ok());
        
        // Second acquire should fail
        let permit2 = client.concurrency_limiter.try_acquire();
        assert!(permit2.is_err());
        
        // Release first permit
        drop(permit1);
        
        // Now second acquire should succeed
        let permit3 = client.concurrency_limiter.try_acquire();
        assert!(permit3.is_ok());
    }
}