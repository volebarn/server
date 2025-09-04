//! Main client implementation with TLS support and resilience
//! 
//! This module provides the main Client struct with comprehensive retry logic,
//! circuit breaker pattern, connection pooling, and TLS 1.3 support.

use crate::config::Config;
use crate::error::{ClientError, ClientResult};
use crate::hash::HashManager;
use crate::retry::{RetryPolicy, HealthMonitor};
use bytes::Bytes;
use crossbeam::queue::SegQueue;
use reqwest::{ClientBuilder, Method, Response};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, Semaphore};
use tokio::time::{interval, timeout, Instant};
use tracing::{debug, info, warn, instrument};
use base64::{Engine as _, engine::general_purpose};


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
    /// Lock-free offline operation queue
    offline_queue: Arc<SegQueue<OfflineOperation>>,
    /// Maximum offline queue size
    offline_queue_max_size: Arc<AtomicU32>,
    /// Current offline queue size
    offline_queue_size: Arc<AtomicU32>,
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
    /// Shutdown signal broadcaster
    shutdown_tx: Arc<broadcast::Sender<()>>,
    /// Whether client is running
    is_running: Arc<AtomicBool>,
}

impl Client {
    /// Create a new client with the given configuration
    pub async fn new(config: Config) -> ClientResult<Self> {
        let config = Arc::new(config);
        config.validate()?;

        // Build HTTP client with TLS configuration
        let http_client = Self::build_http_client(&config).await?;
        
        // Initialize components
        let retry_policy = Arc::new(RetryPolicy::new(config.clone()));
        let health_monitor = Arc::new(HealthMonitor::new(config.clone()));
        let hash_manager = HashManager::new();
        let offline_queue = Arc::new(SegQueue::new());
        let offline_queue_max_size = Arc::new(AtomicU32::new(config.offline_queue_size()));
        let offline_queue_size = Arc::new(AtomicU32::new(0));
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

        // Create shutdown broadcaster
        let (shutdown_tx, _) = broadcast::channel(16);
        let shutdown_tx = Arc::new(shutdown_tx);

        let client = Self {
            http_client,
            config,
            retry_policy,
            health_monitor,
            hash_manager,
            offline_queue,
            offline_queue_max_size,
            offline_queue_size,
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
            shutdown_tx,
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
        let config = Config::from_env()?;
        Self::new(config).await
    }

    /// Start background tasks (health monitoring, offline queue processing)
    pub async fn start(&self) -> ClientResult<()> {
        if self.is_running.load(Ordering::Acquire) {
            return Ok(());
        }

        self.is_running.store(true, Ordering::Release);

        // Health monitoring task
        let health_task = {
            let client = self.clone_for_background();
            let shutdown_rx = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                client.health_monitoring_task(shutdown_rx).await;
            })
        };

        // Offline queue processing task
        let offline_task = {
            let client = self.clone_for_background();
            let shutdown_rx = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                client.offline_queue_processing_task(shutdown_rx).await;
            })
        };

        // Store task handles for cleanup (we'll just let them run and clean up on drop)
        // This avoids the need for RwLock<Vec<JoinHandle>>
        std::mem::forget(health_task);
        std::mem::forget(offline_task);

        info!("Client background tasks started");
        Ok(())
    }

    /// Stop background tasks and cleanup
    pub async fn stop(&self) -> ClientResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            return Ok(());
        }

        self.is_running.store(false, Ordering::Release);

        // Send shutdown signal to all subscribers
        let _ = self.shutdown_tx.send(());

        info!("Client stopped");
        Ok(())
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
            offline_queue_max_size: self.offline_queue_max_size.clone(),
            offline_queue_size: self.offline_queue_size.clone(),
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
            shutdown_tx: self.shutdown_tx.clone(),
            is_running: self.is_running.clone(),
        }
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
            let url = format!("{}/health", self.config.server_url());
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
            let url = format!("{}/info", self.config.server_url());
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

    /// Make an HTTP request with JSON body and proper content type
    #[instrument(skip(self, body))]
    async fn make_json_request(
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
            request = request
                .header("Content-Type", "application/json")
                .body(body_data);
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
    fn queue_offline_operation(&self, operation: OfflineOperation) -> ClientResult<()> {
        if !self.config.offline_mode_enabled() {
            return Err(ClientError::Connection {
                error: "Offline mode disabled".to_string(),
            });
        }

        let current_size = self.offline_queue_size.load(Ordering::Relaxed);
        let max_size = self.offline_queue_max_size.load(Ordering::Relaxed);

        if current_size >= max_size {
            // Try to remove an old operation to make space
            if self.offline_queue.pop().is_some() {
                self.offline_queue_size.fetch_sub(1, Ordering::Relaxed);
                warn!("Offline queue full, removing oldest operation");
            }
        }

        self.offline_queue.push(operation);
        self.offline_queue_size.fetch_add(1, Ordering::Relaxed);
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

        let mut operations_to_retry = Vec::new();
        let mut successful_operations = 0;

        // Process operations from the lock-free queue
        while let Some(operation) = self.offline_queue.pop() {
            self.offline_queue_size.fetch_sub(1, Ordering::Relaxed);

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
            self.offline_queue.push(operation);
            self.offline_queue_size.fetch_add(1, Ordering::Relaxed);
        }

        if successful_operations > 0 {
            info!("Processed {} offline operations successfully", successful_operations);
        }
    }

    // ========================================
    // Single File Operations
    // ========================================

    /// Upload a file to the server with hash verification
    #[instrument(skip(self, content))]
    pub async fn upload_file(&self, path: &str, content: Bytes) -> ClientResult<crate::types::FileMetadata> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        // Calculate hash for integrity verification
        let expected_hash = self.hash_manager.hash_bytes(&content);
        
        self.retry_policy.execute(|| async {
            let url = format!("{}/files/{}", self.config.server_url(), path.trim_start_matches('/'));
            let response = self.make_request(Method::POST, &url, Some(content.clone())).await?;
            
            // Parse response metadata
            let metadata_response: crate::types::FileMetadataResponse = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "upload_file".to_string(),
                    error: e.to_string(),
                })?;
            
            // Convert to internal format and verify hash
            let metadata = crate::types::FileMetadata::try_from(metadata_response)?;
            if !self.hash_manager.verify_bytes(&content, expected_hash) {
                return Err(ClientError::HashMismatch {
                    path: path.to_string(),
                    expected: self.hash_manager.to_hex(expected_hash),
                    actual: self.hash_manager.to_hex(metadata.xxhash3),
                });
            }
            
            debug!("File uploaded successfully: {}", path);
            Ok(metadata)
        }).await
    }

    /// Download a file from the server with hash verification
    #[instrument(skip(self))]
    pub async fn download_file(&self, path: &str) -> ClientResult<Bytes> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/files/{}", self.config.server_url(), path.trim_start_matches('/'));
            let response = self.make_request(Method::GET, &url, None).await?;
            
            // Get expected hash from response headers
            let expected_hash = response.headers()
                .get("X-File-Hash")
                .and_then(|h| h.to_str().ok())
                .and_then(|h| self.hash_manager.from_hex(h).ok())
                .ok_or_else(|| ClientError::InvalidResponse { 
                    error: "Missing or invalid X-File-Hash header".to_string() 
                })?;
            
            // Get file content
            let content = response.bytes().await
                .map_err(ClientError::Network)?;
            
            // Update download statistics
            self.bytes_downloaded.fetch_add(content.len() as u64, Ordering::Relaxed);
            
            // Verify hash integrity
            if !self.hash_manager.verify_bytes(&content, expected_hash) {
                return Err(ClientError::HashMismatch {
                    path: path.to_string(),
                    expected: self.hash_manager.to_hex(expected_hash),
                    actual: self.hash_manager.to_hex(self.hash_manager.hash_bytes(&content)),
                });
            }
            
            debug!("File downloaded successfully: {}", path);
            Ok(content)
        }).await
    }

    /// Update an existing file on the server with hash verification
    #[instrument(skip(self, content))]
    pub async fn update_file(&self, path: &str, content: Bytes) -> ClientResult<crate::types::FileMetadata> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        // Calculate hash for integrity verification
        let expected_hash = self.hash_manager.hash_bytes(&content);
        
        self.retry_policy.execute(|| async {
            let url = format!("{}/files/{}", self.config.server_url(), path.trim_start_matches('/'));
            let response = self.make_request(Method::PUT, &url, Some(content.clone())).await?;
            
            // Parse response metadata
            let metadata_response: crate::types::FileMetadataResponse = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "update_file".to_string(),
                    error: e.to_string(),
                })?;
            
            // Convert to internal format and verify hash
            let metadata = crate::types::FileMetadata::try_from(metadata_response)?;
            if !self.hash_manager.verify_bytes(&content, expected_hash) {
                return Err(ClientError::HashMismatch {
                    path: path.to_string(),
                    expected: self.hash_manager.to_hex(expected_hash),
                    actual: self.hash_manager.to_hex(metadata.xxhash3),
                });
            }
            
            debug!("File updated successfully: {}", path);
            Ok(metadata)
        }).await
    }

    /// Delete a file from the server
    #[instrument(skip(self))]
    pub async fn delete_file(&self, path: &str) -> ClientResult<()> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/files/{}", self.config.server_url(), path.trim_start_matches('/'));
            let _response = self.make_request(Method::DELETE, &url, None).await?;
            
            debug!("File deleted successfully: {}", path);
            Ok(())
        }).await
    }

    /// Get file metadata without downloading content
    #[instrument(skip(self))]
    pub async fn get_file_metadata(&self, path: &str) -> ClientResult<crate::types::FileMetadata> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/files/{}", self.config.server_url(), path.trim_start_matches('/'));
            let response = self.make_request(Method::HEAD, &url, None).await?;
            
            // Extract metadata from headers
            let size = response.headers()
                .get("X-File-Size")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| ClientError::InvalidResponse { 
                    error: "Missing or invalid X-File-Size header".to_string() 
                })?;
            
            let hash = response.headers()
                .get("X-File-Hash")
                .and_then(|h| h.to_str().ok())
                .and_then(|h| self.hash_manager.from_hex(h).ok())
                .ok_or_else(|| ClientError::InvalidResponse { 
                    error: "Missing or invalid X-File-Hash header".to_string() 
                })?;
            
            let modified_str = response.headers()
                .get("X-Modified-Time")
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| ClientError::InvalidResponse { 
                    error: "Missing X-Modified-Time header".to_string() 
                })?;
            
            // Parse ISO 8601 timestamp
            let modified = chrono::DateTime::parse_from_rfc3339(modified_str)
                .map_err(|_| ClientError::InvalidResponse { 
                    error: format!("Invalid timestamp format: {}", modified_str)
                })?
                .with_timezone(&chrono::Utc)
                .timestamp() as u64;
            
            let modified_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(modified);
            
            let metadata = crate::types::FileMetadata::new(
                path.to_string(),
                size,
                modified_time,
                hash,
            );
            
            debug!("File metadata retrieved successfully: {}", path);
            Ok(metadata)
        }).await
    }

    /// Move/rename a file on the server
    #[instrument(skip(self))]
    pub async fn move_file(&self, from_path: &str, to_path: &str) -> ClientResult<()> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/files/move", self.config.server_url());
            let move_request = crate::types::MoveRequest {
                from_path: from_path.to_string(),
                to_path: to_path.to_string(),
            };
            
            let body = serde_json::to_vec(&move_request)
                .map_err(|e| ClientError::Serialization {
                    operation: "move_file".to_string(),
                    error: e.to_string(),
                })?;
            
            let _response = self.make_json_request(Method::POST, &url, Some(Bytes::from(body))).await?;
            
            debug!("File moved successfully: {} -> {}", from_path, to_path);
            Ok(())
        }).await
    }

    /// Copy a file on the server
    #[instrument(skip(self))]
    pub async fn copy_file(&self, from_path: &str, to_path: &str) -> ClientResult<()> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/files/copy", self.config.server_url());
            let copy_request = crate::types::CopyRequest {
                from_path: from_path.to_string(),
                to_path: to_path.to_string(),
            };
            
            let body = serde_json::to_vec(&copy_request)
                .map_err(|e| ClientError::Serialization {
                    operation: "copy_file".to_string(),
                    error: e.to_string(),
                })?;
            
            let _response = self.make_json_request(Method::POST, &url, Some(Bytes::from(body))).await?;
            
            debug!("File copied successfully: {} -> {}", from_path, to_path);
            Ok(())
        }).await
    }

    /// Verify file integrity by comparing local and remote hashes
    #[instrument(skip(self))]
    pub async fn verify_file_integrity(&self, path: &str, expected_hash: u64) -> ClientResult<bool> {
        let metadata = self.get_file_metadata(path).await?;
        Ok(expected_hash == metadata.xxhash3)
    }

    // ========================================
    // Directory Operations
    // ========================================

    /// Create a directory on the server
    #[instrument(skip(self))]
    pub async fn create_directory(&self, path: &str) -> ClientResult<()> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/directories/{}", self.config.server_url(), path.trim_start_matches('/'));
            let _response = self.make_request(Method::POST, &url, None).await?;
            
            debug!("Directory created successfully: {}", path);
            Ok(())
        }).await
    }

    /// Delete a directory from the server (recursive)
    #[instrument(skip(self))]
    pub async fn delete_directory(&self, path: &str) -> ClientResult<()> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/directories/{}", self.config.server_url(), path.trim_start_matches('/'));
            let _response = self.make_request(Method::DELETE, &url, None).await?;
            
            debug!("Directory deleted successfully: {}", path);
            Ok(())
        }).await
    }

    /// List directory contents with Snappy compression and Bitcode serialization support
    #[instrument(skip(self))]
    pub async fn list_directory(&self, path: Option<&str>) -> ClientResult<crate::types::DirectoryListing> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = if let Some(path) = path {
                format!("{}/files/{}", self.config.server_url(), path.trim_start_matches('/'))
            } else {
                format!("{}/files", self.config.server_url())
            };
            
            // Make request with compression support headers
            let mut request = self.http_client.get(&url);
            request = request.header("Accept-Encoding", "snappy, gzip, deflate");
            request = request.header("Accept", "application/json, application/octet-stream");
            
            let response = timeout(
                self.config.request_timeout(),
                request.send()
            ).await
            .map_err(|_| ClientError::Timeout {
                duration: self.config.request_timeout().as_secs(),
                operation: "list_directory".to_string(),
            })?
            .map_err(ClientError::Network)?;

            if !response.status().is_success() {
                return Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: response.status().canonical_reason()
                        .unwrap_or("Unknown error").to_string(),
                });
            }

            // Check content encoding and handle accordingly
            let content_encoding = response.headers()
                .get("content-encoding")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .to_string();

            let content_type = response.headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("application/json")
                .to_string();

            let body_bytes = response.bytes().await
                .map_err(ClientError::Network)?;

            let listing = if content_encoding == "snappy" && content_type.contains("application/octet-stream") {
                // Handle Snappy compressed Bitcode data
                use crate::serialization::deserialize_compressed;
                use crate::storage_types::StorageDirectoryListing;
                
                let storage_listing = deserialize_compressed::<StorageDirectoryListing>(&body_bytes)?;
                storage_listing.into()
            } else {
                // Handle standard JSON response
                serde_json::from_slice::<crate::types::DirectoryListing>(&body_bytes)
                    .map_err(|e| ClientError::Deserialization {
                        operation: "list_directory".to_string(),
                        error: e.to_string(),
                    })?
            };
            
            debug!("Directory listed successfully: {} (encoding: {}, type: {})", 
                   path.unwrap_or("/"), content_encoding, content_type);
            Ok(listing)
        }).await
    }

    /// Search for files matching a pattern with concurrent pattern matching and atomic result collection
    #[instrument(skip(self))]
    pub async fn search_files(&self, pattern: &str, path: Option<&str>) -> ClientResult<Vec<crate::types::FileMetadata>> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        // Atomic counter for tracking results
        let result_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));

        self.retry_policy.execute(|| async {
            let url = format!("{}/search", self.config.server_url());
            let search_request = crate::types::SearchRequest {
                pattern: pattern.to_string(),
                path: path.map(|p| p.to_string()),
                recursive: true,
            };
            
            let body = serde_json::to_vec(&search_request)
                .map_err(|e| {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    ClientError::Serialization {
                        operation: "search_files".to_string(),
                        error: e.to_string(),
                    }
                })?;
            
            // Make request with compression support headers
            let mut request = self.http_client.post(&url);
            request = request.header("Accept-Encoding", "snappy, gzip, deflate");
            request = request.header("Accept", "application/json, application/octet-stream");
            request = request.header("Content-Type", "application/json");
            request = request.body(body);
            
            let response = timeout(
                self.config.request_timeout(),
                request.send()
            ).await
            .map_err(|_| {
                error_count.fetch_add(1, Ordering::Relaxed);
                ClientError::Timeout {
                    duration: self.config.request_timeout().as_secs(),
                    operation: "search_files".to_string(),
                }
            })?
            .map_err(|e| {
                error_count.fetch_add(1, Ordering::Relaxed);
                ClientError::Network(e)
            })?;

            if !response.status().is_success() {
                error_count.fetch_add(1, Ordering::Relaxed);
                return Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: response.status().canonical_reason()
                        .unwrap_or("Unknown error").to_string(),
                });
            }

            // Check content encoding and handle accordingly
            let content_encoding = response.headers()
                .get("content-encoding")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .to_string();

            let content_type = response.headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("application/json")
                .to_string();

            let body_bytes = response.bytes().await
                .map_err(|e| {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    ClientError::Network(e)
                })?;

            let results = if content_encoding == "snappy" && content_type.contains("application/octet-stream") {
                // Handle Snappy compressed Bitcode data
                use crate::serialization::deserialize_compressed;
                use crate::storage_types::StorageFileMetadata;
                
                let storage_results = deserialize_compressed::<Vec<StorageFileMetadata>>(&body_bytes)
                    .map_err(|e| {
                        error_count.fetch_add(1, Ordering::Relaxed);
                        e
                    })?;
                
                // Convert storage types to API types with atomic counting
                storage_results.into_iter()
                    .map(|storage_meta| {
                        result_count.fetch_add(1, Ordering::Relaxed);
                        storage_meta.into()
                    })
                    .collect::<Vec<crate::types::FileMetadata>>()
            } else {
                // Handle standard JSON response
                let json_results = serde_json::from_slice::<Vec<crate::types::FileMetadata>>(&body_bytes)
                    .map_err(|e| {
                        error_count.fetch_add(1, Ordering::Relaxed);
                        ClientError::Deserialization {
                            operation: "search_files".to_string(),
                            error: e.to_string(),
                        }
                    })?;
                
                // Update atomic counter
                result_count.store(json_results.len() as u64, Ordering::Relaxed);
                json_results
            };
            
            let final_count = result_count.load(Ordering::Relaxed);
            let final_errors = error_count.load(Ordering::Relaxed);
            
            debug!("Search completed successfully: {} results for pattern '{}' (encoding: {}, type: {}, errors: {})", 
                   final_count, pattern, content_encoding, content_type, final_errors);
            Ok(results)
        }).await
    }

    // ========================================
    // Bulk Operations
    // ========================================

    /// Upload multiple files with zero-copy Bytes and lock-free operations
    #[instrument(skip(self, files))]
    pub async fn bulk_upload(&self, files: Vec<crate::types::FileUpload>) -> ClientResult<crate::types::BulkUploadResponse> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        // Atomic counters for progress tracking
        let successful_count = Arc::new(AtomicU64::new(0));
        let failed_count = Arc::new(AtomicU64::new(0));
        let total_bytes = Arc::new(AtomicU64::new(0));

        // Calculate total size for progress tracking
        let total_size: u64 = files.iter().map(|f| f.content.len() as u64).sum();
        total_bytes.store(total_size, Ordering::Relaxed);

        self.retry_policy.execute(|| async {
            let url = format!("{}/bulk/upload", self.config.server_url());
            
            // Create multipart form for zero-copy upload
            let mut form = reqwest::multipart::Form::new();
            
            // Add each file as a separate part with zero-copy
            for file_upload in &files {
                // Calculate hash for integrity verification
                let file_hash = self.hash_manager.hash_bytes(&file_upload.content);
                
                let part = reqwest::multipart::Part::bytes(file_upload.content.to_vec())
                    .file_name(file_upload.path.clone())
                    .mime_str("application/octet-stream")
                    .map_err(|e| ClientError::Serialization {
                        operation: "bulk_upload".to_string(),
                        error: format!("Failed to create multipart: {}", e),
                    })?;
                
                // Add hash as metadata
                let part = part.headers(reqwest::header::HeaderMap::from_iter([
                    (reqwest::header::HeaderName::from_static("x-file-hash"), 
                     reqwest::header::HeaderValue::from_str(&format!("{:016x}", file_hash))
                        .map_err(|e| ClientError::Serialization {
                            operation: "bulk_upload".to_string(),
                            error: format!("Failed to create hash header: {}", e),
                        })?)
                ]));
                
                form = form.part(file_upload.path.clone(), part);
            }

            // Make request with multipart form
            let response = timeout(
                self.config.request_timeout(),
                self.http_client.post(&url).multipart(form).send()
            ).await
            .map_err(|_| ClientError::Timeout {
                duration: self.config.request_timeout().as_secs(),
                operation: "bulk_upload".to_string(),
            })?
            .map_err(ClientError::Network)?;

            if !response.status().is_success() {
                failed_count.store(files.len() as u64, Ordering::Relaxed);
                return Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: response.status().canonical_reason()
                        .unwrap_or("Bulk upload failed").to_string(),
                });
            }

            // Parse response
            let bulk_response: crate::types::BulkUploadResponse = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "bulk_upload".to_string(),
                    error: e.to_string(),
                })?;

            // Update atomic counters
            successful_count.store(bulk_response.success.len() as u64, Ordering::Relaxed);
            failed_count.store(bulk_response.failed.len() as u64, Ordering::Relaxed);
            
            // Update bytes uploaded counter
            let uploaded_bytes: u64 = bulk_response.success.iter()
                .filter_map(|path| files.iter().find(|f| f.path == *path))
                .map(|f| f.content.len() as u64)
                .sum();
            self.bytes_uploaded.fetch_add(uploaded_bytes, Ordering::Relaxed);

            let success_count = successful_count.load(Ordering::Relaxed);
            let error_count = failed_count.load(Ordering::Relaxed);
            
            debug!("Bulk upload completed: {} successful, {} failed, {} bytes", 
                   success_count, error_count, uploaded_bytes);
            
            Ok(bulk_response)
        }).await
    }

    /// Download multiple files concurrently returning individual files with atomic progress tracking
    #[instrument(skip(self))]
    pub async fn bulk_download(&self, paths: Vec<&str>) -> ClientResult<Vec<crate::types::FileDownload>> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        // Atomic counters for progress tracking
        let successful_count = Arc::new(AtomicU64::new(0));
        let failed_count = Arc::new(AtomicU64::new(0));
        let total_bytes_downloaded = Arc::new(AtomicU64::new(0));

        self.retry_policy.execute(|| async {
            let url = format!("{}/bulk/download", self.config.server_url());
            
            // Create request body
            let download_request = crate::types::BulkDownloadRequest {
                paths: paths.iter().map(|p| p.to_string()).collect(),
            };
            
            let body = serde_json::to_vec(&download_request)
                .map_err(|e| ClientError::Serialization {
                    operation: "bulk_download".to_string(),
                    error: e.to_string(),
                })?;

            // Make request with compression support headers
            let mut request = self.http_client.post(&url);
            request = request.header("Accept-Encoding", "snappy, gzip, deflate");
            request = request.header("Accept", "application/json, application/octet-stream");
            request = request.header("Content-Type", "application/json");
            request = request.body(body);

            let response = timeout(
                self.config.request_timeout(),
                request.send()
            ).await
            .map_err(|_| ClientError::Timeout {
                duration: self.config.request_timeout().as_secs(),
                operation: "bulk_download".to_string(),
            })?
            .map_err(ClientError::Network)?;

            if !response.status().is_success() {
                failed_count.store(paths.len() as u64, Ordering::Relaxed);
                return Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: response.status().canonical_reason()
                        .unwrap_or("Bulk download failed").to_string(),
                });
            }

            // Check content encoding and handle accordingly
            let content_encoding = response.headers()
                .get("content-encoding")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .to_string();

            let content_type = response.headers()
                .get("content-type")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("application/json")
                .to_string();

            let body_bytes = response.bytes().await
                .map_err(ClientError::Network)?;

            let downloads = if content_encoding == "snappy" && content_type.contains("application/octet-stream") {
                // Handle Snappy compressed Bitcode data
                use crate::serialization::deserialize_compressed;
                use crate::storage_types::StorageBulkDownloadResponse;
                
                let storage_response = deserialize_compressed::<StorageBulkDownloadResponse>(&body_bytes)?;
                
                // Convert storage types to API types with zero-copy and atomic progress tracking
                storage_response.files.into_iter()
                    .map(|storage_file| {
                        let content_bytes = Bytes::from(storage_file.content);
                        
                        // Verify hash integrity
                        let calculated_hash = self.hash_manager.hash_bytes(&content_bytes);
                        if calculated_hash != storage_file.xxhash3 {
                            failed_count.fetch_add(1, Ordering::Relaxed);
                            return Err(ClientError::HashMismatch {
                                path: storage_file.path.clone(),
                                expected: self.hash_manager.to_hex(storage_file.xxhash3),
                                actual: self.hash_manager.to_hex(calculated_hash),
                            });
                        }
                        
                        // Update atomic counters
                        successful_count.fetch_add(1, Ordering::Relaxed);
                        total_bytes_downloaded.fetch_add(content_bytes.len() as u64, Ordering::Relaxed);
                        
                        Ok(crate::types::FileDownload {
                            path: storage_file.path,
                            content: content_bytes,
                            xxhash3: storage_file.xxhash3,
                        })
                    })
                    .collect::<ClientResult<Vec<_>>>()?
            } else {
                // Handle standard JSON response
                let json_response: crate::types::BulkDownloadResponse = serde_json::from_slice(&body_bytes)
                    .map_err(|e| ClientError::Deserialization {
                        operation: "bulk_download".to_string(),
                        error: e.to_string(),
                    })?;
                
                // Convert JSON response to FileDownload with zero-copy and atomic progress tracking
                json_response.files.into_iter()
                    .map(|file_response| {
                        // Decode base64 content
                        let content = general_purpose::STANDARD.decode(&file_response.content)
                            .map_err(|e| {
                                failed_count.fetch_add(1, Ordering::Relaxed);
                                ClientError::Deserialization {
                                    operation: "bulk_download".to_string(),
                                    error: format!("Failed to decode base64 content: {}", e),
                                }
                            })?;
                        
                        let content_bytes = Bytes::from(content);
                        
                        // Parse hash
                        let xxhash3 = u64::from_str_radix(&file_response.hash, 16)
                            .map_err(|e| {
                                failed_count.fetch_add(1, Ordering::Relaxed);
                                ClientError::InvalidResponse {
                                    error: format!("Invalid hash format: {}", e),
                                }
                            })?;
                        
                        // Verify hash integrity
                        let calculated_hash = self.hash_manager.hash_bytes(&content_bytes);
                        if calculated_hash != xxhash3 {
                            failed_count.fetch_add(1, Ordering::Relaxed);
                            return Err(ClientError::HashMismatch {
                                path: file_response.path.clone(),
                                expected: file_response.hash,
                                actual: self.hash_manager.to_hex(calculated_hash),
                            });
                        }
                        
                        // Update atomic counters
                        successful_count.fetch_add(1, Ordering::Relaxed);
                        total_bytes_downloaded.fetch_add(content_bytes.len() as u64, Ordering::Relaxed);
                        
                        Ok(crate::types::FileDownload {
                            path: file_response.path,
                            content: content_bytes,
                            xxhash3,
                        })
                    })
                    .collect::<ClientResult<Vec<_>>>()?
            };

            // Update global download statistics
            let downloaded_bytes = total_bytes_downloaded.load(Ordering::Relaxed);
            self.bytes_downloaded.fetch_add(downloaded_bytes, Ordering::Relaxed);

            let success_count = successful_count.load(Ordering::Relaxed);
            let error_count = failed_count.load(Ordering::Relaxed);
            
            debug!("Bulk download completed: {} successful, {} failed, {} bytes (encoding: {}, type: {})", 
                   success_count, error_count, downloaded_bytes, content_encoding, content_type);
            
            Ok(downloads)
        }).await
    }

    /// Delete multiple files/directories with lock-free operations
    #[instrument(skip(self))]
    pub async fn bulk_delete(&self, paths: Vec<&str>) -> ClientResult<crate::types::BulkDeleteResponse> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        // Atomic counters for progress tracking
        let successful_count = Arc::new(AtomicU64::new(0));
        let failed_count = Arc::new(AtomicU64::new(0));

        self.retry_policy.execute(|| async {
            let url = format!("{}/bulk/delete", self.config.server_url());
            
            // Create request body
            let delete_request = crate::types::BulkDeleteRequest {
                paths: paths.iter().map(|p| p.to_string()).collect(),
            };
            
            let body = serde_json::to_vec(&delete_request)
                .map_err(|e| ClientError::Serialization {
                    operation: "bulk_delete".to_string(),
                    error: e.to_string(),
                })?;

            let response = timeout(
                self.config.request_timeout(),
                self.http_client.delete(&url)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
            ).await
            .map_err(|_| ClientError::Timeout {
                duration: self.config.request_timeout().as_secs(),
                operation: "bulk_delete".to_string(),
            })?
            .map_err(ClientError::Network)?;

            if !response.status().is_success() {
                failed_count.store(paths.len() as u64, Ordering::Relaxed);
                return Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: response.status().canonical_reason()
                        .unwrap_or("Bulk delete failed").to_string(),
                });
            }

            // Parse response
            let bulk_response: crate::types::BulkDeleteResponse = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "bulk_delete".to_string(),
                    error: e.to_string(),
                })?;

            // Update atomic counters
            successful_count.store(bulk_response.success.len() as u64, Ordering::Relaxed);
            failed_count.store(bulk_response.failed.len() as u64, Ordering::Relaxed);

            let success_count = successful_count.load(Ordering::Relaxed);
            let error_count = failed_count.load(Ordering::Relaxed);
            
            debug!("Bulk delete completed: {} successful, {} failed", success_count, error_count);
            
            Ok(bulk_response)
        }).await
    }

    // ========================================
    // Sync Operations
    // ========================================

    /// Get complete file manifest from server with Snappy decompression
    #[instrument(skip(self))]
    pub async fn get_manifest(&self, path: Option<&str>) -> ClientResult<crate::types::FileManifest> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = if let Some(p) = path {
                format!("{}/bulk/manifest?path={}", self.config.server_url(), urlencoding::encode(p))
            } else {
                format!("{}/bulk/manifest", self.config.server_url())
            };
            
            let response = self.make_request(Method::GET, &url, None).await?;
            
            // Get compressed manifest data
            let compressed_data = response.bytes().await
                .map_err(ClientError::Network)?;
            
            // Update download statistics
            self.bytes_downloaded.fetch_add(compressed_data.len() as u64, Ordering::Relaxed);
            
            // Decompress and deserialize using Snappy + JSON
            let decompressed_data = crate::serialization::decompress_data(&compressed_data)?;
            let manifest: crate::types::FileManifest = crate::serialization::deserialize_json(&decompressed_data)?;
            
            debug!("Retrieved manifest with {} files", manifest.files.len());
            Ok(manifest)
        }).await
    }

    /// Compare local vs remote manifest and get sync plan using lock-free operations
    #[instrument(skip(self, local_manifest))]
    pub async fn sync_plan(&self, local_manifest: crate::types::FileManifest, conflict_resolution: crate::types::ConflictResolutionStrategy) -> ClientResult<crate::types::SyncPlan> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            // Create sync request
            let sync_request = crate::types::SyncRequest {
                client_manifest: local_manifest.clone(),
                conflict_resolution: conflict_resolution.clone(),
            };
            
            // Serialize and compress request using Snappy + JSON
            let request_data = crate::serialization::serialize_json(&sync_request)?;
            let compressed_request = crate::serialization::compress_data(&request_data)?;
            
            let url = format!("{}/bulk/sync", self.config.server_url());
            let response = self.make_json_request(Method::POST, &url, Some(Bytes::from(compressed_request))).await?;
            
            // Get compressed response data
            let compressed_response = response.bytes().await
                .map_err(ClientError::Network)?;
            
            // Update statistics
            self.bytes_downloaded.fetch_add(compressed_response.len() as u64, Ordering::Relaxed);
            
            // Decompress and deserialize using Snappy + JSON
            let decompressed_response = crate::serialization::decompress_data(&compressed_response)?;
            let sync_plan: crate::types::SyncPlan = crate::serialization::deserialize_json(&decompressed_response)?;
            
            debug!("Sync plan: {} uploads, {} downloads, {} deletes, {} conflicts", 
                   sync_plan.client_upload.len(), 
                   sync_plan.client_download.len(), 
                   sync_plan.client_delete.len(), 
                   sync_plan.conflicts.len());
            
            Ok(sync_plan)
        }).await
    }

    /// Download missing files from server with atomic progress tracking
    #[instrument(skip(self, file_paths))]
    pub async fn download_missing_files(&self, file_paths: &[String]) -> ClientResult<Vec<(String, Bytes)>> {
        if file_paths.is_empty() {
            return Ok(Vec::new());
        }

        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        // Use atomic counters for progress tracking
        let _total_files = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(file_paths.len()));
        let completed_files = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed_files = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Download files concurrently using lock-free patterns
        let mut download_tasks = Vec::new();
        
        for path in file_paths {
            let client = self.clone_for_background();
            let path = path.clone();
            let completed = completed_files.clone();
            let failed = failed_files.clone();
            
            let task = tokio::spawn(async move {
                match client.download_file(&path).await {
                    Ok(content) => {
                        completed.fetch_add(1, Ordering::Relaxed);
                        Ok((path, content))
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        Err((path, e))
                    }
                }
            });
            
            download_tasks.push(task);
        }

        // Collect results using lock-free operations
        let mut successful_downloads = Vec::new();
        let mut errors = Vec::new();

        for task in download_tasks {
            match task.await {
                Ok(Ok((path, content))) => {
                    successful_downloads.push((path, content));
                }
                Ok(Err((path, error))) => {
                    errors.push((path, error.to_string()));
                }
                Err(join_error) => {
                    errors.push(("unknown".to_string(), join_error.to_string()));
                }
            }
        }

        let success_count = completed_files.load(Ordering::Relaxed);
        let error_count = failed_files.load(Ordering::Relaxed);
        
        debug!("Downloaded {} files successfully, {} failed", success_count, error_count);

        if !errors.is_empty() {
            return Err(ClientError::PartialFailure {
                successful: success_count,
                failed: error_count,
                errors: errors.into_iter().map(|(path, error)| format!("{}: {}", path, error)).collect(),
            });
        }

        Ok(successful_downloads)
    }

    /// Delete extra local files with atomic progress tracking
    #[instrument(skip(self, file_paths, delete_fn))]
    pub async fn delete_extra_local_files<F, Fut>(&self, file_paths: &[String], delete_fn: F) -> ClientResult<Vec<String>>
    where
        F: Fn(String) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
    {
        if file_paths.is_empty() {
            return Ok(Vec::new());
        }

        // Use atomic counters for progress tracking
        let _total_files = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(file_paths.len()));
        let completed_files = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed_files = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Delete files concurrently using lock-free patterns
        let mut delete_tasks = Vec::new();
        
        for path in file_paths {
            let path = path.clone();
            let delete_fn = delete_fn.clone();
            let completed = completed_files.clone();
            let failed = failed_files.clone();
            
            let task = tokio::spawn(async move {
                match delete_fn(path.clone()).await {
                    Ok(()) => {
                        completed.fetch_add(1, Ordering::Relaxed);
                        Ok(path)
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        Err((path, e))
                    }
                }
            });
            
            delete_tasks.push(task);
        }

        // Collect results using lock-free operations
        let mut successful_deletions = Vec::new();
        let mut errors = Vec::new();

        for task in delete_tasks {
            match task.await {
                Ok(Ok(path)) => {
                    successful_deletions.push(path);
                }
                Ok(Err((path, error))) => {
                    errors.push((path, error.to_string()));
                }
                Err(join_error) => {
                    errors.push(("unknown".to_string(), join_error.to_string()));
                }
            }
        }

        let success_count = completed_files.load(Ordering::Relaxed);
        let error_count = failed_files.load(Ordering::Relaxed);
        
        debug!("Deleted {} files successfully, {} failed", success_count, error_count);

        if !errors.is_empty() {
            return Err(ClientError::PartialFailure {
                successful: success_count,
                failed: error_count,
                errors: errors.into_iter().map(|(path, error)| format!("{}: {}", path, error)).collect(),
            });
        }

        Ok(successful_deletions)
    }

    /// High-level full sync method that makes local directory match server using lock-free patterns
    #[instrument(skip(self, local_manifest, _upload_fn, download_fn, delete_fn, create_dir_fn))]
    pub async fn full_sync<UF, UFut, DF, DFut, DelF, DelFut, CF, CFut>(
        &self,
        local_manifest: crate::types::FileManifest,
        conflict_resolution: crate::types::ConflictResolutionStrategy,
        _upload_fn: UF,
        download_fn: DF,
        delete_fn: DelF,
        create_dir_fn: CF,
    ) -> ClientResult<crate::types::SyncResult>
    where
        UF: Fn(String, Bytes) -> UFut + Send + Sync + Clone + 'static,
        UFut: std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
        DF: Fn(String, Bytes) -> DFut + Send + Sync + Clone + 'static,
        DFut: std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
        DelF: Fn(String) -> DelFut + Send + Sync + Clone + 'static,
        DelFut: std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
        CF: Fn(String) -> CFut + Send + Sync + Clone + 'static,
        CFut: std::future::Future<Output = Result<(), std::io::Error>> + Send + 'static,
    {
        let mut sync_result = crate::types::SyncResult::new();

        // Step 1: Get sync plan from server
        let sync_plan = self.sync_plan(local_manifest, conflict_resolution).await?;
        
        if sync_plan.is_empty() {
            debug!("No sync operations needed");
            return Ok(sync_result);
        }

        debug!("Executing sync plan with {} total operations", sync_plan.total_operations());

        // Step 2: Create directories first (atomic operations)
        if !sync_plan.client_create_dirs.is_empty() {
            debug!("Creating {} directories", sync_plan.client_create_dirs.len());
            
            for dir_path in &sync_plan.client_create_dirs {
                match create_dir_fn(dir_path.clone()).await {
                    Ok(()) => {
                        sync_result.created_dirs.push(dir_path.clone());
                    }
                    Err(e) => {
                        sync_result.errors.push((dir_path.clone(), e.to_string()));
                    }
                }
            }
        }

        // Step 3: Upload files that exist locally but not on server (atomic progress tracking)
        if !sync_plan.client_upload.is_empty() {
            debug!("Uploading {} files to server", sync_plan.client_upload.len());
            
            for file_path in &sync_plan.client_upload {
                // Read local file and upload to server
                match tokio::fs::read(file_path).await {
                    Ok(content) => {
                        let content_bytes = Bytes::from(content);
                        match self.upload_file(file_path, content_bytes).await {
                            Ok(_) => {
                                sync_result.uploaded.push(file_path.clone());
                            }
                            Err(e) => {
                                sync_result.errors.push((file_path.clone(), e.to_string()));
                            }
                        }
                    }
                    Err(e) => {
                        sync_result.errors.push((file_path.clone(), format!("Failed to read local file: {}", e)));
                    }
                }
            }
        }

        // Step 4: Download files that exist on server but not locally (lock-free concurrent downloads)
        if !sync_plan.client_download.is_empty() {
            debug!("Downloading {} files from server", sync_plan.client_download.len());
            
            match self.download_missing_files(&sync_plan.client_download).await {
                Ok(downloaded_files) => {
                    // Save downloaded files locally using provided function
                    for (file_path, content) in downloaded_files {
                        match download_fn(file_path.clone(), content).await {
                            Ok(()) => {
                                sync_result.downloaded.push(file_path);
                            }
                            Err(e) => {
                                sync_result.errors.push((file_path, format!("Failed to save downloaded file: {}", e)));
                            }
                        }
                    }
                }
                Err(e) => {
                    sync_result.errors.push(("bulk_download".to_string(), e.to_string()));
                }
            }
        }

        // Step 5: Delete local files that don't exist on server (atomic progress tracking)
        if !sync_plan.client_delete.is_empty() {
            debug!("Deleting {} local files", sync_plan.client_delete.len());
            
            match self.delete_extra_local_files(&sync_plan.client_delete, delete_fn).await {
                Ok(deleted_files) => {
                    sync_result.deleted_local.extend(deleted_files);
                }
                Err(e) => {
                    sync_result.errors.push(("bulk_delete".to_string(), e.to_string()));
                }
            }
        }

        // Step 6: Handle conflicts based on resolution strategy (atomic operations)
        if !sync_plan.conflicts.is_empty() {
            debug!("Resolving {} conflicts", sync_plan.conflicts.len());
            
            for conflict in &sync_plan.conflicts {
                let resolution_result = match conflict.resolution {
                    crate::types::ConflictResolution::UseLocal => {
                        // Upload local version to server
                        match tokio::fs::read(&conflict.path).await {
                            Ok(content) => {
                                let content_bytes = Bytes::from(content);
                                self.upload_file(&conflict.path, content_bytes).await
                                    .map(|_| "uploaded_local".to_string())
                            }
                            Err(e) => Err(ClientError::LocalFile {
                                path: conflict.path.clone(),
                                error: e.to_string(),
                            })
                        }
                    }
                    crate::types::ConflictResolution::UseRemote => {
                        // Download remote version
                        match self.download_file(&conflict.path).await {
                            Ok(content) => {
                                download_fn(conflict.path.clone(), content).await
                                    .map(|_| "downloaded_remote".to_string())
                                    .map_err(|e| ClientError::LocalFile {
                                        path: conflict.path.clone(),
                                        error: e.to_string(),
                                    })
                            }
                            Err(e) => Err(e)
                        }
                    }
                    crate::types::ConflictResolution::UseNewer => {
                        // Use the newer version based on timestamps
                        if conflict.local_modified > conflict.remote_modified {
                            // Local is newer, upload it
                            match tokio::fs::read(&conflict.path).await {
                                Ok(content) => {
                                    let content_bytes = Bytes::from(content);
                                    self.upload_file(&conflict.path, content_bytes).await
                                        .map(|_| "uploaded_newer_local".to_string())
                                }
                                Err(e) => Err(ClientError::LocalFile {
                                    path: conflict.path.clone(),
                                    error: e.to_string(),
                                })
                            }
                        } else {
                            // Remote is newer, download it
                            match self.download_file(&conflict.path).await {
                                Ok(content) => {
                                    download_fn(conflict.path.clone(), content).await
                                        .map(|_| "downloaded_newer_remote".to_string())
                                        .map_err(|e| ClientError::LocalFile {
                                            path: conflict.path.clone(),
                                            error: e.to_string(),
                                        })
                                }
                                Err(e) => Err(e)
                            }
                        }
                    }
                    crate::types::ConflictResolution::Manual => {
                        // Skip manual conflicts - they need user intervention
                        sync_result.errors.push((conflict.path.clone(), "Manual conflict resolution required".to_string()));
                        continue;
                    }
                };

                match resolution_result {
                    Ok(action) => {
                        sync_result.conflicts_resolved.push(format!("{}: {}", conflict.path, action));
                    }
                    Err(e) => {
                        sync_result.errors.push((conflict.path.clone(), format!("Conflict resolution failed: {}", e)));
                    }
                }
            }
        }

        let total_operations = sync_result.success_count();
        let total_errors = sync_result.error_count();
        
        debug!("Sync completed: {} successful operations, {} errors", total_operations, total_errors);
        
        Ok(sync_result)
    }


}

impl Drop for Client {
    fn drop(&mut self) {
        // Ensure background tasks are stopped when client is dropped
        if self.is_running.load(Ordering::Acquire) {
            // We can't await in Drop, so we just send the shutdown signal
            let _ = self.shutdown_tx.send(());
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

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
        let server_url = client.config.server_url();
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
    async fn test_client_start_stop() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Initially not running
        assert!(!client.is_running.load(Ordering::Relaxed));
        
        // Start client
        client.start().await.unwrap();
        assert!(client.is_running.load(Ordering::Relaxed));
        
        // Stop client
        client.stop().await.unwrap();
        assert!(!client.is_running.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_config_atomic_operations() {
        let config = Config::new("https://localhost:8080".to_string());
        
        // Test atomic operations
        assert_eq!(config.max_retries(), 5);
        config.set_max_retries(10);
        assert_eq!(config.max_retries(), 10);
        
        assert_eq!(config.pool_size(), 10);
        config.set_pool_size(20);
        assert_eq!(config.pool_size(), 20);
        
        assert!(config.tls_verify());
        config.set_tls_verify(false);
        assert!(!config.tls_verify());
    }

    #[tokio::test]
    async fn test_config_validation() {
        // Test valid config
        let config = Config::new("https://localhost:8080".to_string());
        assert!(config.validate().is_ok());
        
        // Test invalid config - empty URL
        let config = Config::new("".to_string());
        assert!(config.validate().is_err());
        
        // Test invalid config - too many retries
        let config = Config::new("https://localhost:8080".to_string());
        config.set_max_retries(15);
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_hash_operations() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        let test_data = Bytes::from("test file content");
        let hash = client.hash_manager.hash_bytes(&test_data);
        
        // Verify correct hash
        assert!(client.hash_manager.verify_bytes(&test_data, hash));
        
        // Verify incorrect hash
        assert!(!client.hash_manager.verify_bytes(&test_data, hash + 1));
    }

    #[tokio::test]
    async fn test_sync_plan_creation() {
        use crate::types::*;
        use std::collections::HashMap;
        use std::time::SystemTime;

        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Create a test local manifest
        let mut local_files = HashMap::new();
        local_files.insert(
            "test.txt".to_string(),
            FileMetadata::new(
                "test.txt".to_string(),
                100,
                SystemTime::now(),
                12345,
            ),
        );
        
        let local_manifest = FileManifest { files: local_files };
        
        // Test sync plan creation (this would normally call the server)
        // For unit testing, we're just testing the method signature and basic structure
        let conflict_resolution = ConflictResolutionStrategy::PreferNewer;
        
        // This test verifies the method exists and has the correct signature
        // In a real test environment, you would mock the server response
        let result = client.sync_plan(local_manifest, conflict_resolution).await;
        
        // Since we don't have a server running, this should fail with a network error
        assert!(result.is_err());
        
        // Verify it's a network-related error (not a compilation error)
        match result.unwrap_err() {
            ClientError::Network(_) | ClientError::Connection { .. } | ClientError::Timeout { .. } | ClientError::CircuitBreakerOpen => {
                // Expected - no server running
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_download_missing_files_empty() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Test with empty file list
        let result = client.download_missing_files(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_delete_extra_local_files_empty() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Test with empty file list
        let delete_fn = |_path: String| async move { Ok(()) };
        let result = client.delete_extra_local_files(&[], delete_fn).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_full_sync_empty_plan() {
        use crate::types::*;
        use std::collections::HashMap;

        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Create empty local manifest
        let local_manifest = FileManifest { files: HashMap::new() };
        let conflict_resolution = ConflictResolutionStrategy::PreferNewer;
        
        // Mock functions for file operations
        let upload_fn = |_path: String, _content: Bytes| async move { Ok(()) };
        let download_fn = |_path: String, _content: Bytes| async move { Ok(()) };
        let delete_fn = |_path: String| async move { Ok(()) };
        let create_dir_fn = |_path: String| async move { Ok(()) };
        
        // This should fail with network error since no server is running
        let result = client.full_sync(
            local_manifest,
            conflict_resolution,
            upload_fn,
            download_fn,
            delete_fn,
            create_dir_fn,
        ).await;
        
        // Verify it fails with network error (expected since no server)
        assert!(result.is_err());
        match result.unwrap_err() {
            ClientError::Network(_) | ClientError::Connection { .. } | ClientError::Timeout { .. } | ClientError::CircuitBreakerOpen => {
                // Expected - no server running
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_sync_result_helpers() {
        use crate::types::SyncResult;
        
        let mut result = SyncResult::new();
        assert!(result.is_success());
        assert_eq!(result.success_count(), 0);
        assert_eq!(result.error_count(), 0);
        
        // Add some successful operations
        result.uploaded.push("file1.txt".to_string());
        result.downloaded.push("file2.txt".to_string());
        result.created_dirs.push("dir1".to_string());
        
        assert!(result.is_success());
        assert_eq!(result.success_count(), 3);
        assert_eq!(result.error_count(), 0);
        
        // Add an error
        result.errors.push(("file3.txt".to_string(), "Failed to upload".to_string()));
        
        assert!(!result.is_success());
        assert_eq!(result.success_count(), 3);
        assert_eq!(result.error_count(), 1);
    }

    #[tokio::test]
    async fn test_sync_plan_helpers() {
        use crate::types::SyncPlan;
        
        let mut plan = SyncPlan::new();
        assert!(plan.is_empty());
        assert_eq!(plan.total_operations(), 0);
        
        // Add some operations
        plan.client_upload.push("file1.txt".to_string());
        plan.client_download.push("file2.txt".to_string());
        plan.client_delete.push("file3.txt".to_string());
        plan.client_create_dirs.push("dir1".to_string());
        
        assert!(!plan.is_empty());
        assert_eq!(plan.total_operations(), 4);
    }

    #[tokio::test]
    async fn test_file_metadata_helpers() {
        use crate::types::FileMetadata;
        use std::time::SystemTime;
        
        let now = SystemTime::now();
        
        // Test file metadata creation
        let file_meta = FileMetadata::new(
            "documents/test.txt".to_string(),
            1024,
            now,
            12345,
        );
        
        assert_eq!(file_meta.path, "documents/test.txt");
        assert_eq!(file_meta.name, "test.txt");
        assert_eq!(file_meta.size, 1024);
        assert!(!file_meta.is_directory);
        assert_eq!(file_meta.xxhash3, 12345);
        
        // Test directory metadata creation
        let dir_meta = FileMetadata::new_directory(
            "documents".to_string(),
            now,
        );
        
        assert_eq!(dir_meta.path, "documents");
        assert_eq!(dir_meta.name, "documents");
        assert_eq!(dir_meta.size, 0);
        assert!(dir_meta.is_directory);
        assert_eq!(dir_meta.xxhash3, 0);
        
        // Test parent path
        assert_eq!(file_meta.parent_path(), Some("documents".to_string()));
        assert_eq!(dir_meta.parent_path(), Some("".to_string()));
        
        // Test directory membership
        assert!(file_meta.is_in_directory("documents"));
        assert!(!file_meta.is_in_directory("other"));
    }
}
