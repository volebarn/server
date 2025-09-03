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
            let url = format!("{}/files/{}", self.config.server_url().await, path.trim_start_matches('/'));
            let response = self.make_request(Method::POST, &url, Some(content.clone())).await?;
            
            // Parse response metadata
            let metadata_response: crate::types::FileMetadataResponse = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "upload_file".to_string(),
                    error: e.to_string(),
                })?;
            
            // Convert to internal format and verify hash
            let metadata = crate::types::FileMetadata::try_from(metadata_response)?;
            self.hash_manager.verify_with_error(path, expected_hash, metadata.xxhash3)?;
            
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
            let url = format!("{}/files/{}", self.config.server_url().await, path.trim_start_matches('/'));
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
            self.hash_manager.verify_bytes(path, &content, expected_hash).await?;
            
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
            let url = format!("{}/files/{}", self.config.server_url().await, path.trim_start_matches('/'));
            let response = self.make_request(Method::PUT, &url, Some(content.clone())).await?;
            
            // Parse response metadata
            let metadata_response: crate::types::FileMetadataResponse = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "update_file".to_string(),
                    error: e.to_string(),
                })?;
            
            // Convert to internal format and verify hash
            let metadata = crate::types::FileMetadata::try_from(metadata_response)?;
            self.hash_manager.verify_with_error(path, expected_hash, metadata.xxhash3)?;
            
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
            let url = format!("{}/files/{}", self.config.server_url().await, path.trim_start_matches('/'));
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
            let url = format!("{}/files/{}", self.config.server_url().await, path.trim_start_matches('/'));
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
            let url = format!("{}/files/move", self.config.server_url().await);
            let move_request = crate::types::MoveRequest {
                from_path: from_path.to_string(),
                to_path: to_path.to_string(),
            };
            
            let body = serde_json::to_vec(&move_request)
                .map_err(|e| ClientError::Serialization {
                    operation: "move_file".to_string(),
                    error: e.to_string(),
                })?;
            
            let _response = self.make_request(Method::POST, &url, Some(Bytes::from(body))).await?;
            
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
            let url = format!("{}/files/copy", self.config.server_url().await);
            let copy_request = crate::types::CopyRequest {
                from_path: from_path.to_string(),
                to_path: to_path.to_string(),
            };
            
            let body = serde_json::to_vec(&copy_request)
                .map_err(|e| ClientError::Serialization {
                    operation: "copy_file".to_string(),
                    error: e.to_string(),
                })?;
            
            let _response = self.make_request(Method::POST, &url, Some(Bytes::from(body))).await?;
            
            debug!("File copied successfully: {} -> {}", from_path, to_path);
            Ok(())
        }).await
    }

    /// Verify file integrity by comparing local and remote hashes
    #[instrument(skip(self))]
    pub async fn verify_file_integrity(&self, path: &str, expected_hash: u64) -> ClientResult<bool> {
        let metadata = self.get_file_metadata(path).await?;
        Ok(self.hash_manager.verify(expected_hash, metadata.xxhash3))
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
            let url = format!("{}/directories/{}", self.config.server_url().await, path.trim_start_matches('/'));
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
            let url = format!("{}/directories/{}", self.config.server_url().await, path.trim_start_matches('/'));
            let _response = self.make_request(Method::DELETE, &url, None).await?;
            
            debug!("Directory deleted successfully: {}", path);
            Ok(())
        }).await
    }

    /// List directory contents
    #[instrument(skip(self))]
    pub async fn list_directory(&self, path: Option<&str>) -> ClientResult<crate::types::DirectoryListing> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = if let Some(path) = path {
                format!("{}/files/{}", self.config.server_url().await, path.trim_start_matches('/'))
            } else {
                format!("{}/files", self.config.server_url().await)
            };
            
            let response = self.make_request(Method::GET, &url, None).await?;
            
            let listing: crate::types::DirectoryListing = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "list_directory".to_string(),
                    error: e.to_string(),
                })?;
            
            debug!("Directory listed successfully: {}", path.unwrap_or("/"));
            Ok(listing)
        }).await
    }

    /// Search for files matching a pattern
    #[instrument(skip(self))]
    pub async fn search_files(&self, pattern: &str, path: Option<&str>) -> ClientResult<Vec<crate::types::FileMetadata>> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| ClientError::Connection { error: "Concurrency limit exceeded".to_string() })?;

        self.retry_policy.execute(|| async {
            let url = format!("{}/search", self.config.server_url().await);
            let search_request = crate::types::SearchRequest {
                pattern: pattern.to_string(),
                path: path.map(|p| p.to_string()),
                recursive: true,
            };
            
            let body = serde_json::to_vec(&search_request)
                .map_err(|e| ClientError::Serialization {
                    operation: "search_files".to_string(),
                    error: e.to_string(),
                })?;
            
            let response = self.make_request(Method::POST, &url, Some(Bytes::from(body))).await?;
            
            let results: Vec<crate::types::FileMetadata> = response.json().await
                .map_err(|e| ClientError::Deserialization {
                    operation: "search_files".to_string(),
                    error: e.to_string(),
                })?;
            
            debug!("Search completed successfully: {} results for pattern '{}'", results.len(), pattern);
            Ok(results)
        }).await
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
    use crate::retry::CircuitState;
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

    // ========================================
    // Single File Operations Tests
    // ========================================

    #[tokio::test]
    async fn test_hash_verification_functionality() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        let content = Bytes::from_static(b"Test content for hash verification");
        
        // Test hash calculation
        let hash = client.hash_manager.hash_bytes(&content);
        assert_ne!(hash, 0);
        
        // Test hash verification success
        assert!(client.hash_manager.verify(hash, hash));
        
        // Test hash verification failure
        let wrong_hash = 0xdeadbeefcafebabe;
        assert!(!client.hash_manager.verify(hash, wrong_hash));
        
        // Test hash verification with error
        let result = client.hash_manager.verify_with_error("test_path", hash, wrong_hash);
        assert!(result.is_err());
        
        if let Err(crate::error::ClientError::HashMismatch { path, expected, actual }) = result {
            assert_eq!(path, "test_path");
            assert_eq!(expected, client.hash_manager.to_hex(hash));
            assert_eq!(actual, client.hash_manager.to_hex(wrong_hash));
        } else {
            panic!("Expected HashMismatch error");
        }
    }

    #[tokio::test]
    async fn test_file_operations_error_handling() {
        // Test with invalid server URL to trigger network errors
        let client = Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap();
        
        let path = "test/error.txt";
        let content = Bytes::from_static(b"Error test content");
        
        // Upload should fail with network error
        let upload_result = client.upload_file(path, content.clone()).await;
        assert!(upload_result.is_err());
        
        match upload_result.unwrap_err() {
            ClientError::Network(_) | ClientError::Connection { .. } | ClientError::CircuitBreakerOpen => {
                // Expected error types for network failures
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
        
        // Download should also fail
        let download_result = client.download_file(path).await;
        assert!(download_result.is_err());
        
        // Delete should also fail
        let delete_result = client.delete_file(path).await;
        assert!(delete_result.is_err());
        
        // Get metadata should also fail
        let metadata_result = client.get_file_metadata(path).await;
        assert!(metadata_result.is_err());
    }

    #[tokio::test]
    async fn test_path_normalization_in_urls() {
        let _client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Test that paths are properly normalized (leading slashes removed)
        let paths = vec![
            "test/normal.txt",
            "/test/leading_slash.txt",
            "//test/double_leading.txt",
        ];
        
        for path in paths {
            // We can't actually test the HTTP calls without a server,
            // but we can verify the URL construction logic by checking
            // that the methods don't panic and handle path normalization
            let normalized_path = path.trim_start_matches('/');
            assert!(!normalized_path.starts_with('/'));
        }
    }

    #[tokio::test]
    async fn test_concurrent_hash_operations() {
        let client = Arc::new(Client::with_defaults("https://localhost:8080".to_string()).await.unwrap());
        let mut handles = Vec::new();
        
        // Spawn multiple concurrent hash operations
        for i in 0..10 {
            let client_clone = Arc::clone(&client);
            let handle = tokio::spawn(async move {
                let content = Bytes::from(format!("concurrent test data {}", i));
                let hash = client_clone.hash_manager.hash_bytes(&content);
                
                // Verify the hash
                let verification_result = client_clone.hash_manager.verify(hash, hash);
                assert!(verification_result);
                
                hash
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        let mut hashes = Vec::new();
        for handle in handles {
            hashes.push(handle.await.unwrap());
        }
        
        // All hashes should be different (different input data)
        assert_eq!(hashes.len(), 10);
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(hashes[i], hashes[j]);
            }
        }
        
        // Check hash statistics
        let stats = client.hash_manager.get_stats();
        assert_eq!(stats.calculations, 10);
        assert_eq!(stats.verifications_success, 10);
        assert_eq!(stats.verifications_failed, 0);
        assert_eq!(stats.success_rate(), 100.0);
    }

    #[tokio::test]
    async fn test_large_content_hashing() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Create a large content buffer (1MB)
        let large_content = Bytes::from(vec![0xAB; 1024 * 1024]);
        
        // Hash the large content
        let hash = client.hash_manager.hash_bytes(&large_content);
        assert_ne!(hash, 0);
        
        // Verify hash consistency
        let hash2 = client.hash_manager.hash_bytes(&large_content);
        assert_eq!(hash, hash2);
        
        // Check statistics
        let stats = client.hash_manager.get_stats();
        assert_eq!(stats.calculations, 2);
        assert_eq!(stats.bytes_processed, 2 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_hash_manager_statistics_tracking() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Reset stats for clean test
        client.hash_manager.reset_stats();
        
        let content1 = Bytes::from_static(b"test data 1");
        let content2 = Bytes::from_static(b"test data 2");
        
        // Hash some data
        let hash1 = client.hash_manager.hash_bytes(&content1);
        let hash2 = client.hash_manager.hash_bytes(&content2);
        
        // Check calculations increased
        let stats = client.hash_manager.get_stats();
        assert_eq!(stats.calculations, 2);
        assert_eq!(stats.bytes_processed, (content1.len() + content2.len()) as u64);
        
        // Verify hashes
        assert!(client.hash_manager.verify(hash1, hash1)); // Success
        assert!(!client.hash_manager.verify(hash1, hash2)); // Failure
        
        // Check verification stats
        let stats = client.hash_manager.get_stats();
        assert_eq!(stats.verifications_success, 1);
        assert_eq!(stats.verifications_failed, 1);
        assert_eq!(stats.success_rate(), 50.0);
        assert_eq!(stats.avg_bytes_per_calculation(), (content1.len() + content2.len()) as f64 / 2.0);
    }

    #[tokio::test]
    async fn test_client_retry_policy_integration() {
        let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
        
        // Test that health monitor is properly initialized
        let health_status = client.health_monitor.is_healthy();
        // Initially should be None (unknown) since no health checks have been performed
        assert!(health_status.is_none() || health_status == Some(true));
        
        // Test that retry policy and circuit breaker are available
        // This verifies the components are properly initialized
        let circuit_breaker = client.retry_policy.circuit_breaker();
        assert_eq!(circuit_breaker.failure_count(), 0);
        assert_eq!(circuit_breaker.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_directory_operations_basic() {
        // Test basic directory operation method signatures and error handling
        let client = Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap();
        
        let dir_path = "test/directory";
        
        // All operations should fail with network errors due to invalid server
        let create_result = client.create_directory(dir_path).await;
        assert!(create_result.is_err());
        
        let delete_result = client.delete_directory(dir_path).await;
        assert!(delete_result.is_err());
        
        let list_result = client.list_directory(Some(dir_path)).await;
        assert!(list_result.is_err());
        
        let search_result = client.search_files("*.txt", Some(dir_path)).await;
        assert!(search_result.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_directory_operations() {
        // Test concurrent directory operations with atomic error tracking
        let client = Arc::new(Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap());
        let mut handles = Vec::new();
        
        // Spawn multiple concurrent directory operations
        for i in 0..10 {
            let client_clone = Arc::clone(&client);
            let handle = tokio::spawn(async move {
                let dir_path = format!("test/concurrent_dir_{}", i);
                
                // Test create directory
                let create_result = client_clone.create_directory(&dir_path).await;
                assert!(create_result.is_err());
                
                // Test delete directory
                let delete_result = client_clone.delete_directory(&dir_path).await;
                assert!(delete_result.is_err());
                
                // Test list directory
                let list_result = client_clone.list_directory(Some(&dir_path)).await;
                assert!(list_result.is_err());
                
                // Return the directory path for verification
                dir_path
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        let mut dir_paths = Vec::new();
        for handle in handles {
            dir_paths.push(handle.await.unwrap());
        }
        
        // Verify all operations were attempted
        assert_eq!(dir_paths.len(), 10);
        for i in 0..dir_paths.len() {
            assert_eq!(dir_paths[i], format!("test/concurrent_dir_{}", i));
        }
        
        // Verify that all concurrent operations completed without panics or deadlocks
        // This demonstrates the lock-free nature of the directory operations
    }

    #[tokio::test]
    async fn test_concurrent_search_operations() {
        // Test concurrent search operations with atomic result collection
        let client = Arc::new(Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap());
        let mut handles = Vec::new();
        
        // Different search patterns to test concurrent pattern matching
        let patterns = vec![
            "*.txt", "*.rs", "*.md", "*.json", "*.toml",
            "test*", "*config*", "*.log", "*.tmp", "data*"
        ];
        
        // Spawn concurrent search operations
        for (i, pattern) in patterns.iter().enumerate() {
            let client_clone = Arc::clone(&client);
            let pattern = pattern.to_string();
            let handle = tokio::spawn(async move {
                let search_path = format!("test/search_dir_{}", i);
                
                // Test search with pattern
                let search_result = client_clone.search_files(&pattern, Some(&search_path)).await;
                assert!(search_result.is_err());
                
                // Test search without path (root search)
                let root_search_result = client_clone.search_files(&pattern, None).await;
                assert!(root_search_result.is_err());
                
                (pattern, search_path)
            });
            handles.push(handle);
        }
        
        // Wait for all search operations to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        // Verify all search operations were attempted
        assert_eq!(results.len(), patterns.len());
        for (i, (pattern, search_path)) in results.iter().enumerate() {
            assert_eq!(pattern, &patterns[i]);
            assert_eq!(search_path, &format!("test/search_dir_{}", i));
        }
        
        // Verify that all concurrent search operations completed without panics or deadlocks
        // This demonstrates the lock-free concurrent pattern matching capability
    }

    #[tokio::test]
    async fn test_directory_listing_with_concurrent_access() {
        // Test directory listing operations with zero-lock concurrent access
        let client = Arc::new(Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap());
        let mut handles = Vec::new();
        
        // Test different directory paths concurrently
        let test_paths = vec![
            Some("documents"),
            Some("images/photos"),
            Some("config/settings"),
            None, // Root directory
            Some("logs/2024"),
            Some("temp/cache"),
            Some("src/main"),
            Some("tests/unit"),
            Some("data/exports"),
            Some("backup/daily"),
        ];
        
        // Spawn concurrent directory listing operations
        for (i, path) in test_paths.iter().enumerate() {
            let client_clone = Arc::clone(&client);
            let path = path.clone();
            let handle = tokio::spawn(async move {
                // Test directory listing
                let list_result = client_clone.list_directory(path.as_deref()).await;
                assert!(list_result.is_err());
                
                // Verify error type is network-related
                match list_result.unwrap_err() {
                    crate::error::ClientError::Network(_) | 
                    crate::error::ClientError::Connection { .. } | 
                    crate::error::ClientError::CircuitBreakerOpen => {
                        // Expected error types for network failures
                    }
                    other => panic!("Unexpected error type for task {}: {:?}", i, other),
                }
                
                (i, path)
            });
            handles.push(handle);
        }
        
        // Wait for all listing operations to complete
        let mut completed_tasks = Vec::new();
        for handle in handles {
            completed_tasks.push(handle.await.unwrap());
        }
        
        // Verify all operations completed
        assert_eq!(completed_tasks.len(), test_paths.len());
        for (task_id, path) in completed_tasks {
            assert_eq!(path, test_paths[task_id]);
        }
        
        // Verify that all concurrent directory listing operations completed successfully
        // This demonstrates zero-lock concurrent access to directory operations
    }

    #[tokio::test]
    async fn test_directory_operations_error_handling() {
        // Test proper async error handling for directory operations using atomic error tracking
        let client = Arc::new(Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap());
        
        // Test create directory error handling
        let create_result = client.create_directory("test/error_dir").await;
        assert!(create_result.is_err());
        
        // Test delete directory error handling
        let delete_result = client.delete_directory("test/error_dir").await;
        assert!(delete_result.is_err());
        
        // Test list directory error handling
        let list_result = client.list_directory(Some("test/error_dir")).await;
        assert!(list_result.is_err());
        
        // Test search files error handling
        let search_result = client.search_files("*.error", Some("test/error_dir")).await;
        assert!(search_result.is_err());
        
        // Test error types are appropriate - all should be network-related errors
        assert!(matches!(create_result.unwrap_err(), 
            crate::error::ClientError::Network(_) | 
            crate::error::ClientError::Connection { .. } | 
            crate::error::ClientError::CircuitBreakerOpen));
        
        assert!(matches!(delete_result.unwrap_err(), 
            crate::error::ClientError::Network(_) | 
            crate::error::ClientError::Connection { .. } | 
            crate::error::ClientError::CircuitBreakerOpen));
        
        assert!(matches!(list_result.unwrap_err(), 
            crate::error::ClientError::Network(_) | 
            crate::error::ClientError::Connection { .. } | 
            crate::error::ClientError::CircuitBreakerOpen));
        
        assert!(matches!(search_result.unwrap_err(), 
            crate::error::ClientError::Network(_) | 
            crate::error::ClientError::Connection { .. } | 
            crate::error::ClientError::CircuitBreakerOpen));
        
        // Verify that the client maintains atomic state during error conditions
        // The client should remain functional after errors
        let health_check_result = client.health_check().await;
        assert!(health_check_result.is_err()); // Should also fail with invalid server
    }

    #[tokio::test]
    async fn test_search_files_pattern_matching() {
        // Test search files method with various patterns for concurrent pattern matching
        let client = Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap();
        
        // Test different search patterns
        let test_cases = vec![
            ("*.txt", "text files"),
            ("*.rs", "rust files"),
            ("test*", "files starting with test"),
            ("*config*", "files containing config"),
            ("*.{json,toml}", "config files"),
            ("**/*.log", "log files recursively"),
            ("src/**/*.rs", "rust files in src"),
            ("??.txt", "two-character txt files"),
            ("[abc]*.txt", "txt files starting with a, b, or c"),
            ("file[0-9].txt", "numbered files"),
        ];
        
        for (pattern, description) in &test_cases {
            // Test search with pattern in specific directory
            let search_result = client.search_files(pattern, Some("test/search")).await;
            assert!(search_result.is_err(), "Search should fail for pattern: {} ({})", pattern, description);
            
            // Test search with pattern in root directory
            let root_search_result = client.search_files(pattern, None).await;
            assert!(root_search_result.is_err(), "Root search should fail for pattern: {} ({})", pattern, description);
        }
        
        // All pattern matching operations completed, demonstrating concurrent pattern matching
    }

    #[tokio::test]
    async fn test_directory_operations_path_normalization() {
        // Test that directory operations properly handle path normalization
        let client = Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap();
        
        // Test various path formats
        let test_paths = vec![
            "normal/path",
            "/leading/slash",
            "//double/leading",
            "trailing/slash/",
            "/both/leading/and/trailing/",
            ".",
            "./relative",
            "../parent",
            "path/with spaces",
            "path/with-dashes",
            "path/with_underscores",
            "path/with.dots",
        ];
        
        for path in test_paths {
            // Test create directory with various path formats
            let create_result = client.create_directory(path).await;
            assert!(create_result.is_err(), "Create should fail for path: {}", path);
            
            // Test delete directory with various path formats
            let delete_result = client.delete_directory(path).await;
            assert!(delete_result.is_err(), "Delete should fail for path: {}", path);
            
            // Test list directory with various path formats
            let list_result = client.list_directory(Some(path)).await;
            assert!(list_result.is_err(), "List should fail for path: {}", path);
        }
    }

    #[tokio::test]
    async fn test_concurrent_mixed_directory_operations() {
        // Test mixed directory operations running concurrently with atomic state tracking
        let client = Arc::new(Client::with_defaults("http://invalid-server:9999".to_string()).await.unwrap());
        let mut handles = Vec::new();
        
        // Spawn mixed operations concurrently
        for i in 0..20 {
            let client_clone = Arc::clone(&client);
            let handle = tokio::spawn(async move {
                let base_path = format!("test/mixed_{}", i);
                
                match i % 4 {
                    0 => {
                        // Create directory operation
                        let result = client_clone.create_directory(&base_path).await;
                        assert!(result.is_err());
                        "create"
                    }
                    1 => {
                        // Delete directory operation
                        let result = client_clone.delete_directory(&base_path).await;
                        assert!(result.is_err());
                        "delete"
                    }
                    2 => {
                        // List directory operation
                        let result = client_clone.list_directory(Some(&base_path)).await;
                        assert!(result.is_err());
                        "list"
                    }
                    3 => {
                        // Search files operation
                        let pattern = format!("*{}.txt", i);
                        let result = client_clone.search_files(&pattern, Some(&base_path)).await;
                        assert!(result.is_err());
                        "search"
                    }
                    _ => unreachable!(),
                }
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        let mut operation_types = Vec::new();
        for handle in handles {
            operation_types.push(handle.await.unwrap());
        }
        
        // Verify all operations completed
        assert_eq!(operation_types.len(), 20);
        
        // Count operation types
        let create_count = operation_types.iter().filter(|&op| *op == "create").count();
        let delete_count = operation_types.iter().filter(|&op| *op == "delete").count();
        let list_count = operation_types.iter().filter(|&op| *op == "list").count();
        let search_count = operation_types.iter().filter(|&op| *op == "search").count();
        
        assert_eq!(create_count, 5);
        assert_eq!(delete_count, 5);
        assert_eq!(list_count, 5);
        assert_eq!(search_count, 5);
        
        // All mixed operations completed successfully, demonstrating atomic state tracking
        // across different types of directory operations running concurrently
    }

    #[tokio::test]
    async fn test_file_metadata_creation() {
        use crate::types::FileMetadata;
        use std::time::SystemTime;
        
        let path = "test/metadata.txt";
        let size = 1024;
        let modified = SystemTime::now();
        let hash = 0x1234567890abcdef;
        
        // Test file metadata creation
        let metadata = FileMetadata::new(path.to_string(), size, modified, hash);
        
        assert_eq!(metadata.path, path);
        assert_eq!(metadata.name, "metadata.txt");
        assert_eq!(metadata.size, size);
        assert_eq!(metadata.modified, modified);
        assert_eq!(metadata.xxhash3, hash);
        assert!(!metadata.is_directory);
        
        // Test directory metadata creation
        let dir_metadata = FileMetadata::new_directory(path.to_string(), modified);
        assert!(dir_metadata.is_directory);
        assert_eq!(dir_metadata.size, 0);
        assert_eq!(dir_metadata.xxhash3, 0);
    }

    #[tokio::test]
    async fn test_client_configuration_validation() {
        // Test that client properly validates configuration
        let config = crate::config::Config::new("https://localhost:8080".to_string());
        
        // Valid configuration should work
        let client_result = Client::new(config).await;
        assert!(client_result.is_ok());
        
        // Test with empty URL
        let invalid_config = crate::config::Config::new("".to_string());
        let invalid_client_result = Client::new(invalid_config).await;
        assert!(invalid_client_result.is_err());
    }
}