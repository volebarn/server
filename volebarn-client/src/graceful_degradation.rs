//! Graceful degradation with read-only mode and cached metadata
//! 
//! This module provides graceful degradation capabilities when the server
//! is unavailable, including read-only mode and cached metadata management.

use crate::error::{ClientError, ClientResult};
use crate::types::{FileManifest, FileMetadata};
use bytes::Bytes;
use dashmap::DashMap;
use rocksdb::{DB, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::{interval, Instant};
use tracing::{debug, info, warn, error, instrument};

/// Degradation modes for client operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationMode {
    /// Normal operation - full functionality
    Normal,
    /// Read-only mode - no write operations allowed
    ReadOnly,
    /// Offline mode - use cached data only
    Offline,
    /// Emergency mode - minimal functionality
    Emergency,
}

/// Cached metadata entry with expiration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct CachedMetadata {
    pub metadata: crate::types::SerializableFileMetadata,
    pub cached_at: u64,     // Seconds since epoch
    pub expires_at: u64,    // Seconds since epoch
    pub access_count: u32,
    pub last_accessed: u64, // Seconds since epoch
}

/// Cached file content with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct CachedFile {
    pub path: String,
    pub content: Vec<u8>,   // Serialized bytes for persistence
    pub metadata: crate::types::SerializableFileMetadata,
    pub cached_at: u64,
    pub expires_at: u64,
    pub access_count: u32,
    pub last_accessed: u64,
}

/// Statistics for graceful degradation
#[derive(Debug, Clone)]
pub struct DegradationStats {
    pub current_mode: DegradationMode,
    pub cache_hit_rate: f64,
    pub cached_files: u32,
    pub cached_metadata: u32,
    pub cache_size_bytes: u64,
    pub offline_duration: Duration,
    pub last_server_contact: Option<SystemTime>,
    pub degradation_triggers: u32,
    pub recovery_attempts: u32,
}

/// Graceful degradation manager
#[derive(Debug)]
pub struct GracefulDegradationManager {
    /// Current degradation mode
    current_mode: Arc<AtomicU32>, // DegradationMode as u32
    /// RocksDB for persistent cache
    cache_db: Arc<DB>,
    /// In-memory metadata cache (path -> cached_metadata)
    metadata_cache: DashMap<String, CachedMetadata>,
    /// In-memory file cache (path -> cached_file)
    file_cache: DashMap<String, CachedFile>,
    /// Cache configuration
    max_cache_size: Arc<AtomicU64>,
    max_cache_entries: Arc<AtomicU32>,
    default_ttl: Duration,
    /// Statistics
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
    degradation_triggers: Arc<AtomicU32>,
    recovery_attempts: Arc<AtomicU32>,
    offline_since: Arc<AtomicU64>, // Timestamp when went offline
    last_server_contact: Arc<AtomicU64>, // Last successful server contact
    /// Shutdown signal
    shutdown_tx: Arc<broadcast::Sender<()>>,
}

impl GracefulDegradationManager {
    /// Create a new graceful degradation manager
    pub async fn new(
        cache_path: PathBuf,
        max_cache_size: u64,
        max_cache_entries: u32,
        default_ttl: Duration,
    ) -> ClientResult<Self> {
        // Create cache directory if it doesn't exist
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| ClientError::Io {
                    path: parent.to_string_lossy().to_string(),
                    error: e.to_string(),
                })?;
        }

        // Configure RocksDB for cache
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        opts.set_write_buffer_size(32 * 1024 * 1024); // 32MB
        opts.set_max_write_buffer_number(2);
        opts.set_target_file_size_base(128 * 1024 * 1024); // 128MB

        // Open cache database
        let cache_db = DB::open(&opts, &cache_path)
            .map_err(|e| ClientError::Io {
                path: cache_path.to_string_lossy().to_string(),
                error: e.to_string(),
            })?;

        let cache_db = Arc::new(cache_db);
        let metadata_cache = DashMap::new();
        let file_cache = DashMap::new();

        // Load existing cache entries
        let mut loaded_metadata = 0u32;
        let mut loaded_files = 0u32;
        let mut cache_size = 0u64;

        let iter = cache_db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item.map_err(|e| ClientError::Io {
                path: "cache_db".to_string(),
                error: e.to_string(),
            })?;

            let key_str = String::from_utf8_lossy(&key);
            
            if key_str.starts_with("meta:") {
                // Metadata entry
                let path = key_str.strip_prefix("meta:").unwrap().to_string();
                match bitcode::decode::<CachedMetadata>(&value) {
                    Ok(cached_meta) => {
                        // Check if not expired
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        
                        if cached_meta.expires_at > now {
                            metadata_cache.insert(path, cached_meta);
                            loaded_metadata += 1;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to deserialize cached metadata for {}: {}", path, e);
                    }
                }
            } else if key_str.starts_with("file:") {
                // File content entry
                let path = key_str.strip_prefix("file:").unwrap().to_string();
                match bitcode::decode::<CachedFile>(&value) {
                    Ok(cached_file) => {
                        // Check if not expired
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        
                        if cached_file.expires_at > now {
                            cache_size += cached_file.content.len() as u64;
                            file_cache.insert(path, cached_file);
                            loaded_files += 1;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to deserialize cached file for {}: {}", path, e);
                    }
                }
            }
        }

        let (shutdown_tx, _) = broadcast::channel(16);

        info!(
            "Loaded cache: {} metadata entries, {} files ({} bytes)",
            loaded_metadata, loaded_files, cache_size
        );

        Ok(Self {
            current_mode: Arc::new(AtomicU32::new(DegradationMode::Normal as u32)),
            cache_db,
            metadata_cache,
            file_cache,
            max_cache_size: Arc::new(AtomicU64::new(max_cache_size)),
            max_cache_entries: Arc::new(AtomicU32::new(max_cache_entries)),
            default_ttl,
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            degradation_triggers: Arc::new(AtomicU32::new(0)),
            recovery_attempts: Arc::new(AtomicU32::new(0)),
            offline_since: Arc::new(AtomicU64::new(0)),
            last_server_contact: Arc::new(AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            )),
            shutdown_tx: Arc::new(shutdown_tx),
        })
    }

    /// Get current degradation mode
    pub fn current_mode(&self) -> DegradationMode {
        match self.current_mode.load(Ordering::Acquire) {
            0 => DegradationMode::Normal,
            1 => DegradationMode::ReadOnly,
            2 => DegradationMode::Offline,
            3 => DegradationMode::Emergency,
            _ => DegradationMode::Emergency,
        }
    }

    /// Set degradation mode
    #[instrument(skip(self))]
    pub async fn set_mode(&self, mode: DegradationMode) -> ClientResult<()> {
        let old_mode = self.current_mode();
        self.current_mode.store(mode as u32, Ordering::Release);

        if mode != DegradationMode::Normal && old_mode == DegradationMode::Normal {
            // Entering degraded mode
            self.degradation_triggers.fetch_add(1, Ordering::Relaxed);
            
            if mode == DegradationMode::Offline {
                self.offline_since.store(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    Ordering::Relaxed
                );
            }
        }

        info!("Degradation mode changed: {:?} -> {:?}", old_mode, mode);
        Ok(())
    }

    /// Check if operation is allowed in current mode
    pub fn is_operation_allowed(&self, operation: &str) -> bool {
        match self.current_mode() {
            DegradationMode::Normal => true,
            DegradationMode::ReadOnly => {
                // Only allow read operations
                matches!(operation, "download" | "get_metadata" | "list_directory" | "search")
            }
            DegradationMode::Offline => {
                // Only allow cached read operations
                matches!(operation, "download_cached" | "get_cached_metadata" | "list_cached")
            }
            DegradationMode::Emergency => {
                // Only allow essential operations
                matches!(operation, "get_cached_metadata" | "health_check")
            }
        }
    }

    /// Cache file metadata
    #[instrument(skip(self))]
    pub async fn cache_metadata(&self, path: &str, metadata: FileMetadata) -> ClientResult<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let cached_metadata = CachedMetadata {
            metadata: metadata.into(),
            cached_at: now,
            expires_at: now + self.default_ttl.as_secs(),
            access_count: 0,
            last_accessed: now,
        };

        // Store in memory cache
        self.metadata_cache.insert(path.to_string(), cached_metadata.clone());

        // Persist to RocksDB
        let key = format!("meta:{}", path);
        let serialized = bitcode::encode(&cached_metadata);
        self.cache_db.put(&key, &serialized)
            .map_err(|e| ClientError::Io {
                path: "cache_db".to_string(),
                error: e.to_string(),
            })?;

        debug!("Cached metadata for: {}", path);
        Ok(())
    }

    /// Get cached metadata
    #[instrument(skip(self))]
    pub async fn get_cached_metadata(&self, path: &str) -> Option<FileMetadata> {
        if let Some(mut cached) = self.metadata_cache.get_mut(path) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // Check if expired
            if cached.expires_at <= now {
                drop(cached);
                self.metadata_cache.remove(path);
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            // Update access statistics
            cached.access_count += 1;
            cached.last_accessed = now;
            
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            debug!("Cache hit for metadata: {}", path);
            Some(cached.metadata.clone().into())
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            debug!("Cache miss for metadata: {}", path);
            None
        }
    }

    /// Cache file content
    #[instrument(skip(self, content))]
    pub async fn cache_file(&self, path: &str, content: Bytes, metadata: FileMetadata) -> ClientResult<()> {
        // Check cache size limits
        let current_entries = self.file_cache.len() as u32;
        let max_entries = self.max_cache_entries.load(Ordering::Relaxed);
        
        if current_entries >= max_entries {
            self.evict_lru_files().await?;
        }

        let content_size = content.len() as u64;
        let max_size = self.max_cache_size.load(Ordering::Relaxed);
        let current_size = self.calculate_cache_size();
        
        if current_size + content_size > max_size {
            self.evict_files_by_size(content_size).await?;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let cached_file = CachedFile {
            path: path.to_string(),
            content: content.to_vec(),
            metadata: metadata.into(),
            cached_at: now,
            expires_at: now + self.default_ttl.as_secs(),
            access_count: 0,
            last_accessed: now,
        };

        // Store in memory cache
        self.file_cache.insert(path.to_string(), cached_file.clone());

        // Persist to RocksDB
        let key = format!("file:{}", path);
        let serialized = bitcode::encode(&cached_file);
        self.cache_db.put(&key, &serialized)
            .map_err(|e| ClientError::Io {
                path: "cache_db".to_string(),
                error: e.to_string(),
            })?;

        debug!("Cached file content for: {} ({} bytes)", path, content_size);
        Ok(())
    }

    /// Get cached file content
    #[instrument(skip(self))]
    pub async fn get_cached_file(&self, path: &str) -> Option<Bytes> {
        if let Some(mut cached) = self.file_cache.get_mut(path) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // Check if expired
            if cached.expires_at <= now {
                drop(cached);
                self.file_cache.remove(path);
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            // Update access statistics
            cached.access_count += 1;
            cached.last_accessed = now;
            
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            debug!("Cache hit for file: {}", path);
            Some(Bytes::from(cached.content.clone()))
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            debug!("Cache miss for file: {}", path);
            None
        }
    }

    /// Get cached manifest (list of all cached files)
    pub async fn get_cached_manifest(&self) -> FileManifest {
        let mut files = std::collections::HashMap::new();
        
        for entry in self.metadata_cache.iter() {
            let (path, cached_meta) = (entry.key(), entry.value());
            files.insert(path.clone(), cached_meta.metadata.clone().into());
        }

        FileManifest { files }
    }

    /// Record successful server contact
    pub async fn record_server_contact(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        self.last_server_contact.store(now, Ordering::Relaxed);
        
        // If we were offline, transition back to normal mode
        if self.current_mode() != DegradationMode::Normal {
            let _ = self.set_mode(DegradationMode::Normal).await;
            self.offline_since.store(0, Ordering::Relaxed);
            info!("Recovered from degraded mode - server contact restored");
        }
    }

    /// Record server failure and potentially trigger degradation
    pub async fn record_server_failure(&self, error: &ClientError) -> ClientResult<()> {
        // Determine appropriate degradation mode based on error type
        let suggested_mode = match error {
            ClientError::Network(_) | ClientError::Connection { .. } => {
                DegradationMode::Offline
            }
            ClientError::Server { status, .. } if *status >= 500 => {
                DegradationMode::ReadOnly
            }
            ClientError::CircuitBreakerOpen => {
                DegradationMode::ReadOnly
            }
            ClientError::Timeout { .. } => {
                DegradationMode::ReadOnly
            }
            _ => return Ok(()), // Don't degrade for other error types
        };

        // Only degrade if we're currently in normal mode
        if self.current_mode() == DegradationMode::Normal {
            self.set_mode(suggested_mode).await?;
            warn!("Triggered degradation to {:?} due to: {}", suggested_mode, error);
        }

        Ok(())
    }

    /// Attempt to recover from degraded mode
    pub async fn attempt_recovery<F, Fut>(&self, health_check: F) -> ClientResult<bool>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = ClientResult<bool>>,
    {
        if self.current_mode() == DegradationMode::Normal {
            return Ok(true); // Already recovered
        }

        self.recovery_attempts.fetch_add(1, Ordering::Relaxed);

        match health_check().await {
            Ok(true) => {
                // Health check succeeded, recover to normal mode
                self.record_server_contact().await;
                info!("Recovery successful - returning to normal mode");
                Ok(true)
            }
            Ok(false) => {
                debug!("Recovery attempt failed - server not healthy");
                Ok(false)
            }
            Err(e) => {
                debug!("Recovery attempt failed with error: {}", e);
                Ok(false)
            }
        }
    }

    /// Get degradation statistics
    pub async fn stats(&self) -> DegradationStats {
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_requests = cache_hits + cache_misses;
        
        let cache_hit_rate = if total_requests > 0 {
            cache_hits as f64 / total_requests as f64
        } else {
            0.0
        };

        let offline_since = self.offline_since.load(Ordering::Relaxed);
        let offline_duration = if offline_since > 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Duration::from_secs(now.saturating_sub(offline_since))
        } else {
            Duration::ZERO
        };

        let last_contact_timestamp = self.last_server_contact.load(Ordering::Relaxed);
        let last_server_contact = if last_contact_timestamp > 0 {
            Some(UNIX_EPOCH + Duration::from_secs(last_contact_timestamp))
        } else {
            None
        };

        DegradationStats {
            current_mode: self.current_mode(),
            cache_hit_rate,
            cached_files: self.file_cache.len() as u32,
            cached_metadata: self.metadata_cache.len() as u32,
            cache_size_bytes: self.calculate_cache_size(),
            offline_duration,
            last_server_contact,
            degradation_triggers: self.degradation_triggers.load(Ordering::Relaxed),
            recovery_attempts: self.recovery_attempts.load(Ordering::Relaxed),
        }
    }

    /// Calculate current cache size
    fn calculate_cache_size(&self) -> u64 {
        self.file_cache.iter()
            .map(|entry| entry.value().content.len() as u64)
            .sum()
    }

    /// Evict least recently used files
    async fn evict_lru_files(&self) -> ClientResult<()> {
        let mut files_by_access: Vec<_> = self.file_cache.iter()
            .map(|entry| (entry.key().clone(), entry.value().last_accessed))
            .collect();

        // Sort by last accessed time (oldest first)
        files_by_access.sort_by_key(|(_, last_accessed)| *last_accessed);

        // Remove oldest 25% of files
        let to_remove = (files_by_access.len() / 4).max(1);
        
        for (path, _) in files_by_access.into_iter().take(to_remove) {
            self.remove_cached_file(&path).await?;
        }

        debug!("Evicted {} LRU files from cache", to_remove);
        Ok(())
    }

    /// Evict files to make space for new content
    async fn evict_files_by_size(&self, needed_size: u64) -> ClientResult<()> {
        let mut freed_size = 0u64;
        let mut files_by_access: Vec<_> = self.file_cache.iter()
            .map(|entry| {
                let path = entry.key().clone();
                let cached_file = entry.value();
                (path, cached_file.last_accessed, cached_file.content.len() as u64)
            })
            .collect();

        // Sort by last accessed time (oldest first)
        files_by_access.sort_by_key(|(_, last_accessed, _)| *last_accessed);

        for (path, _, size) in files_by_access {
            self.remove_cached_file(&path).await?;
            freed_size += size;
            
            if freed_size >= needed_size {
                break;
            }
        }

        debug!("Evicted files to free {} bytes (needed {})", freed_size, needed_size);
        Ok(())
    }

    /// Remove a cached file
    async fn remove_cached_file(&self, path: &str) -> ClientResult<()> {
        // Remove from memory cache
        self.file_cache.remove(path);

        // Remove from persistent cache
        let key = format!("file:{}", path);
        self.cache_db.delete(&key)
            .map_err(|e| ClientError::Io {
                path: "cache_db".to_string(),
                error: e.to_string(),
            })?;

        Ok(())
    }

    /// Clear all cached data
    pub async fn clear_cache(&self) -> ClientResult<()> {
        // Clear memory caches
        self.metadata_cache.clear();
        self.file_cache.clear();

        // Clear persistent cache
        let iter = self.cache_db.iterator(rocksdb::IteratorMode::Start);
        let mut batch = WriteBatch::default();
        
        for item in iter {
            if let Ok((key, _)) = item {
                batch.delete(&key);
            }
        }

        self.cache_db.write(batch)
            .map_err(|e| ClientError::Io {
                path: "cache_db".to_string(),
                error: e.to_string(),
            })?;

        info!("Cleared all cached data");
        Ok(())
    }

    /// Start background maintenance tasks
    pub async fn start_maintenance(&self) -> ClientResult<()> {
        let manager = self.clone_for_background();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(300)); // 5 minutes
            let mut recovery_interval = interval(Duration::from_secs(30));  // 30 seconds

            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        if let Err(e) = manager.cleanup_expired_entries().await {
                            error!("Cache cleanup failed: {}", e);
                        }
                    }
                    _ = recovery_interval.tick() => {
                        if manager.current_mode() != DegradationMode::Normal {
                            // Attempt recovery with a simple health check
                            let _ = manager.attempt_recovery(|| async {
                                // This would be replaced with actual health check
                                Ok(false)
                            }).await;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Graceful degradation maintenance task shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Clean up expired cache entries
    async fn cleanup_expired_entries(&self) -> ClientResult<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut expired_metadata = Vec::new();
        let mut expired_files = Vec::new();

        // Find expired metadata
        for entry in self.metadata_cache.iter() {
            let (path, cached_meta) = (entry.key(), entry.value());
            if cached_meta.expires_at <= now {
                expired_metadata.push(path.clone());
            }
        }

        // Find expired files
        for entry in self.file_cache.iter() {
            let (path, cached_file) = (entry.key(), entry.value());
            if cached_file.expires_at <= now {
                expired_files.push(path.clone());
            }
        }

        // Count expired entries before processing
        let expired_metadata_count = expired_metadata.len();
        let expired_files_count = expired_files.len();

        // Remove expired entries
        for path in &expired_metadata {
            self.metadata_cache.remove(path);
            let key = format!("meta:{}", path);
            let _ = self.cache_db.delete(&key);
        }

        for path in &expired_files {
            let _ = self.remove_cached_file(path).await;
        }

        if expired_metadata_count > 0 || expired_files_count > 0 {
            debug!(
                "Cleaned up {} expired metadata entries and {} expired files",
                expired_metadata_count,
                expired_files_count
            );
        }

        Ok(())
    }

    /// Stop maintenance tasks
    pub async fn stop(&self) -> ClientResult<()> {
        let _ = self.shutdown_tx.send(());
        info!("Graceful degradation manager stopped");
        Ok(())
    }

    /// Clone for background tasks
    fn clone_for_background(&self) -> Self {
        Self {
            current_mode: self.current_mode.clone(),
            cache_db: self.cache_db.clone(),
            metadata_cache: self.metadata_cache.clone(),
            file_cache: self.file_cache.clone(),
            max_cache_size: self.max_cache_size.clone(),
            max_cache_entries: self.max_cache_entries.clone(),
            default_ttl: self.default_ttl,
            cache_hits: self.cache_hits.clone(),
            cache_misses: self.cache_misses.clone(),
            degradation_triggers: self.degradation_triggers.clone(),
            recovery_attempts: self.recovery_attempts.clone(),
            offline_since: self.offline_since.clone(),
            last_server_contact: self.last_server_contact.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }
}

impl Drop for GracefulDegradationManager {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_degradation_mode_transitions() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("test_cache");
        
        let manager = GracefulDegradationManager::new(
            cache_path,
            10 * 1024 * 1024, // 10MB cache
            1000,              // 1000 entries
            Duration::from_secs(3600), // 1 hour TTL
        ).await.unwrap();

        // Initially normal mode
        assert_eq!(manager.current_mode(), DegradationMode::Normal);

        // Transition to read-only
        manager.set_mode(DegradationMode::ReadOnly).await.unwrap();
        assert_eq!(manager.current_mode(), DegradationMode::ReadOnly);

        // Check operation permissions
        assert!(manager.is_operation_allowed("download"));
        assert!(!manager.is_operation_allowed("upload"));

        // Transition to offline
        manager.set_mode(DegradationMode::Offline).await.unwrap();
        assert_eq!(manager.current_mode(), DegradationMode::Offline);
        assert!(!manager.is_operation_allowed("download"));
        assert!(manager.is_operation_allowed("download_cached"));
    }

    #[tokio::test]
    async fn test_metadata_caching() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("metadata_cache");
        
        let manager = GracefulDegradationManager::new(
            cache_path,
            10 * 1024 * 1024,
            1000,
            Duration::from_secs(60), // 1 minute TTL for testing
        ).await.unwrap();

        let metadata = FileMetadata {
            path: "test.txt".to_string(),
            name: "test.txt".to_string(),
            size: 1024,
            modified: SystemTime::now(),
            is_directory: false,
            xxhash3: 12345,
        };

        // Cache metadata
        manager.cache_metadata("test.txt", metadata.clone()).await.unwrap();

        // Retrieve cached metadata
        let cached = manager.get_cached_metadata("test.txt").await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().path, "test.txt");

        // Check cache miss
        let missing = manager.get_cached_metadata("missing.txt").await;
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_file_caching() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("file_cache");
        
        let manager = GracefulDegradationManager::new(
            cache_path,
            10 * 1024 * 1024,
            1000,
            Duration::from_secs(60),
        ).await.unwrap();

        let content = Bytes::from("test file content");
        let metadata = FileMetadata {
            path: "test.txt".to_string(),
            name: "test.txt".to_string(),
            size: content.len() as u64,
            modified: SystemTime::now(),
            is_directory: false,
            xxhash3: 12345,
        };

        // Cache file
        manager.cache_file("test.txt", content.clone(), metadata).await.unwrap();

        // Retrieve cached file
        let cached = manager.get_cached_file("test.txt").await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap(), content);

        // Check cache miss
        let missing = manager.get_cached_file("missing.txt").await;
        assert!(missing.is_none());
    }
}