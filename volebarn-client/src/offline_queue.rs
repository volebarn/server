//! Offline queue management with persistence to disk
//! 
//! This module provides persistent offline queue management for failed sync operations
//! using RocksDB for durability and lock-free operations for performance.

use crate::error::{ClientError, ClientResult};
use bytes::Bytes;
use dashmap::DashMap;
use rocksdb::{DB, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn, error, instrument};
use uuid::Uuid;

/// Serializable offline operation for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct OfflineOperation {
    pub id: String,
    pub operation_type: OperationType,
    pub path: String,
    pub data: Option<Vec<u8>>, // Serialized bytes for persistence
    pub timestamp: u64,        // Seconds since epoch
    pub retry_count: u32,
    pub priority: OperationPriority,
    pub metadata: Option<OperationMetadata>,
}

/// Types of operations that can be queued offline
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub enum OperationType {
    Upload,
    Download,
    Delete,
    Move { from: String, to: String },
    Copy { from: String, to: String },
    CreateDirectory,
    DeleteDirectory,
    Sync,
}

impl OperationType {
    pub fn as_str(&self) -> &str {
        match self {
            OperationType::Upload => "upload",
            OperationType::Download => "download",
            OperationType::Delete => "delete",
            OperationType::Move { .. } => "move",
            OperationType::Copy { .. } => "copy",
            OperationType::CreateDirectory => "create_directory",
            OperationType::DeleteDirectory => "delete_directory",
            OperationType::Sync => "sync",
        }
    }
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Priority levels for offline operations
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub enum OperationPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Additional metadata for operations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct OperationMetadata {
    pub file_size: Option<u64>,
    pub expected_hash: Option<u64>,
    pub chunk_info: Option<ChunkInfo>,
    pub conflict_resolution: Option<String>,
}

/// Information about chunked uploads for resume capability
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(bitcode::Encode, bitcode::Decode)]
pub struct ChunkInfo {
    pub total_chunks: u32,
    pub chunk_size: u64,
    pub completed_chunks: Vec<u32>,
    pub upload_id: String,
}

/// Statistics for offline queue operations
#[derive(Debug, Clone)]
pub struct OfflineQueueStats {
    pub total_operations: u32,
    pub pending_operations: u32,
    pub failed_operations: u32,
    pub completed_operations: u32,
    pub total_size_bytes: u64,
    pub oldest_operation_age: Duration,
    pub retry_operations: u32,
}

/// Persistent offline queue manager using RocksDB
#[derive(Debug)]
pub struct OfflineQueue {
    /// RocksDB instance for persistence
    db: Arc<DB>,
    /// In-memory index for fast access (operation_id -> priority)
    memory_index: DashMap<String, OperationPriority>,
    /// Maximum queue size
    max_size: Arc<AtomicU32>,
    /// Current queue size
    current_size: Arc<AtomicU32>,
    /// Total bytes in queue
    total_bytes: Arc<AtomicU64>,
    /// Statistics counters
    pending_operations: Arc<AtomicU32>,
    failed_operations: Arc<AtomicU32>,
    completed_operations: Arc<AtomicU32>,
    retry_operations: Arc<AtomicU32>,
    /// Shutdown signal
    shutdown_tx: Arc<broadcast::Sender<()>>,
    /// Processing interval
    processing_interval: Duration,
}

impl OfflineQueue {
    /// Create a new offline queue with persistence
    pub async fn new(
        storage_path: PathBuf,
        max_size: u32,
        processing_interval: Duration,
    ) -> ClientResult<Self> {
        // Create storage directory if it doesn't exist
        if let Some(parent) = storage_path.parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| ClientError::Io {
                    path: parent.to_string_lossy().to_string(),
                    error: e.to_string(),
                })?;
        }

        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        opts.set_write_buffer_size(16 * 1024 * 1024); // 16MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB

        // Open database
        let db = DB::open(&opts, &storage_path)
            .map_err(|e| ClientError::Io {
                path: storage_path.to_string_lossy().to_string(),
                error: e.to_string(),
            })?;

        let db = Arc::new(db);
        let memory_index = DashMap::new();

        // Load existing operations into memory index
        let mut current_size = 0u32;
        let mut total_bytes = 0u64;
        let mut pending_ops = 0u32;
        let mut failed_ops = 0u32;
        let mut retry_ops = 0u32;

        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item.map_err(|e| ClientError::Io {
                path: "offline_queue".to_string(),
                error: e.to_string(),
            })?;

            let operation_id = String::from_utf8_lossy(&key).to_string();
            
            match bitcode::decode::<OfflineOperation>(&value) {
                Ok(operation) => {
                    memory_index.insert(operation_id, operation.priority);
                    current_size += 1;
                    
                    if let Some(data) = &operation.data {
                        total_bytes += data.len() as u64;
                    }
                    
                    if operation.retry_count > 0 {
                        retry_ops += 1;
                    }
                    
                    pending_ops += 1;
                }
                Err(e) => {
                    warn!("Failed to deserialize operation {}: {}", operation_id, e);
                    failed_ops += 1;
                }
            }
        }

        let (shutdown_tx, _) = broadcast::channel(16);

        info!(
            "Loaded offline queue: {} operations, {} bytes, {} retries",
            current_size, total_bytes, retry_ops
        );

        Ok(Self {
            db,
            memory_index,
            max_size: Arc::new(AtomicU32::new(max_size)),
            current_size: Arc::new(AtomicU32::new(current_size)),
            total_bytes: Arc::new(AtomicU64::new(total_bytes)),
            pending_operations: Arc::new(AtomicU32::new(pending_ops)),
            failed_operations: Arc::new(AtomicU32::new(failed_ops)),
            completed_operations: Arc::new(AtomicU32::new(0)),
            retry_operations: Arc::new(AtomicU32::new(retry_ops)),
            shutdown_tx: Arc::new(shutdown_tx),
            processing_interval,
        })
    }

    /// Add an operation to the queue with persistence
    #[instrument(skip(self, data))]
    pub async fn enqueue(
        &self,
        operation_type: OperationType,
        path: String,
        data: Option<Bytes>,
        priority: OperationPriority,
        metadata: Option<OperationMetadata>,
    ) -> ClientResult<String> {
        // Check if queue is full
        let current_size = self.current_size.load(Ordering::Relaxed);
        let max_size = self.max_size.load(Ordering::Relaxed);

        if current_size >= max_size {
            // Try to remove lowest priority operations to make space
            self.cleanup_low_priority_operations().await?;
            
            // Check again after cleanup
            let current_size = self.current_size.load(Ordering::Relaxed);
            if current_size >= max_size {
                return Err(ClientError::ResourceLimit {
                    resource: "offline_queue".to_string(),
                    limit: format!("Maximum {} operations", max_size),
                });
            }
        }

        let operation_id = Uuid::new_v4().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let operation = OfflineOperation {
            id: operation_id.clone(),
            operation_type,
            path,
            data: data.map(|b| b.to_vec()),
            timestamp,
            retry_count: 0,
            priority,
            metadata,
        };

        // Serialize and store in RocksDB
        let serialized = bitcode::encode(&operation);
        self.db.put(&operation_id, &serialized)
            .map_err(|e| ClientError::Io {
                path: "offline_queue".to_string(),
                error: e.to_string(),
            })?;

        // Update in-memory index
        self.memory_index.insert(operation_id.clone(), priority);
        self.current_size.fetch_add(1, Ordering::Relaxed);
        self.pending_operations.fetch_add(1, Ordering::Relaxed);

        if let Some(data) = &operation.data {
            self.total_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        }

        debug!(
            "Enqueued offline operation: {} (type: {:?}, priority: {:?})",
            operation_id, operation.operation_type, priority
        );

        Ok(operation_id)
    }

    /// Get the next operation to process (highest priority first)
    pub async fn dequeue(&self) -> ClientResult<Option<OfflineOperation>> {
        if self.memory_index.is_empty() {
            return Ok(None);
        }

        // Find highest priority operation
        let mut highest_priority = OperationPriority::Low;
        let mut selected_id: Option<String> = None;

        for entry in self.memory_index.iter() {
            let (id, priority) = (entry.key(), *entry.value());
            if priority >= highest_priority {
                highest_priority = priority;
                selected_id = Some(id.clone());
            }
        }

        if let Some(operation_id) = selected_id {
            // Remove from memory index first
            self.memory_index.remove(&operation_id);

            // Get from RocksDB
            match self.db.get(&operation_id) {
                Ok(Some(data)) => {
                    match bitcode::decode::<OfflineOperation>(&data) {
                        Ok(operation) => {
                            debug!("Dequeued operation: {} (priority: {:?})", operation_id, operation.priority);
                            Ok(Some(operation))
                        }
                        Err(e) => {
                            error!("Failed to deserialize operation {}: {}", operation_id, e);
                            self.failed_operations.fetch_add(1, Ordering::Relaxed);
                            Ok(None)
                        }
                    }
                }
                Ok(None) => {
                    warn!("Operation {} not found in database", operation_id);
                    Ok(None)
                }
                Err(e) => Err(ClientError::Io {
                    path: "offline_queue".to_string(),
                    error: e.to_string(),
                }),
            }
        } else {
            Ok(None)
        }
    }

    /// Mark an operation as completed and remove it
    #[instrument(skip(self))]
    pub async fn complete_operation(&self, operation_id: &str) -> ClientResult<()> {
        // Remove from RocksDB
        self.db.delete(operation_id)
            .map_err(|e| ClientError::Io {
                path: "offline_queue".to_string(),
                error: e.to_string(),
            })?;

        // Remove from memory index if still there
        self.memory_index.remove(operation_id);

        self.current_size.fetch_sub(1, Ordering::Relaxed);
        self.pending_operations.fetch_sub(1, Ordering::Relaxed);
        self.completed_operations.fetch_add(1, Ordering::Relaxed);

        debug!("Completed offline operation: {}", operation_id);
        Ok(())
    }

    /// Retry an operation with incremented retry count
    #[instrument(skip(self))]
    pub async fn retry_operation(
        &self,
        mut operation: OfflineOperation,
        max_retries: u32,
    ) -> ClientResult<bool> {
        operation.retry_count += 1;

        if operation.retry_count > max_retries {
            // Remove operation that has exceeded retry limit
            self.db.delete(&operation.id)
                .map_err(|e| ClientError::Io {
                    path: "offline_queue".to_string(),
                    error: e.to_string(),
                })?;

            self.memory_index.remove(&operation.id);
            self.current_size.fetch_sub(1, Ordering::Relaxed);
            self.pending_operations.fetch_sub(1, Ordering::Relaxed);
            self.failed_operations.fetch_add(1, Ordering::Relaxed);

            warn!(
                "Operation {} exceeded retry limit ({}), removing from queue",
                operation.id, max_retries
            );
            return Ok(false);
        }

        // Update operation in RocksDB
        let serialized = bitcode::encode(&operation);
        self.db.put(&operation.id, &serialized)
            .map_err(|e| ClientError::Io {
                path: "offline_queue".to_string(),
                error: e.to_string(),
            })?;

        // Re-add to memory index
        self.memory_index.insert(operation.id.clone(), operation.priority);
        self.retry_operations.fetch_add(1, Ordering::Relaxed);

        debug!(
            "Retrying operation: {} (attempt {})",
            operation.id, operation.retry_count
        );
        Ok(true)
    }

    /// Get current queue statistics
    pub async fn stats(&self) -> OfflineQueueStats {
        let total_operations = self.current_size.load(Ordering::Relaxed);
        let pending_operations = self.pending_operations.load(Ordering::Relaxed);
        let failed_operations = self.failed_operations.load(Ordering::Relaxed);
        let completed_operations = self.completed_operations.load(Ordering::Relaxed);
        let total_size_bytes = self.total_bytes.load(Ordering::Relaxed);
        let retry_operations = self.retry_operations.load(Ordering::Relaxed);

        // Find oldest operation
        let mut oldest_timestamp = u64::MAX;
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            if let Ok((_, value)) = item {
                if let Ok(operation) = bitcode::decode::<OfflineOperation>(&value) {
                    oldest_timestamp = oldest_timestamp.min(operation.timestamp);
                }
            }
        }

        let oldest_operation_age = if oldest_timestamp == u64::MAX {
            Duration::ZERO
        } else {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Duration::from_secs(now.saturating_sub(oldest_timestamp))
        };

        OfflineQueueStats {
            total_operations,
            pending_operations,
            failed_operations,
            completed_operations,
            total_size_bytes,
            oldest_operation_age,
            retry_operations,
        }
    }

    /// Clean up low priority operations to make space
    async fn cleanup_low_priority_operations(&self) -> ClientResult<()> {
        let mut operations_to_remove = Vec::new();

        // Find low priority operations
        for entry in self.memory_index.iter() {
            let (id, priority) = (entry.key(), *entry.value());
            if priority == OperationPriority::Low {
                operations_to_remove.push(id.clone());
            }
        }

        // Remove up to 10% of low priority operations
        let max_to_remove = (operations_to_remove.len() / 10).max(1);
        operations_to_remove.truncate(max_to_remove);

        for operation_id in operations_to_remove {
            if let Err(e) = self.complete_operation(&operation_id).await {
                warn!("Failed to remove low priority operation {}: {}", operation_id, e);
            }
        }

        Ok(())
    }

    /// Clear all operations from the queue
    pub async fn clear(&self) -> ClientResult<()> {
        // Clear RocksDB
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        let mut batch = WriteBatch::default();
        
        for item in iter {
            if let Ok((key, _)) = item {
                batch.delete(&key);
            }
        }

        self.db.write(batch)
            .map_err(|e| ClientError::Io {
                path: "offline_queue".to_string(),
                error: e.to_string(),
            })?;

        // Clear memory index
        self.memory_index.clear();

        // Reset counters
        self.current_size.store(0, Ordering::Relaxed);
        self.total_bytes.store(0, Ordering::Relaxed);
        self.pending_operations.store(0, Ordering::Relaxed);

        info!("Cleared offline queue");
        Ok(())
    }

    /// Get all operations (for debugging/monitoring)
    pub async fn list_operations(&self) -> ClientResult<Vec<OfflineOperation>> {
        let mut operations = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| ClientError::Io {
                path: "offline_queue".to_string(),
                error: e.to_string(),
            })?;

            match bitcode::decode::<OfflineOperation>(&value) {
                Ok(operation) => operations.push(operation),
                Err(e) => {
                    warn!("Failed to deserialize operation: {}", e);
                }
            }
        }

        // Sort by priority (highest first) then by timestamp (oldest first)
        operations.sort_by(|a, b| {
            b.priority.cmp(&a.priority)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        Ok(operations)
    }

    /// Start background processing task
    pub async fn start_processing<F, Fut>(&self, processor: F) -> ClientResult<()>
    where
        F: Fn(OfflineOperation) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ClientResult<()>> + Send + 'static,
    {
        let queue = self.clone_for_background();
        let processor = Arc::new(processor);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = interval(queue.processing_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = queue.process_operations(&processor).await {
                            error!("Error processing offline operations: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Offline queue processing task shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Process operations using the provided processor function
    async fn process_operations<F, Fut>(&self, processor: &Arc<F>) -> ClientResult<()>
    where
        F: Fn(OfflineOperation) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ClientResult<()>> + Send + 'static,
    {
        // Process up to 10 operations per cycle
        for _ in 0..10 {
            if let Some(operation) = self.dequeue().await? {
                let operation_id = operation.id.clone();
                
                match processor(operation.clone()).await {
                    Ok(()) => {
                        // Operation succeeded, remove from queue
                        if let Err(e) = self.complete_operation(&operation_id).await {
                            error!("Failed to complete operation {}: {}", operation_id, e);
                        }
                    }
                    Err(e) => {
                        // Operation failed, retry if possible
                        debug!("Operation {} failed: {}", operation_id, e);
                        
                        if !self.retry_operation(operation, 5).await? {
                            error!("Operation {} permanently failed after retries", operation_id);
                        }
                    }
                }
            } else {
                // No more operations to process
                break;
            }
        }

        Ok(())
    }

    /// Stop background processing
    pub async fn stop(&self) -> ClientResult<()> {
        let _ = self.shutdown_tx.send(());
        info!("Offline queue stopped");
        Ok(())
    }

    /// Clone for background tasks
    fn clone_for_background(&self) -> Self {
        Self {
            db: self.db.clone(),
            memory_index: self.memory_index.clone(),
            max_size: self.max_size.clone(),
            current_size: self.current_size.clone(),
            total_bytes: self.total_bytes.clone(),
            pending_operations: self.pending_operations.clone(),
            failed_operations: self.failed_operations.clone(),
            completed_operations: self.completed_operations.clone(),
            retry_operations: self.retry_operations.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            processing_interval: self.processing_interval,
        }
    }
}

impl Drop for OfflineQueue {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_offline_queue_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let queue_path = temp_dir.path().join("test_queue");
        
        let queue = OfflineQueue::new(
            queue_path,
            100,
            Duration::from_millis(100),
        ).await.unwrap();

        // Test enqueue
        let op_id = queue.enqueue(
            OperationType::Upload,
            "test.txt".to_string(),
            Some(Bytes::from("test data")),
            OperationPriority::Normal,
            None,
        ).await.unwrap();

        assert!(!op_id.is_empty());

        // Test dequeue
        let operation = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(operation.id, op_id);
        assert_eq!(operation.path, "test.txt");
        assert!(matches!(operation.operation_type, OperationType::Upload));

        // Test complete
        queue.complete_operation(&op_id).await.unwrap();

        // Should be empty now
        assert!(queue.dequeue().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let temp_dir = TempDir::new().unwrap();
        let queue_path = temp_dir.path().join("priority_queue");
        
        let queue = OfflineQueue::new(
            queue_path,
            100,
            Duration::from_millis(100),
        ).await.unwrap();

        // Enqueue operations with different priorities
        let low_id = queue.enqueue(
            OperationType::Upload,
            "low.txt".to_string(),
            None,
            OperationPriority::Low,
            None,
        ).await.unwrap();

        let high_id = queue.enqueue(
            OperationType::Upload,
            "high.txt".to_string(),
            None,
            OperationPriority::High,
            None,
        ).await.unwrap();

        let normal_id = queue.enqueue(
            OperationType::Upload,
            "normal.txt".to_string(),
            None,
            OperationPriority::Normal,
            None,
        ).await.unwrap();

        // Should dequeue in priority order: High, Normal, Low
        let op1 = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(op1.id, high_id);

        let op2 = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(op2.id, normal_id);

        let op3 = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(op3.id, low_id);
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let queue_path = temp_dir.path().join("persistent_queue");
        
        // Create queue and add operation
        {
            let queue = OfflineQueue::new(
                queue_path.clone(),
                100,
                Duration::from_millis(100),
            ).await.unwrap();

            queue.enqueue(
                OperationType::Upload,
                "persistent.txt".to_string(),
                Some(Bytes::from("persistent data")),
                OperationPriority::High,
                None,
            ).await.unwrap();
        }

        // Create new queue instance and verify operation is still there
        {
            let queue = OfflineQueue::new(
                queue_path,
                100,
                Duration::from_millis(100),
            ).await.unwrap();

            let operation = queue.dequeue().await.unwrap().unwrap();
            assert_eq!(operation.path, "persistent.txt");
            assert_eq!(operation.data.unwrap(), b"persistent data");
        }
    }
}