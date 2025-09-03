//! Hash utilities for file integrity verification and content-addressable storage
//! 
//! This module provides xxHash3-based hashing for fast file integrity verification
//! and content deduplication using zero-copy operations where possible.

use bytes::Bytes;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::ClientError;

/// Calculate xxHash3 for bytes using zero-copy operations
pub fn hash_bytes(data: &Bytes) -> u64 {
    xxh3_64(data)
}

/// Calculate xxHash3 for a byte slice
pub fn hash_slice(data: &[u8]) -> u64 {
    xxh3_64(data)
}

/// Calculate xxHash3 for any readable stream
pub fn hash_reader<R: Read>(mut reader: R) -> std::io::Result<u64> {
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(xxh3_64(&buffer))
}

/// Convert hash to hex string for JSON serialization
pub fn hash_to_hex(hash: u64) -> String {
    format!("{:016x}", hash)
}

/// Parse hex string back to hash
pub fn hex_to_hash(hex: &str) -> Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(hex, 16)
}

/// Verify file integrity by comparing hashes
pub fn verify_integrity(expected: u64, actual: u64) -> bool {
    expected == actual
}

/// Hash verification statistics for monitoring and debugging
#[derive(Debug, Default)]
pub struct HashStats {
    /// Total number of hash calculations performed
    pub calculations: AtomicU64,
    /// Total number of successful verifications
    pub verifications_success: AtomicU64,
    /// Total number of failed verifications (hash mismatches)
    pub verifications_failed: AtomicU64,
    /// Total bytes processed for hashing
    pub bytes_processed: AtomicU64,
    /// Number of concurrent hash operations
    pub concurrent_operations: AtomicU64,
}

impl HashStats {
    /// Create new hash statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a hash calculation
    pub fn record_calculation(&self, bytes_processed: u64) {
        self.calculations.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes_processed, Ordering::Relaxed);
    }

    /// Record a successful verification
    pub fn record_verification_success(&self) {
        self.verifications_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed verification
    pub fn record_verification_failure(&self) {
        self.verifications_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Start tracking a concurrent operation
    pub fn start_operation(&self) {
        self.concurrent_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// End tracking a concurrent operation
    pub fn end_operation(&self) {
        self.concurrent_operations.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current statistics snapshot
    pub fn snapshot(&self) -> HashStatsSnapshot {
        HashStatsSnapshot {
            calculations: self.calculations.load(Ordering::Relaxed),
            verifications_success: self.verifications_success.load(Ordering::Relaxed),
            verifications_failed: self.verifications_failed.load(Ordering::Relaxed),
            bytes_processed: self.bytes_processed.load(Ordering::Relaxed),
            concurrent_operations: self.concurrent_operations.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of hash statistics at a point in time
#[derive(Debug, Clone)]
pub struct HashStatsSnapshot {
    pub calculations: u64,
    pub verifications_success: u64,
    pub verifications_failed: u64,
    pub bytes_processed: u64,
    pub concurrent_operations: u64,
}

impl HashStatsSnapshot {
    /// Calculate success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total = self.verifications_success + self.verifications_failed;
        if total == 0 {
            100.0
        } else {
            (self.verifications_success as f64 / total as f64) * 100.0
        }
    }

    /// Calculate average bytes per calculation
    pub fn avg_bytes_per_calculation(&self) -> f64 {
        if self.calculations == 0 {
            0.0
        } else {
            self.bytes_processed as f64 / self.calculations as f64
        }
    }
}

/// Async hash manager for coordinating hash operations with lock-free statistics
#[derive(Debug, Clone)]
pub struct HashManager {
    /// Statistics for monitoring hash operations
    stats: Arc<HashStats>,
}

impl HashManager {
    /// Create a new hash manager with statistics tracking
    pub fn new() -> Self {
        Self {
            stats: Arc::new(HashStats::new()),
        }
    }

    /// Get reference to hash statistics
    pub fn stats(&self) -> &Arc<HashStats> {
        &self.stats
    }

    /// Calculate hash for bytes with zero-copy and statistics tracking
    pub fn hash_bytes(&self, data: &Bytes) -> u64 {
        self.stats.record_calculation(data.len() as u64);
        hash_bytes(data)
    }

    /// Calculate hash for file content asynchronously with zero-copy
    pub async fn hash_file(&self, path: &Path) -> Result<u64, ClientError> {
        self.stats.start_operation();
        
        let result = async {
            let content = tokio::fs::read(path).await.map_err(|e| ClientError::Io {
                path: path.to_string_lossy().to_string(),
                error: e.to_string(),
            })?;
            
            let bytes = Bytes::from(content);
            let hash = self.hash_bytes(&bytes);
            Ok(hash)
        }.await;
        
        self.stats.end_operation();
        result
    }

    /// Calculate hash for async reader with streaming and zero-copy
    pub async fn hash_reader<R: AsyncRead + Unpin>(&self, mut reader: R) -> Result<u64, ClientError> {
        self.stats.start_operation();
        
        let result = async {
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.map_err(|e| ClientError::Io {
                path: "stream".to_string(),
                error: e.to_string(),
            })?;
            
            let bytes = Bytes::from(buffer);
            let hash = self.hash_bytes(&bytes);
            Ok(hash)
        }.await;
        
        self.stats.end_operation();
        result
    }

    /// Verify file integrity with statistics tracking
    pub fn verify(&self, expected: u64, actual: u64) -> bool {
        let is_valid = verify_integrity(expected, actual);
        
        if is_valid {
            self.stats.record_verification_success();
        } else {
            self.stats.record_verification_failure();
        }
        
        is_valid
    }

    /// Verify file integrity and return detailed error on mismatch
    pub fn verify_with_error(&self, path: &str, expected: u64, actual: u64) -> Result<(), ClientError> {
        if self.verify(expected, actual) {
            Ok(())
        } else {
            Err(ClientError::HashMismatch {
                path: path.to_string(),
                expected: self.to_hex(expected),
                actual: self.to_hex(actual),
            })
        }
    }

    /// Async verify file content against expected hash
    pub async fn verify_file(&self, path: &Path, expected_hash: u64) -> Result<(), ClientError> {
        let actual_hash = self.hash_file(path).await?;
        self.verify_with_error(&path.to_string_lossy(), expected_hash, actual_hash)
    }

    /// Async verify bytes content against expected hash
    pub async fn verify_bytes(&self, path: &str, data: &Bytes, expected_hash: u64) -> Result<(), ClientError> {
        let actual_hash = self.hash_bytes(data);
        self.verify_with_error(path, expected_hash, actual_hash)
    }

    /// Async verify reader content against expected hash
    pub async fn verify_reader<R: AsyncRead + Unpin>(&self, path: &str, reader: R, expected_hash: u64) -> Result<(), ClientError> {
        let actual_hash = self.hash_reader(reader).await?;
        self.verify_with_error(path, expected_hash, actual_hash)
    }

    /// Convert hash to hex string
    pub fn to_hex(&self, hash: u64) -> String {
        hash_to_hex(hash)
    }

    /// Parse hex string to hash
    pub fn from_hex(&self, hex: &str) -> Result<u64, std::num::ParseIntError> {
        hex_to_hash(hex)
    }

    /// Get current statistics snapshot
    pub fn get_stats(&self) -> HashStatsSnapshot {
        self.stats.snapshot()
    }

    /// Reset statistics (useful for testing)
    pub fn reset_stats(&self) {
        self.stats.calculations.store(0, Ordering::Relaxed);
        self.stats.verifications_success.store(0, Ordering::Relaxed);
        self.stats.verifications_failed.store(0, Ordering::Relaxed);
        self.stats.bytes_processed.store(0, Ordering::Relaxed);
        self.stats.concurrent_operations.store(0, Ordering::Relaxed);
    }
}

impl Default for HashManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::io::Cursor;
    use tokio::task::JoinSet;

    #[test]
    fn test_hash_consistency() {
        let data = b"Hello, World!";
        let bytes = Bytes::from_static(data);
        
        let hash1 = hash_slice(data);
        let hash2 = hash_bytes(&bytes);
        
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hex_conversion() {
        let hash = 0x1234567890abcdef;
        let hex = hash_to_hex(hash);
        let parsed = hex_to_hash(&hex).unwrap();
        
        assert_eq!(hash, parsed);
        assert_eq!(hex, "1234567890abcdef");
    }

    #[test]
    fn test_integrity_verification() {
        let hash1 = 0x1234567890abcdef;
        let hash2 = 0x1234567890abcdef;
        let hash3 = 0xfedcba0987654321;
        
        assert!(verify_integrity(hash1, hash2));
        assert!(!verify_integrity(hash1, hash3));
    }

    #[test]
    fn test_hash_manager_basic() {
        let manager = HashManager::new();
        let data = Bytes::from_static(b"test data");
        
        let hash = manager.hash_bytes(&data);
        let hex = manager.to_hex(hash);
        let parsed = manager.from_hex(&hex).unwrap();
        
        assert_eq!(hash, parsed);
        assert!(manager.verify(hash, parsed));
    }

    #[test]
    fn test_hash_stats() {
        let manager = HashManager::new();
        let data1 = Bytes::from_static(b"test data 1");
        let data2 = Bytes::from_static(b"test data 2");
        
        // Initial stats should be zero
        let stats = manager.get_stats();
        assert_eq!(stats.calculations, 0);
        assert_eq!(stats.verifications_success, 0);
        assert_eq!(stats.verifications_failed, 0);
        
        // Hash some data
        let hash1 = manager.hash_bytes(&data1);
        let hash2 = manager.hash_bytes(&data2);
        
        // Check calculations increased
        let stats = manager.get_stats();
        assert_eq!(stats.calculations, 2);
        assert_eq!(stats.bytes_processed, (data1.len() + data2.len()) as u64);
        
        // Verify hashes
        assert!(manager.verify(hash1, hash1)); // Success
        assert!(!manager.verify(hash1, hash2)); // Failure
        
        // Check verification stats
        let stats = manager.get_stats();
        assert_eq!(stats.verifications_success, 1);
        assert_eq!(stats.verifications_failed, 1);
        assert_eq!(stats.success_rate(), 50.0);
    }

    #[tokio::test]
    async fn test_async_hash_file() {
        let manager = HashManager::new();
        
        // Create a temporary file
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        let test_data = b"Hello, async world!";
        
        tokio::fs::write(&file_path, test_data).await.unwrap();
        
        // Hash the file
        let hash = manager.hash_file(&file_path).await.unwrap();
        
        // Verify it matches direct hash
        let expected_hash = xxh3_64(test_data);
        assert_eq!(hash, expected_hash);
        
        // Check stats
        let stats = manager.get_stats();
        assert_eq!(stats.calculations, 1);
        assert_eq!(stats.bytes_processed, test_data.len() as u64);
    }

    #[tokio::test]
    async fn test_async_hash_reader() {
        let manager = HashManager::new();
        let test_data = b"Hello, async reader!";
        let cursor = Cursor::new(test_data);
        
        let hash = manager.hash_reader(cursor).await.unwrap();
        let expected_hash = xxh3_64(test_data);
        
        assert_eq!(hash, expected_hash);
        
        let stats = manager.get_stats();
        assert_eq!(stats.calculations, 1);
        assert_eq!(stats.bytes_processed, test_data.len() as u64);
    }

    #[tokio::test]
    async fn test_async_verify_file() {
        let manager = HashManager::new();
        
        // Create a temporary file
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("verify_test.txt");
        let test_data = b"Verify this file!";
        
        tokio::fs::write(&file_path, test_data).await.unwrap();
        
        let expected_hash = xxh3_64(test_data);
        let wrong_hash = 0x1234567890abcdef;
        
        // Should succeed with correct hash
        assert!(manager.verify_file(&file_path, expected_hash).await.is_ok());
        
        // Should fail with wrong hash
        let result = manager.verify_file(&file_path, wrong_hash).await;
        assert!(result.is_err());
        
        if let Err(ClientError::HashMismatch { path, expected, actual }) = result {
            assert_eq!(path, file_path.to_string_lossy());
            assert_eq!(expected, manager.to_hex(wrong_hash));
            assert_eq!(actual, manager.to_hex(expected_hash));
        } else {
            panic!("Expected HashMismatch error");
        }
        
        // Check verification stats
        let stats = manager.get_stats();
        assert_eq!(stats.verifications_success, 1);
        assert_eq!(stats.verifications_failed, 1);
    }

    #[tokio::test]
    async fn test_async_verify_bytes() {
        let manager = HashManager::new();
        let test_data = Bytes::from_static(b"Verify these bytes!");
        let expected_hash = xxh3_64(&test_data);
        let wrong_hash = 0xfedcba0987654321;
        
        // Should succeed with correct hash
        assert!(manager.verify_bytes("test_path", &test_data, expected_hash).await.is_ok());
        
        // Should fail with wrong hash
        let result = manager.verify_bytes("test_path", &test_data, wrong_hash).await;
        assert!(result.is_err());
        
        if let Err(ClientError::HashMismatch { path, expected, actual }) = result {
            assert_eq!(path, "test_path");
            assert_eq!(expected, manager.to_hex(wrong_hash));
            assert_eq!(actual, manager.to_hex(expected_hash));
        } else {
            panic!("Expected HashMismatch error");
        }
    }

    #[tokio::test]
    async fn test_concurrent_hash_operations() {
        let manager = Arc::new(HashManager::new());
        let mut join_set = JoinSet::new();
        
        // Spawn multiple concurrent hash operations
        for i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            join_set.spawn(async move {
                let data = Bytes::from(format!("concurrent test data {}", i));
                let hash = manager_clone.hash_bytes(&data);
                
                // Verify the hash
                let verification_result = manager_clone.verify(hash, hash);
                assert!(verification_result);
                
                hash
            });
        }
        
        // Wait for all operations to complete
        let mut hashes = Vec::new();
        while let Some(result) = join_set.join_next().await {
            hashes.push(result.unwrap());
        }
        
        // All hashes should be different (different input data)
        assert_eq!(hashes.len(), 10);
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(hashes[i], hashes[j]);
            }
        }
        
        // Check final stats
        let stats = manager.get_stats();
        assert_eq!(stats.calculations, 10);
        assert_eq!(stats.verifications_success, 10);
        assert_eq!(stats.verifications_failed, 0);
        assert_eq!(stats.concurrent_operations, 0); // All operations completed
        assert_eq!(stats.success_rate(), 100.0);
    }

    #[tokio::test]
    async fn test_concurrent_file_verification() {
        let manager = Arc::new(HashManager::new());
        let temp_dir = tempfile::tempdir().unwrap();
        let mut join_set = JoinSet::new();
        
        // Create multiple test files
        let mut expected_hashes = Vec::new();
        for i in 0..5 {
            let file_path = temp_dir.path().join(format!("concurrent_test_{}.txt", i));
            let test_data = format!("Concurrent file test data {}", i);
            tokio::fs::write(&file_path, &test_data).await.unwrap();
            expected_hashes.push(xxh3_64(test_data.as_bytes()));
        }
        
        // Spawn concurrent verification tasks
        for i in 0..5 {
            let manager_clone = Arc::clone(&manager);
            let file_path = temp_dir.path().join(format!("concurrent_test_{}.txt", i));
            let expected_hash = expected_hashes[i];
            
            join_set.spawn(async move {
                manager_clone.verify_file(&file_path, expected_hash).await
            });
        }
        
        // Wait for all verifications to complete
        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            results.push(result.unwrap());
        }
        
        // All verifications should succeed
        for result in results {
            assert!(result.is_ok());
        }
        
        // Check final stats
        let stats = manager.get_stats();
        assert_eq!(stats.calculations, 5);
        assert_eq!(stats.verifications_success, 5);
        assert_eq!(stats.verifications_failed, 0);
        assert_eq!(stats.success_rate(), 100.0);
    }

    #[tokio::test]
    async fn test_concurrent_mixed_operations() {
        let manager = Arc::new(HashManager::new());
        let temp_dir = tempfile::tempdir().unwrap();
        let mut join_set = JoinSet::new();
        
        // Mix of different async operations
        for i in 0..20 {
            let manager_clone = Arc::clone(&manager);
            
            match i % 4 {
                0 => {
                    // Hash bytes operation
                    join_set.spawn(async move {
                        let data = Bytes::from(format!("bytes test {}", i));
                        let hash = manager_clone.hash_bytes(&data);
                        manager_clone.verify(hash, hash);
                        "bytes"
                    });
                }
                1 => {
                    // Hash file operation
                    let file_path = temp_dir.path().join(format!("mixed_test_{}.txt", i));
                    join_set.spawn(async move {
                        let test_data = format!("file test {}", i);
                        tokio::fs::write(&file_path, &test_data).await.unwrap();
                        let _hash = manager_clone.hash_file(&file_path).await.unwrap();
                        "file"
                    });
                }
                2 => {
                    // Hash reader operation
                    join_set.spawn(async move {
                        let test_data = format!("reader test {}", i);
                        let cursor = Cursor::new(test_data.as_bytes());
                        let _hash = manager_clone.hash_reader(cursor).await.unwrap();
                        "reader"
                    });
                }
                3 => {
                    // Verify bytes operation
                    join_set.spawn(async move {
                        let data = Bytes::from(format!("verify test {}", i));
                        let hash = manager_clone.hash_bytes(&data);
                        let _result = manager_clone.verify_bytes(&format!("path_{}", i), &data, hash).await;
                        "verify"
                    });
                }
                _ => unreachable!(),
            }
        }
        
        // Wait for all operations to complete
        let mut operation_types = Vec::new();
        while let Some(result) = join_set.join_next().await {
            operation_types.push(result.unwrap());
        }
        
        assert_eq!(operation_types.len(), 20);
        
        // Check that we have a mix of operation types
        let bytes_count = operation_types.iter().filter(|&t| *t == "bytes").count();
        let file_count = operation_types.iter().filter(|&t| *t == "file").count();
        let reader_count = operation_types.iter().filter(|&t| *t == "reader").count();
        let verify_count = operation_types.iter().filter(|&t| *t == "verify").count();
        
        assert_eq!(bytes_count, 5);
        assert_eq!(file_count, 5);
        assert_eq!(reader_count, 5);
        assert_eq!(verify_count, 5);
        
        // Check final stats - should have processed many operations
        let stats = manager.get_stats();
        assert!(stats.calculations > 0);
        assert!(stats.bytes_processed > 0);
        assert_eq!(stats.concurrent_operations, 0); // All operations completed
    }

    #[test]
    fn test_stats_reset() {
        let manager = HashManager::new();
        let data = Bytes::from_static(b"test data");
        
        // Generate some stats
        let hash = manager.hash_bytes(&data);
        manager.verify(hash, hash);
        
        let stats = manager.get_stats();
        assert!(stats.calculations > 0);
        assert!(stats.verifications_success > 0);
        
        // Reset stats
        manager.reset_stats();
        
        let stats = manager.get_stats();
        assert_eq!(stats.calculations, 0);
        assert_eq!(stats.verifications_success, 0);
        assert_eq!(stats.verifications_failed, 0);
        assert_eq!(stats.bytes_processed, 0);
        assert_eq!(stats.concurrent_operations, 0);
    }
}