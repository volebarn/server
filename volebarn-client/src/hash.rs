//! Hash utilities for file integrity verification using xxHash3
//! 
//! This module provides fast hash calculation and verification using xxHash3,
//! which is optimized for performance and suitable for content-addressable storage.

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

/// Calculate xxHash3 for the given data
/// 
/// Uses xxHash3 which is extremely fast and suitable for file integrity verification.
/// Not cryptographically secure, but perfect for detecting corruption and deduplication.
pub fn calculate_hash(data: &[u8]) -> u64 {
    xxh3_64(data)
}

/// Calculate xxHash3 for Bytes (zero-copy)
pub fn calculate_hash_bytes(data: &Bytes) -> u64 {
    xxh3_64(data)
}

/// Verify that data matches the expected hash
pub fn verify_hash(data: &[u8], expected_hash: u64) -> bool {
    calculate_hash(data) == expected_hash
}

/// Verify that Bytes data matches the expected hash (zero-copy)
pub fn verify_hash_bytes(data: &Bytes, expected_hash: u64) -> bool {
    calculate_hash_bytes(data) == expected_hash
}

/// Convert hash to hex string for JSON serialization
pub fn hash_to_hex(hash: u64) -> String {
    format!("{:016x}", hash)
}

/// Parse hex string back to hash
pub fn hex_to_hash(hex: &str) -> Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(hex, 16)
}

/// Hash manager for file integrity verification
#[derive(Debug)]
pub struct HashManager {
    hash_count: std::sync::atomic::AtomicU64,
    verify_count: std::sync::atomic::AtomicU64,
}

impl Clone for HashManager {
    fn clone(&self) -> Self {
        Self {
            hash_count: std::sync::atomic::AtomicU64::new(
                self.hash_count.load(std::sync::atomic::Ordering::Relaxed)
            ),
            verify_count: std::sync::atomic::AtomicU64::new(
                self.verify_count.load(std::sync::atomic::Ordering::Relaxed)
            ),
        }
    }
}

impl HashManager {
    /// Create a new hash manager
    pub fn new() -> Self {
        Self {
            hash_count: std::sync::atomic::AtomicU64::new(0),
            verify_count: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    /// Calculate hash for data
    pub fn calculate_hash(&self, data: &[u8]) -> u64 {
        self.hash_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        calculate_hash(data)
    }
    
    /// Calculate hash for Bytes
    pub fn calculate_hash_bytes(&self, data: &Bytes) -> u64 {
        self.hash_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        calculate_hash_bytes(data)
    }
    
    /// Verify hash
    pub fn verify_hash(&self, data: &[u8], expected_hash: u64) -> bool {
        self.verify_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        verify_hash(data, expected_hash)
    }
    
    /// Verify hash for Bytes
    pub fn verify_hash_bytes(&self, data: &Bytes, expected_hash: u64) -> bool {
        self.verify_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        verify_hash_bytes(data, expected_hash)
    }
    
    /// Alias for calculate_hash_bytes
    pub fn hash_bytes(&self, data: &Bytes) -> u64 {
        self.calculate_hash_bytes(data)
    }
    
    /// Verify hash and return result with error details
    pub fn verify(&self, data: &[u8], expected_hash: u64) -> bool {
        self.verify_hash(data, expected_hash)
    }
    
    /// Verify bytes with error details
    pub fn verify_bytes(&self, data: &Bytes, expected_hash: u64) -> bool {
        self.verify_hash_bytes(data, expected_hash)
    }
    
    /// Verify with detailed error information
    pub fn verify_with_error(&self, data: &[u8], expected_hash: u64) -> Result<(), String> {
        if self.verify_hash(data, expected_hash) {
            Ok(())
        } else {
            let actual_hash = self.calculate_hash(data);
            Err(format!("Hash mismatch: expected {:016x}, got {:016x}", expected_hash, actual_hash))
        }
    }
    
    /// Convert hash to hex string
    pub fn to_hex(&self, hash: u64) -> String {
        hash_to_hex(hash)
    }
    
    /// Parse hex string to hash
    pub fn from_hex(&self, hex: &str) -> Result<u64, std::num::ParseIntError> {
        hex_to_hash(hex)
    }
    
    /// Get hash operation statistics
    pub fn get_stats(&self) -> (u64, u64) {
        (
            self.hash_count.load(std::sync::atomic::Ordering::Relaxed),
            self.verify_count.load(std::sync::atomic::Ordering::Relaxed),
        )
    }
    
    /// Reset statistics
    pub fn reset_stats(&self) {
        self.hash_count.store(0, std::sync::atomic::Ordering::Relaxed);
        self.verify_count.store(0, std::sync::atomic::Ordering::Relaxed);
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

    #[test]
    fn test_hash_calculation() {
        let data = b"Hello, World!";
        let hash = calculate_hash(data);
        
        // Hash should be consistent
        assert_eq!(hash, calculate_hash(data));
        
        // Different data should produce different hash
        let different_data = b"Hello, World?";
        assert_ne!(hash, calculate_hash(different_data));
    }

    #[test]
    fn test_hash_verification() {
        let data = b"Test data for verification";
        let hash = calculate_hash(data);
        
        assert!(verify_hash(data, hash));
        assert!(!verify_hash(b"Different data", hash));
    }

    #[test]
    fn test_bytes_hash() {
        let data = Bytes::from_static(b"Test bytes data");
        let hash = calculate_hash_bytes(&data);
        
        assert!(verify_hash_bytes(&data, hash));
        
        let different_data = Bytes::from_static(b"Different bytes");
        assert!(!verify_hash_bytes(&different_data, hash));
    }

    #[test]
    fn test_hex_conversion() {
        let hash = 0x123456789abcdef0u64;
        let hex = hash_to_hex(hash);
        assert_eq!(hex, "123456789abcdef0");
        
        let parsed = hex_to_hash(&hex).unwrap();
        assert_eq!(parsed, hash);
    }

    #[test]
    fn test_empty_data() {
        let empty_data = b"";
        let hash = calculate_hash(empty_data);
        assert!(verify_hash(empty_data, hash));
        
        let empty_bytes = Bytes::new();
        let bytes_hash = calculate_hash_bytes(&empty_bytes);
        assert!(verify_hash_bytes(&empty_bytes, bytes_hash));
    }
}