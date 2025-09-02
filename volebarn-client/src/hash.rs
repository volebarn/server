//! Hash utilities for file integrity verification and content-addressable storage
//! 
//! This module provides xxHash3-based hashing for fast file integrity verification
//! and content deduplication using zero-copy operations where possible.

use bytes::Bytes;
use std::io::Read;
use xxhash_rust::xxh3::xxh3_64;

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

/// Hash manager for coordinating hash operations
#[derive(Debug, Clone)]
pub struct HashManager;

impl HashManager {
    /// Create a new hash manager
    pub fn new() -> Self {
        Self
    }

    /// Calculate hash for bytes with zero-copy
    pub fn hash_bytes(&self, data: &Bytes) -> u64 {
        hash_bytes(data)
    }

    /// Calculate hash for file content
    pub async fn hash_file(&self, path: &std::path::Path) -> std::io::Result<u64> {
        let content = tokio::fs::read(path).await?;
        Ok(xxh3_64(&content))
    }

    /// Verify file integrity
    pub fn verify(&self, expected: u64, actual: u64) -> bool {
        verify_integrity(expected, actual)
    }

    /// Convert hash to hex string
    pub fn to_hex(&self, hash: u64) -> String {
        hash_to_hex(hash)
    }

    /// Parse hex string to hash
    pub fn from_hex(&self, hex: &str) -> Result<u64, std::num::ParseIntError> {
        hex_to_hash(hex)
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
    fn test_hash_manager() {
        let manager = HashManager::new();
        let data = Bytes::from_static(b"test data");
        
        let hash = manager.hash_bytes(&data);
        let hex = manager.to_hex(hash);
        let parsed = manager.from_hex(&hex).unwrap();
        
        assert_eq!(hash, parsed);
        assert!(manager.verify(hash, parsed));
    }
}