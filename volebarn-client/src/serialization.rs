//! Dual serialization support for Volebarn client
//! 
//! This module provides high-performance serialization using:
//! - serde_json for API layer (human-readable, interoperable)
//! - Snappy compression for optimal speed/ratio balance

use crate::error::{ClientError, ClientResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Serialize data for API layer using JSON
pub fn serialize_json<T: Serialize>(data: &T) -> ClientResult<Vec<u8>> {
    serde_json::to_vec(data).map_err(|e| ClientError::Serialization {
        operation: "json_serialize".to_string(),
        error: e.to_string(),
    })
}

/// Deserialize data from API layer using JSON
pub fn deserialize_json<T: for<'de> Deserialize<'de>>(data: &[u8]) -> ClientResult<T> {
    serde_json::from_slice(data).map_err(|e| ClientError::Deserialization {
        operation: "json_deserialize".to_string(),
        error: e.to_string(),
    })
}

/// Serialize data for local storage using bitcode (ultra-fast binary format)
pub fn serialize_storage<T: bitcode::Encode>(data: &T) -> ClientResult<Vec<u8>> {
    Ok(bitcode::encode(data))
}

/// Deserialize data from local storage using bitcode
pub fn deserialize_storage<T: for<'a> bitcode::Decode<'a>>(data: &[u8]) -> ClientResult<T> {
    bitcode::decode(data).map_err(|e| ClientError::Deserialization {
        operation: "bitcode_deserialize".to_string(),
        error: e.to_string(),
    })
}

/// Serialize and compress data for storage using bitcode + Snappy
/// 
/// serialize → compress → decompress → deserialize
pub fn serialize_compressed<T: bitcode::Encode>(data: &T) -> ClientResult<Vec<u8>> {
    let serialized = serialize_storage(data)?;
    compress_data(&serialized)
}

/// Decompress and deserialize data from storage using Snappy + bitcode
pub fn deserialize_compressed<T: for<'a> bitcode::Decode<'a>>(compressed_data: &[u8]) -> ClientResult<T> {
    let decompressed = decompress_data(compressed_data)?;
    deserialize_storage(&decompressed)
}

/// Compress data using Snappy (optimal speed/ratio balance)
pub fn compress_data(data: &[u8]) -> ClientResult<Vec<u8>> {
    let mut encoder = snap::raw::Encoder::new();
    encoder.compress_vec(data).map_err(|e| ClientError::Serialization {
        operation: "snappy_compress".to_string(),
        error: e.to_string(),
    })
}

/// Decompress data using Snappy
pub fn decompress_data(compressed_data: &[u8]) -> ClientResult<Vec<u8>> {
    let mut decoder = snap::raw::Decoder::new();
    decoder.decompress_vec(compressed_data).map_err(|e| ClientError::Deserialization {
        operation: "snappy_decompress".to_string(),
        error: e.to_string(),
    })
}

/// Compress Bytes data using Snappy (zero-copy where possible)
pub fn compress_bytes(data: &Bytes) -> ClientResult<Vec<u8>> {
    compress_data(data)
}

/// Decompress to Bytes (zero-copy where possible)
pub fn decompress_to_bytes(compressed_data: &[u8]) -> ClientResult<Bytes> {
    let decompressed = decompress_data(compressed_data)?;
    Ok(Bytes::from(decompressed))
}

/// Utility trait for types that support dual serialization
pub trait DualSerialize: Serialize + bitcode::Encode + Sized {
    /// Serialize for API layer (JSON)
    fn to_json(&self) -> ClientResult<Vec<u8>> {
        serialize_json(self)
    }
    
    /// Serialize for storage layer (bitcode)
    fn to_storage(&self) -> ClientResult<Vec<u8>> {
        serialize_storage(self)
    }
    
    /// Serialize and compress for storage (bitcode + Snappy)
    fn to_compressed_storage(&self) -> ClientResult<Vec<u8>> {
        serialize_compressed(self)
    }
}

/// Utility trait for types that support dual deserialization
pub trait DualDeserialize: for<'de> Deserialize<'de> + for<'a> bitcode::Decode<'a> + Sized {
    /// Deserialize from API layer (JSON)
    fn from_json(data: &[u8]) -> ClientResult<Self> {
        deserialize_json(data)
    }
    
    /// Deserialize from storage layer (bitcode)
    fn from_storage(data: &[u8]) -> ClientResult<Self> {
        deserialize_storage(data)
    }
    
    /// Decompress and deserialize from storage (Snappy + bitcode)
    fn from_compressed_storage(compressed_data: &[u8]) -> ClientResult<Self> {
        deserialize_compressed(compressed_data)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
    struct TestData {
        id: u64,
        name: String,
        values: Vec<i32>,
    }

    impl DualSerialize for TestData {}
    impl DualDeserialize for TestData {}

    #[test]
    fn test_json_serialization() {
        let data = TestData {
            id: 123,
            name: "test".to_string(),
            values: vec![1, 2, 3, 4, 5],
        };

        let serialized = serialize_json(&data).unwrap();
        let deserialized: TestData = deserialize_json(&serialized).unwrap();
        
        assert_eq!(data, deserialized);
    }

    #[test]
    fn test_bitcode_serialization() {
        let data = TestData {
            id: 456,
            name: "bitcode_test".to_string(),
            values: vec![10, 20, 30],
        };

        let serialized = serialize_storage(&data).unwrap();
        let deserialized: TestData = deserialize_storage(&serialized).unwrap();
        
        assert_eq!(data, deserialized);
    }

    #[test]
    fn test_compressed_serialization() {
        let data = TestData {
            id: 789,
            name: "compressed_test_with_longer_name_for_better_compression".to_string(),
            values: (0..100).collect(), // Larger data for compression
        };

        let compressed = serialize_compressed(&data).unwrap();
        let deserialized: TestData = deserialize_compressed(&compressed).unwrap();
        
        assert_eq!(data, deserialized);
        
        // Compressed should be smaller than uncompressed for this data
        let uncompressed = serialize_storage(&data).unwrap();
        assert!(compressed.len() < uncompressed.len());
    }

    #[test]
    fn test_compression_roundtrip() {
        let original_data = b"This is test data that should compress well because it has repeated patterns and text.";
        
        let compressed = compress_data(original_data).unwrap();
        let decompressed = decompress_data(&compressed).unwrap();
        
        assert_eq!(original_data, decompressed.as_slice());
        assert!(compressed.len() < original_data.len()); // Should compress
    }

    #[test]
    fn test_bytes_compression() {
        let original = Bytes::from_static(b"Test bytes data for compression");
        
        let compressed = compress_bytes(&original).unwrap();
        let decompressed = decompress_to_bytes(&compressed).unwrap();
        
        assert_eq!(original, decompressed);
    }

    #[test]
    fn test_dual_serialize_trait() {
        let data = TestData {
            id: 999,
            name: "trait_test".to_string(),
            values: vec![100, 200, 300],
        };

        // Test JSON serialization via trait
        let json_data = data.to_json().unwrap();
        let from_json = TestData::from_json(&json_data).unwrap();
        assert_eq!(data, from_json);

        // Test storage serialization via trait
        let storage_data = data.to_storage().unwrap();
        let from_storage = TestData::from_storage(&storage_data).unwrap();
        assert_eq!(data, from_storage);

        // Test compressed storage serialization via trait
        let compressed_data = data.to_compressed_storage().unwrap();
        let from_compressed = TestData::from_compressed_storage(&compressed_data).unwrap();
        assert_eq!(data, from_compressed);
    }
}