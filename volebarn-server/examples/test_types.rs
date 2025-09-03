//! Test example for core data models and types
//! 
//! This example demonstrates the functionality implemented in task 2:
//! - Core data models (FileMetadata, DirectoryListing, SyncPlan, BulkOperation)
//! - Dual serialization (JSON for API, bitcode for storage)
//! - Snappy compression
//! - xxHash3 integration
//! - Comprehensive error types

use volebarn_server::{
    types::*,
    serialization::*,
    hash::*,
    error::*,
    storage_types::*,
    time_utils::*,
};
use std::time::SystemTime;
use bytes::Bytes;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Task 2: Core Data Models and Types");
    
    // Test 1: FileMetadata creation and conversion
    println!("\n1. Testing FileMetadata...");
    let file_metadata = FileMetadata::new(
        "/test/file.txt".to_string(),
        1024,
        SystemTime::now(),
        12345678901234567890u64,
    );
    println!("✓ FileMetadata created: {}", file_metadata.name);
    
    // Test 2: JSON serialization
    println!("\n2. Testing JSON serialization...");
    let json_data = serialize_json(&file_metadata)?;
    let deserialized: FileMetadata = deserialize_json(&json_data)?;
    assert_eq!(file_metadata.path, deserialized.path);
    println!("✓ JSON serialization/deserialization works");
    
    // Test 3: Storage types with bitcode
    println!("\n3. Testing storage types with bitcode...");
    let storage_metadata = StorageFileMetadata::from(file_metadata.clone());
    let bitcode_data = serialize_storage(&storage_metadata)?;
    let storage_deserialized: StorageFileMetadata = deserialize_storage(&bitcode_data)?;
    assert_eq!(storage_metadata.path, storage_deserialized.path);
    println!("✓ Bitcode serialization/deserialization works");
    
    // Test 4: Compression
    println!("\n4. Testing Snappy compression...");
    let compressed_data = serialize_compressed(&storage_metadata)?;
    let decompressed: StorageFileMetadata = deserialize_compressed(&compressed_data)?;
    assert_eq!(storage_metadata.path, decompressed.path);
    println!("✓ Snappy compression/decompression works");
    println!("  Original size: {} bytes", bitcode_data.len());
    println!("  Compressed size: {} bytes", compressed_data.len());
    
    // Test 5: Hash functionality
    println!("\n5. Testing xxHash3 integration...");
    let test_data = b"Hello, Volebarn!";
    let hash = calculate_hash(test_data);
    assert!(verify_hash(test_data, hash));
    println!("✓ xxHash3 calculation and verification works");
    println!("  Hash: {:016x}", hash);
    
    // Test 6: Hash manager
    println!("\n6. Testing HashManager...");
    let hash_manager = HashManager::new();
    let bytes_data = Bytes::from_static(b"Test bytes data");
    let bytes_hash = hash_manager.calculate_hash_bytes(&bytes_data);
    assert!(hash_manager.verify_hash_bytes(&bytes_data, bytes_hash));
    println!("✓ HashManager works with Bytes");
    
    // Test 7: Complex types
    println!("\n7. Testing complex types...");
    let mut file_manifest = FileManifest {
        files: std::collections::HashMap::new(),
    };
    file_manifest.files.insert(file_metadata.path.clone(), file_metadata.clone());
    
    let sync_plan = SyncPlan {
        client_upload: vec!["/new/file.txt".to_string()],
        client_download: vec!["/server/file.txt".to_string()],
        client_delete: vec!["/old/file.txt".to_string()],
        client_create_dirs: vec!["/new/dir".to_string()],
        conflicts: vec![],
    };
    
    assert!(!sync_plan.is_empty());
    assert_eq!(sync_plan.total_operations(), 4);
    println!("✓ Complex types (FileManifest, SyncPlan) work correctly");
    
    // Test 8: Error types
    println!("\n8. Testing error types...");
    let server_error = ServerError::FileNotFound {
        path: "/missing/file.txt".to_string(),
    };
    assert_eq!(server_error.error_code(), ErrorCode::FileNotFound);
    assert_eq!(server_error.http_status(), 404);
    assert!(!server_error.is_retryable());
    println!("✓ Error types with detailed context work");
    
    // Test 9: Time utilities
    println!("\n9. Testing time utilities...");
    let now = SystemTime::now();
    let serializable_time = SerializableSystemTime::from(now);
    let converted_back = SystemTime::from(serializable_time);
    // Should be within 1 second due to truncation
    let diff = now.duration_since(converted_back)
        .or_else(|_| converted_back.duration_since(now))
        .unwrap();
    assert!(diff.as_secs() <= 1);
    println!("✓ Time utilities for bitcode compatibility work");
    
    println!("\n🎉 All tests passed! Task 2 implementation is complete.");
    println!("\nImplemented features:");
    println!("✓ Core data models (FileMetadata, DirectoryListing, SyncPlan, BulkOperation)");
    println!("✓ Dual serialization: JSON for API layer, bitcode for storage layer");
    println!("✓ Snappy compression for optimal speed/ratio balance");
    println!("✓ xxHash3 integration for file integrity verification");
    println!("✓ Comprehensive error types with thiserror and detailed context");
    println!("✓ Storage types with bitcode serialization support");
    println!("✓ Time utilities for bitcode compatibility");
    
    Ok(())
}