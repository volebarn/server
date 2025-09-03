//! Test example for client core data models and types
//! 
//! This example demonstrates the client-side functionality implemented in task 2

use volebarn_client::{
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
    println!("Testing Task 2: Client Core Data Models and Types");
    
    // Test 1: FileMetadata creation and conversion
    println!("\n1. Testing client FileMetadata...");
    let file_metadata = FileMetadata::new(
        "/client/file.txt".to_string(),
        2048,
        SystemTime::now(),
        9876543210987654321u64,
    );
    println!("✓ Client FileMetadata created: {}", file_metadata.name);
    
    // Test 2: JSON serialization
    println!("\n2. Testing client JSON serialization...");
    let json_data = serialize_json(&file_metadata)?;
    let deserialized: FileMetadata = deserialize_json(&json_data)?;
    assert_eq!(file_metadata.path, deserialized.path);
    println!("✓ Client JSON serialization/deserialization works");
    
    // Test 3: Storage types with bitcode
    println!("\n3. Testing client storage types with bitcode...");
    let storage_metadata = StorageFileMetadata::from(file_metadata.clone());
    let bitcode_data = serialize_storage(&storage_metadata)?;
    let storage_deserialized: StorageFileMetadata = deserialize_storage(&bitcode_data)?;
    assert_eq!(storage_metadata.path, storage_deserialized.path);
    println!("✓ Client bitcode serialization/deserialization works");
    
    // Test 4: Compression
    println!("\n4. Testing client Snappy compression...");
    let compressed_data = serialize_compressed(&storage_metadata)?;
    let decompressed: StorageFileMetadata = deserialize_compressed(&compressed_data)?;
    assert_eq!(storage_metadata.path, decompressed.path);
    println!("✓ Client Snappy compression/decompression works");
    
    // Test 5: Hash functionality with statistics
    println!("\n5. Testing client xxHash3 with statistics...");
    let hash_manager = HashManager::new();
    let test_data = b"Hello, Client!";
    let hash = hash_manager.calculate_hash(test_data);
    assert!(hash_manager.verify_hash(test_data, hash));
    
    let (hash_count, verify_count) = hash_manager.get_stats();
    assert_eq!(hash_count, 1);
    assert_eq!(verify_count, 1);
    println!("✓ Client xxHash3 with statistics works");
    println!("  Hash operations: {}, Verify operations: {}", hash_count, verify_count);
    
    // Test 6: Error types with retry logic
    println!("\n6. Testing client error types...");
    let client_error = ClientError::HashMismatch {
        path: "/test/file.txt".to_string(),
        expected: "abc123".to_string(),
        actual: "def456".to_string(),
    };
    assert_eq!(client_error.error_code(), ErrorCode::HashMismatch);
    assert!(client_error.is_retryable());
    let retry_delay = client_error.retry_delay(1);
    assert!(retry_delay > 0);
    println!("✓ Client error types with retry logic work");
    println!("  Retry delay for attempt 1: {}s", retry_delay);
    
    // Test 7: Complex client types
    println!("\n7. Testing client complex types...");
    let sync_result = SyncResult {
        uploaded: vec!["/uploaded/file.txt".to_string()],
        downloaded: vec!["/downloaded/file.txt".to_string()],
        deleted_local: vec!["/deleted/file.txt".to_string()],
        created_dirs: vec!["/created/dir".to_string()],
        conflicts_resolved: vec!["/resolved/conflict.txt".to_string()],
        errors: vec![],
    };
    
    assert!(sync_result.is_success());
    assert_eq!(sync_result.success_count(), 5);
    assert_eq!(sync_result.error_count(), 0);
    println!("✓ Client complex types (SyncResult) work correctly");
    
    println!("\n🎉 All client tests passed! Task 2 client implementation is complete.");
    
    Ok(())
}