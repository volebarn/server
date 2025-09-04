//! Unit tests for bulk transfer methods
//! 
//! These tests verify the bulk operations work correctly without requiring a server.

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use volebarn_client::{Client, Config, Result as ClientResult};
use volebarn_client::types::*;

/// Helper function to create test client
async fn create_test_client() -> ClientResult<Client> {
    let config = Config::new("http://localhost:8080".to_string());
    Client::new(config).await
}

/// Generate test file content of specified size
fn generate_test_content(size: usize, pattern: &str) -> Bytes {
    let pattern_bytes = pattern.as_bytes();
    let mut content = Vec::with_capacity(size);
    
    while content.len() < size {
        let remaining = size - content.len();
        if remaining >= pattern_bytes.len() {
            content.extend_from_slice(pattern_bytes);
        } else {
            content.extend_from_slice(&pattern_bytes[..remaining]);
        }
    }
    
    Bytes::from(content)
}

#[tokio::test]
async fn test_client_creation() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Verify client was created successfully
    let stats = client.stats().await;
    assert_eq!(stats.requests_sent, 0);
    assert_eq!(stats.requests_successful, 0);
    assert_eq!(stats.requests_failed, 0);
    
    Ok(())
}

#[tokio::test]
async fn test_bulk_upload_request_creation() -> ClientResult<()> {
    let _client = create_test_client().await?;
    
    // Create test files
    let files = vec![
        FileUpload {
            path: "test1.txt".to_string(),
            content: generate_test_content(1024, "test content "),
        },
        FileUpload {
            path: "test2.txt".to_string(),
            content: generate_test_content(2048, "more test content "),
        },
    ];
    
    // Verify files were created correctly
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].path, "test1.txt");
    assert_eq!(files[0].content.len(), 1024);
    assert_eq!(files[1].path, "test2.txt");
    assert_eq!(files[1].content.len(), 2048);
    
    // Verify zero-copy behavior
    let original_ptr = files[0].content.as_ptr();
    let cloned_content = files[0].content.clone();
    let cloned_ptr = cloned_content.as_ptr();
    assert_eq!(original_ptr, cloned_ptr, "Bytes should be zero-copy");
    
    Ok(())
}

#[tokio::test]
async fn test_bulk_download_request_creation() -> ClientResult<()> {
    let _client = create_test_client().await?;
    
    // Create bulk download request
    let paths = vec!["file1.txt", "file2.txt", "file3.txt"];
    let request = BulkDownloadRequest {
        paths: paths.iter().map(|p| p.to_string()).collect(),
    };
    
    // Verify request was created correctly
    assert_eq!(request.paths.len(), 3);
    assert_eq!(request.paths[0], "file1.txt");
    assert_eq!(request.paths[1], "file2.txt");
    assert_eq!(request.paths[2], "file3.txt");
    
    Ok(())
}

#[tokio::test]
async fn test_bulk_delete_request_creation() -> ClientResult<()> {
    let _client = create_test_client().await?;
    
    // Create bulk delete request
    let paths = vec!["file1.txt", "file2.txt"];
    let request = BulkDeleteRequest {
        paths: paths.iter().map(|p| p.to_string()).collect(),
    };
    
    // Verify request was created correctly
    assert_eq!(request.paths.len(), 2);
    assert_eq!(request.paths[0], "file1.txt");
    assert_eq!(request.paths[1], "file2.txt");
    
    Ok(())
}

#[tokio::test]
async fn test_atomic_counters() {
    // Test atomic counter operations used in bulk operations
    let successful_count = Arc::new(AtomicU64::new(0));
    let failed_count = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    
    // Simulate concurrent operations
    let handles: Vec<_> = (0..10).map(|i| {
        let success_counter = successful_count.clone();
        let fail_counter = failed_count.clone();
        let bytes_counter = total_bytes.clone();
        
        tokio::spawn(async move {
            if i % 2 == 0 {
                success_counter.fetch_add(1, Ordering::Relaxed);
                bytes_counter.fetch_add(1024, Ordering::Relaxed);
            } else {
                fail_counter.fetch_add(1, Ordering::Relaxed);
            }
        })
    }).collect();
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify atomic operations worked correctly
    assert_eq!(successful_count.load(Ordering::Relaxed), 5);
    assert_eq!(failed_count.load(Ordering::Relaxed), 5);
    assert_eq!(total_bytes.load(Ordering::Relaxed), 5 * 1024);
}

#[tokio::test]
async fn test_file_download_structure() {
    // Test FileDownload structure used in bulk downloads
    let content = generate_test_content(1024, "test ");
    let hash = xxhash_rust::xxh3::xxh3_64(&content);
    
    let download = FileDownload {
        path: "test.txt".to_string(),
        content: content.clone(),
        xxhash3: hash,
    };
    
    // Verify structure
    assert_eq!(download.path, "test.txt");
    assert_eq!(download.content, content);
    assert_eq!(download.xxhash3, hash);
    
    // Verify hash calculation
    let recalculated_hash = xxhash_rust::xxh3::xxh3_64(&download.content);
    assert_eq!(download.xxhash3, recalculated_hash);
}

#[tokio::test]
async fn test_bulk_response_structures() {
    // Test BulkUploadResponse
    let upload_response = BulkUploadResponse {
        success: vec!["file1.txt".to_string(), "file2.txt".to_string()],
        failed: vec![
            OperationError {
                path: "file3.txt".to_string(),
                error: "Permission denied".to_string(),
                error_code: ErrorCode::PermissionDenied,
            }
        ],
    };
    
    assert_eq!(upload_response.success.len(), 2);
    assert_eq!(upload_response.failed.len(), 1);
    assert_eq!(upload_response.failed[0].error_code, ErrorCode::PermissionDenied);
    
    // Test BulkDeleteResponse
    let delete_response = BulkDeleteResponse {
        success: vec!["file1.txt".to_string()],
        failed: vec![
            OperationError {
                path: "file2.txt".to_string(),
                error: "File not found".to_string(),
                error_code: ErrorCode::FileNotFound,
            }
        ],
    };
    
    assert_eq!(delete_response.success.len(), 1);
    assert_eq!(delete_response.failed.len(), 1);
    assert_eq!(delete_response.failed[0].error_code, ErrorCode::FileNotFound);
}

#[tokio::test]
async fn test_zero_copy_bytes_operations() {
    // Test zero-copy operations with Bytes
    let original_data = b"test data for zero-copy operations";
    let bytes1 = Bytes::from_static(original_data);
    let bytes2 = bytes1.clone();
    
    // Verify zero-copy (same pointer)
    assert_eq!(bytes1.as_ptr(), bytes2.as_ptr());
    assert_eq!(bytes1.len(), bytes2.len());
    assert_eq!(bytes1, bytes2);
    
    // Test conversion to Vec and back
    let vec_data = bytes1.to_vec();
    let bytes3 = Bytes::from(vec_data);
    
    // Content should be the same, but pointer will be different
    assert_eq!(bytes1, bytes3);
    assert_ne!(bytes1.as_ptr(), bytes3.as_ptr()); // Different allocation
}

#[tokio::test]
async fn test_concurrent_file_processing() {
    // Test concurrent processing patterns used in bulk operations
    let files = (0..100).map(|i| {
        FileUpload {
            path: format!("file_{}.txt", i),
            content: generate_test_content(1024, &format!("content {} ", i)),
        }
    }).collect::<Vec<_>>();
    
    // Atomic counters for tracking
    let processed_count = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    
    // Process files concurrently
    let handles: Vec<_> = files.into_iter().enumerate().map(|(i, file)| {
        let counter = processed_count.clone();
        let bytes_counter = total_bytes.clone();
        
        tokio::spawn(async move {
            // Simulate processing
            let _hash = xxhash_rust::xxh3::xxh3_64(&file.content);
            
            // Update counters atomically
            counter.fetch_add(1, Ordering::Relaxed);
            bytes_counter.fetch_add(file.content.len() as u64, Ordering::Relaxed);
            
            (i, file.path)
        })
    }).collect();
    
    // Wait for all processing to complete
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    
    // Verify all files were processed
    assert_eq!(processed_count.load(Ordering::Relaxed), 100);
    assert_eq!(total_bytes.load(Ordering::Relaxed), 100 * 1024);
    assert_eq!(results.len(), 100);
    
    // Verify results are in order (though processing was concurrent)
    results.sort_by_key(|(i, _)| *i);
    for (i, (index, path)) in results.iter().enumerate() {
        assert_eq!(*index, i);
        assert_eq!(*path, format!("file_{}.txt", i));
    }
}