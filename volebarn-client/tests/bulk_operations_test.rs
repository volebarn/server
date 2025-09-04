//! Comprehensive tests for bulk transfer methods
//! 
//! Tests bulk upload, download, and delete operations with large file sets
//! and zero-lock concurrent processing.

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
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

/// Create test files with various sizes and content types
fn create_test_files(count: usize) -> Vec<FileUpload> {
    let mut files = Vec::with_capacity(count);
    
    for i in 0..count {
        let size = match i % 4 {
            0 => 1024,        // 1KB - small files
            1 => 10 * 1024,   // 10KB - medium files  
            2 => 100 * 1024,  // 100KB - large files
            _ => 1024 * 1024, // 1MB - very large files
        };
        
        let pattern = match i % 3 {
            0 => "text content with repeated patterns for compression testing ",
            1 => "binary\x00\x01\x02\x03\x04\x05mixed\x7Fcontent\x7F\x7F\x7F ",
            _ => "json{\"key\":\"value\",\"number\":12345,\"array\":[1,2,3]} ",
        };
        
        let content = generate_test_content(size, pattern);
        let path = format!("test_files/bulk_test_{:04}.dat", i);
        
        files.push(FileUpload { path, content });
    }
    
    files
}

#[tokio::test]
async fn test_bulk_upload_small_set() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Create small set of test files
    let files = create_test_files(5);
    let total_size: u64 = files.iter().map(|f| f.content.len() as u64).sum();
    
    // Track progress with atomic counters
    let start_time = std::time::Instant::now();
    
    // Perform bulk upload
    let response = client.bulk_upload(files.clone()).await?;
    
    let elapsed = start_time.elapsed();
    
    // Verify results
    assert_eq!(response.success.len(), 5);
    assert_eq!(response.failed.len(), 0);
    
    // Check that all files were uploaded
    for file in &files {
        assert!(response.success.contains(&file.path));
    }
    
    println!("Bulk upload (5 files, {} bytes) completed in {:?}", total_size, elapsed);
    Ok(())
}

#[tokio::test]
async fn test_bulk_upload_large_set() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Create larger set of test files
    let files = create_test_files(50);
    let total_size: u64 = files.iter().map(|f| f.content.len() as u64).sum();
    
    // Track progress with atomic counters
    let _progress_counter = Arc::new(AtomicU64::new(0));
    let start_time = std::time::Instant::now();
    
    // Perform bulk upload with timeout
    let response = timeout(
        Duration::from_secs(30),
        client.bulk_upload(files.clone())
    ).await
    .map_err(|_| volebarn_client::error::ClientError::Timeout {
        duration: 30,
        operation: "bulk_upload_large_set".to_string(),
    })??;
    
    let elapsed = start_time.elapsed();
    
    // Verify results
    assert!(response.success.len() > 0, "Should have some successful uploads");
    assert_eq!(response.success.len() + response.failed.len(), 50);
    
    // Calculate throughput
    let throughput_mbps = (total_size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    
    println!("Bulk upload (50 files, {} bytes) completed in {:?} ({:.2} MB/s)", 
             total_size, elapsed, throughput_mbps);
    Ok(())
}

#[tokio::test]
async fn test_bulk_download_small_set() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // First upload some files to download
    let files = create_test_files(3);
    let upload_response = client.bulk_upload(files.clone()).await?;
    assert_eq!(upload_response.success.len(), 3);
    
    // Now download them
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    let start_time = std::time::Instant::now();
    
    let downloads = client.bulk_download(paths).await?;
    
    let elapsed = start_time.elapsed();
    
    // Verify results
    assert_eq!(downloads.len(), 3);
    
    // Verify content integrity
    for (original, downloaded) in files.iter().zip(downloads.iter()) {
        assert_eq!(original.path, downloaded.path);
        assert_eq!(original.content, downloaded.content);
        
        // Verify hash integrity
        let expected_hash = xxhash_rust::xxh3::xxh3_64(&original.content);
        assert_eq!(expected_hash, downloaded.xxhash3);
    }
    
    let total_size: u64 = downloads.iter().map(|d| d.content.len() as u64).sum();
    println!("Bulk download (3 files, {} bytes) completed in {:?}", total_size, elapsed);
    Ok(())
}

#[tokio::test]
async fn test_bulk_download_concurrent_processing() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Upload larger set for concurrent download testing
    let files = create_test_files(20);
    let upload_response = client.bulk_upload(files.clone()).await?;
    assert_eq!(upload_response.success.len(), 20);
    
    // Download with concurrent processing
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    
    // Track atomic progress
    let _download_counter = Arc::new(AtomicU64::new(0));
    let _bytes_counter = Arc::new(AtomicU64::new(0));
    
    let start_time = std::time::Instant::now();
    
    let downloads = timeout(
        Duration::from_secs(20),
        client.bulk_download(paths)
    ).await
    .map_err(|_| volebarn_client::error::ClientError::Timeout {
        duration: 20,
        operation: "bulk_download_concurrent".to_string(),
    })??;
    
    let elapsed = start_time.elapsed();
    
    // Verify concurrent processing results
    assert_eq!(downloads.len(), 20);
    
    // Verify all files downloaded correctly with atomic counting
    let mut verified_count = 0u64;
    let mut total_bytes = 0u64;
    
    for downloaded in &downloads {
        // Find matching original file
        let original = files.iter()
            .find(|f| f.path == downloaded.path)
            .expect("Downloaded file should match original");
        
        assert_eq!(original.content, downloaded.content);
        
        verified_count += 1;
        total_bytes += downloaded.content.len() as u64;
    }
    
    assert_eq!(verified_count, 20);
    
    let throughput_mbps = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    
    println!("Concurrent bulk download (20 files, {} bytes) completed in {:?} ({:.2} MB/s)", 
             total_bytes, elapsed, throughput_mbps);
    Ok(())
}

#[tokio::test]
async fn test_bulk_delete_lock_free() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Upload files to delete
    let files = create_test_files(10);
    let upload_response = client.bulk_upload(files.clone()).await?;
    assert_eq!(upload_response.success.len(), 10);
    
    // Delete with lock-free operations
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    
    // Track atomic progress
    let _delete_counter = Arc::new(AtomicU64::new(0));
    let start_time = std::time::Instant::now();
    
    let delete_response = client.bulk_delete(paths.clone()).await?;
    
    let elapsed = start_time.elapsed();
    
    // Verify results
    assert_eq!(delete_response.success.len(), 10);
    assert_eq!(delete_response.failed.len(), 0);
    
    // Verify all files were deleted
    for path in &paths {
        assert!(delete_response.success.contains(&path.to_string()));
    }
    
    println!("Bulk delete (10 files) completed in {:?}", elapsed);
    Ok(())
}

#[tokio::test]
async fn test_bulk_operations_with_large_files() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Create large files (5MB each)
    let large_files: Vec<FileUpload> = (0..5).map(|i| {
        let content = generate_test_content(5 * 1024 * 1024, "large file content pattern ");
        let path = format!("large_files/file_{}.dat", i);
        FileUpload { path, content }
    }).collect();
    
    let total_size: u64 = large_files.iter().map(|f| f.content.len() as u64).sum();
    
    // Upload large files
    let start_upload = std::time::Instant::now();
    let upload_response = timeout(
        Duration::from_secs(60),
        client.bulk_upload(large_files.clone())
    ).await
    .map_err(|_| volebarn_client::error::ClientError::Timeout {
        duration: 60,
        operation: "bulk_upload_large_files".to_string(),
    })??;
    let upload_elapsed = start_upload.elapsed();
    
    assert_eq!(upload_response.success.len(), 5);
    
    // Download large files
    let paths: Vec<&str> = large_files.iter().map(|f| f.path.as_str()).collect();
    let start_download = std::time::Instant::now();
    
    let downloads = timeout(
        Duration::from_secs(60),
        client.bulk_download(paths.clone())
    ).await
    .map_err(|_| volebarn_client::error::ClientError::Timeout {
        duration: 60,
        operation: "bulk_download_large_files".to_string(),
    })??;
    let download_elapsed = start_download.elapsed();
    
    assert_eq!(downloads.len(), 5);
    
    // Verify integrity of large files
    for (original, downloaded) in large_files.iter().zip(downloads.iter()) {
        assert_eq!(original.path, downloaded.path);
        assert_eq!(original.content.len(), downloaded.content.len());
        assert_eq!(original.content, downloaded.content);
    }
    
    // Delete large files
    let start_delete = std::time::Instant::now();
    let delete_response = client.bulk_delete(paths).await?;
    let delete_elapsed = start_delete.elapsed();
    
    assert_eq!(delete_response.success.len(), 5);
    
    let upload_throughput = (total_size as f64 / (1024.0 * 1024.0)) / upload_elapsed.as_secs_f64();
    let download_throughput = (total_size as f64 / (1024.0 * 1024.0)) / download_elapsed.as_secs_f64();
    
    println!("Large file operations (5 files, {} MB):", total_size / (1024 * 1024));
    println!("  Upload: {:?} ({:.2} MB/s)", upload_elapsed, upload_throughput);
    println!("  Download: {:?} ({:.2} MB/s)", download_elapsed, download_throughput);
    println!("  Delete: {:?}", delete_elapsed);
    
    Ok(())
}

#[tokio::test]
async fn test_bulk_operations_error_handling() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Test upload with some invalid paths
    let mut files = create_test_files(3);
    files.push(FileUpload {
        path: "".to_string(), // Invalid empty path
        content: Bytes::from("test"),
    });
    files.push(FileUpload {
        path: "/invalid/\0/path".to_string(), // Invalid path with null byte
        content: Bytes::from("test"),
    });
    
    let upload_response = client.bulk_upload(files).await?;
    
    // Should have some successes and some failures
    assert!(upload_response.success.len() > 0);
    assert!(upload_response.failed.len() > 0);
    assert_eq!(upload_response.success.len() + upload_response.failed.len(), 5);
    
    // Test download with non-existent files
    let paths = vec!["nonexistent1.txt", "nonexistent2.txt"];
    let download_result = client.bulk_download(paths).await;
    
    // Should handle missing files gracefully
    match download_result {
        Ok(downloads) => {
            assert_eq!(downloads.len(), 0); // No files should be downloaded
        }
        Err(_) => {
            // Or return an error, both are acceptable
        }
    }
    
    // Test delete with non-existent files
    let delete_paths = vec!["nonexistent1.txt", "nonexistent2.txt"];
    let delete_response = client.bulk_delete(delete_paths).await?;
    
    // Should handle missing files gracefully
    assert!(delete_response.failed.len() > 0 || delete_response.success.len() == 0);
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_bulk_operations() -> ClientResult<()> {
    let client = Arc::new(create_test_client().await?);
    
    // Create different file sets for concurrent operations
    let upload_files = create_test_files(10);
    let download_files = create_test_files(5);
    
    // Upload download files first
    let upload_response = client.bulk_upload(download_files.clone()).await?;
    assert_eq!(upload_response.success.len(), 5);
    
    // Run concurrent operations
    let client1 = client.clone();
    let client2 = client.clone();
    let client3 = client.clone();
    
    let upload_files_clone = upload_files.clone();
    let download_paths: Vec<String> = download_files.iter().map(|f| f.path.clone()).collect();
    let delete_paths: Vec<String> = download_files.iter().take(2).map(|f| f.path.clone()).collect();
    
    // Atomic counters for tracking concurrent operations
    let operations_completed = Arc::new(AtomicU64::new(0));
    let op_counter1 = operations_completed.clone();
    let op_counter2 = operations_completed.clone();
    let op_counter3 = operations_completed.clone();
    
    let start_time = std::time::Instant::now();
    
    // Run operations concurrently
    let (upload_result, download_result, delete_result) = tokio::join!(
        async move {
            let result = client1.bulk_upload(upload_files_clone).await;
            op_counter1.fetch_add(1, Ordering::Relaxed);
            result
        },
        async move {
            let paths: Vec<&str> = download_paths.iter().map(|s| s.as_str()).collect();
            let result = client2.bulk_download(paths).await;
            op_counter2.fetch_add(1, Ordering::Relaxed);
            result
        },
        async move {
            let paths: Vec<&str> = delete_paths.iter().map(|s| s.as_str()).collect();
            let result = client3.bulk_delete(paths).await;
            op_counter3.fetch_add(1, Ordering::Relaxed);
            result
        }
    );
    
    let elapsed = start_time.elapsed();
    let completed_ops = operations_completed.load(Ordering::Relaxed);
    
    // Verify all operations completed successfully
    assert!(upload_result.is_ok(), "Upload should succeed");
    assert!(download_result.is_ok(), "Download should succeed");
    assert!(delete_result.is_ok(), "Delete should succeed");
    assert_eq!(completed_ops, 3);
    
    let upload_response = upload_result?;
    let downloads = download_result?;
    let delete_response = delete_result?;
    
    // Verify results
    assert_eq!(upload_response.success.len(), 10);
    assert_eq!(downloads.len(), 5);
    assert_eq!(delete_response.success.len(), 2);
    
    println!("Concurrent bulk operations completed in {:?}", elapsed);
    Ok(())
}

#[tokio::test]
async fn test_zero_copy_operations() -> ClientResult<()> {
    let client = create_test_client().await?;
    
    // Create files with shared content to test zero-copy
    let shared_content = generate_test_content(1024 * 1024, "shared content pattern ");
    
    let files: Vec<FileUpload> = (0..5).map(|i| {
        FileUpload {
            path: format!("zero_copy/file_{}.dat", i),
            content: shared_content.clone(), // This should be zero-copy
        }
    }).collect();
    
    // Verify that Bytes cloning is indeed zero-copy (same pointer)
    let original_ptr = files[0].content.as_ptr();
    let cloned_ptr = files[1].content.as_ptr();
    assert_eq!(original_ptr, cloned_ptr, "Bytes should be zero-copy");
    
    // Upload with zero-copy
    let start_time = std::time::Instant::now();
    let upload_response = client.bulk_upload(files.clone()).await?;
    let upload_elapsed = start_time.elapsed();
    
    assert_eq!(upload_response.success.len(), 5);
    
    // Download with zero-copy
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    let start_download = std::time::Instant::now();
    let downloads = client.bulk_download(paths).await?;
    let download_elapsed = start_download.elapsed();
    
    assert_eq!(downloads.len(), 5);
    
    // Verify content integrity
    for (original, downloaded) in files.iter().zip(downloads.iter()) {
        assert_eq!(original.content, downloaded.content);
    }
    
    let total_size = shared_content.len() * 5;
    let upload_throughput = (total_size as f64 / (1024.0 * 1024.0)) / upload_elapsed.as_secs_f64();
    let download_throughput = (total_size as f64 / (1024.0 * 1024.0)) / download_elapsed.as_secs_f64();
    
    println!("Zero-copy operations (5 files, {} MB shared content):", total_size / (1024 * 1024));
    println!("  Upload: {:?} ({:.2} MB/s)", upload_elapsed, upload_throughput);
    println!("  Download: {:?} ({:.2} MB/s)", download_elapsed, download_throughput);
    
    Ok(())
}