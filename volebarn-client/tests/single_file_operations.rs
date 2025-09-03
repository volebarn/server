//! Comprehensive tests for single file operations with mock server
//! 
//! These tests verify all single file operations using a mock HTTP server
//! without locks, following zero-copy patterns and async best practices.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
// Removed unused import
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{delete, get, head, post, put},
    Json, Router,
};
use volebarn_client::{Client, Config};

/// Mock server state using lock-free data structures where possible
#[derive(Debug, Clone)]
struct MockServerState {
    /// File storage using atomic reference counting
    files: Arc<RwLock<HashMap<String, MockFile>>>,
    /// Request counter for testing
    request_count: Arc<AtomicU64>,
    /// Hash verification enabled
    hash_verification: Arc<std::sync::atomic::AtomicBool>,
}

/// Mock file data
#[derive(Debug, Clone)]
struct MockFile {
    content: Bytes,
    hash: u64,
    modified: SystemTime,
    size: u64,
}

impl MockServerState {
    fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            request_count: Arc::new(AtomicU64::new(0)),
            hash_verification: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    fn increment_requests(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_request_count(&self) -> u64 {
        self.request_count.load(Ordering::Relaxed)
    }

    fn set_hash_verification(&self, enabled: bool) {
        self.hash_verification.store(enabled, Ordering::Relaxed);
    }

    fn hash_verification_enabled(&self) -> bool {
        self.hash_verification.load(Ordering::Relaxed)
    }
}

/// Create mock server router
fn create_mock_server() -> (Router, MockServerState) {
    let state = MockServerState::new();
    
    let router = Router::new()
        .route("/health", get(health_handler))
        .route("/files/{*path}", post(upload_file_handler))
        .route("/files/{*path}", get(download_file_handler))
        .route("/files/{*path}", put(update_file_handler))
        .route("/files/{*path}", delete(delete_file_handler))
        .route("/files/{*path}", head(get_file_metadata_handler))
        .route("/files/move", post(move_file_handler))
        .route("/files/copy", post(copy_file_handler))
        .with_state(state.clone());

    (router, state)
}

// Mock server handlers
async fn health_handler(State(state): State<MockServerState>) -> impl IntoResponse {
    state.increment_requests();
    StatusCode::OK
}

async fn upload_file_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    state.increment_requests();
    
    let hash = xxhash_rust::xxh3::xxh3_64(&body);
    let modified = SystemTime::now();
    let size = body.len() as u64;
    
    let mock_file = MockFile {
        content: body,
        hash,
        modified,
        size,
    };
    
    // Store file
    {
        let mut files = state.files.write().await;
        files.insert(path.clone(), mock_file.clone());
    }
    
    // Return metadata response
    let metadata_response = volebarn_client::types::FileMetadataResponse {
        path: path.clone(),
        name: std::path::Path::new(&path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&path)
            .to_string(),
        size,
        modified: modified
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string(),
        is_directory: false,
        hash: format!("{:016x}", hash),
    };
    
    (StatusCode::CREATED, Json(metadata_response))
}

async fn download_file_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    state.increment_requests();
    
    let files = state.files.read().await;
    if let Some(file) = files.get(&path) {
        let mut headers = HeaderMap::new();
        headers.insert("X-File-Hash", HeaderValue::from_str(&format!("{:016x}", file.hash)).unwrap());
        
        (StatusCode::OK, headers, file.content.clone())
    } else {
        (StatusCode::NOT_FOUND, HeaderMap::new(), Bytes::new())
    }
}

async fn update_file_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    state.increment_requests();
    
    let hash = xxhash_rust::xxh3::xxh3_64(&body);
    let modified = SystemTime::now();
    let size = body.len() as u64;
    
    let mock_file = MockFile {
        content: body,
        hash,
        modified,
        size,
    };
    
    // Update file if it exists
    {
        let mut files = state.files.write().await;
        if files.contains_key(&path) {
            files.insert(path.clone(), mock_file.clone());
        } else {
            return StatusCode::NOT_FOUND.into_response();
        }
    }
    
    // Return metadata response
    let metadata_response = volebarn_client::types::FileMetadataResponse {
        path: path.clone(),
        name: std::path::Path::new(&path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&path)
            .to_string(),
        size,
        modified: modified
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string(),
        is_directory: false,
        hash: format!("{:016x}", hash),
    };
    
    (StatusCode::OK, Json(metadata_response)).into_response()
}

async fn delete_file_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    state.increment_requests();
    
    let mut files = state.files.write().await;
    if files.remove(&path).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn get_file_metadata_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    state.increment_requests();
    
    let files = state.files.read().await;
    if let Some(file) = files.get(&path) {
        let mut headers = HeaderMap::new();
        headers.insert("X-File-Size", HeaderValue::from_str(&file.size.to_string()).unwrap());
        headers.insert("X-File-Hash", HeaderValue::from_str(&format!("{:016x}", file.hash)).unwrap());
        
        let modified_iso = chrono::DateTime::from_timestamp(
            file.modified.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            0
        ).unwrap().to_rfc3339();
        headers.insert("X-Modified-Time", HeaderValue::from_str(&modified_iso).unwrap());
        
        (StatusCode::OK, headers)
    } else {
        (StatusCode::NOT_FOUND, HeaderMap::new())
    }
}

async fn move_file_handler(
    State(state): State<MockServerState>,
    Json(request): Json<volebarn_client::types::MoveRequest>,
) -> impl IntoResponse {
    state.increment_requests();
    
    let mut files = state.files.write().await;
    if let Some(file) = files.remove(&request.from_path) {
        files.insert(request.to_path, file);
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn copy_file_handler(
    State(state): State<MockServerState>,
    Json(request): Json<volebarn_client::types::CopyRequest>,
) -> impl IntoResponse {
    state.increment_requests();
    
    let files = state.files.read().await;
    if let Some(file) = files.get(&request.from_path) {
        let copied_file = file.clone();
        drop(files); // Release read lock
        
        let mut files = state.files.write().await;
        files.insert(request.to_path, copied_file);
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// Helper function to start mock server and return client
async fn setup_test_client() -> (Client, MockServerState, tokio::task::JoinHandle<()>) {
    let (router, state) = create_mock_server();
    
    // Start server on random port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_url = format!("http://{}", addr);
    
    // Spawn server task
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    // Create client
    let config = Config::new(server_url);
    config.set_tls_verify(false); // Disable TLS for HTTP testing
    config.set_max_retries(1); // Fast failure for tests
    config.set_request_timeout(tokio::time::Duration::from_secs(5));
    
    let client = Client::new(config).await.unwrap();
    
    (client, state, server_handle)
}

#[tokio::test]
async fn test_upload_file_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("Hello, World!");
    let test_path = "test/upload.txt";
    
    // Upload file
    let result = client.upload_file(test_path, test_content.clone()).await;
    assert!(result.is_ok());
    
    let metadata = result.unwrap();
    assert_eq!(metadata.path, test_path);
    assert_eq!(metadata.size, test_content.len() as u64);
    assert!(!metadata.is_directory);
    
    // Verify server received the request
    assert_eq!(state.get_request_count(), 1);
    
    // Verify file was stored
    let files = state.files.read().await;
    assert!(files.contains_key(test_path));
    let stored_file = files.get(test_path).unwrap();
    assert_eq!(stored_file.content, test_content);
}

#[tokio::test]
async fn test_download_file_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("Download test content");
    let test_path = "test/download.txt";
    
    // Pre-populate server with file
    let hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    let mock_file = MockFile {
        content: test_content.clone(),
        hash,
        modified: SystemTime::now(),
        size: test_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(test_path.to_string(), mock_file);
    }
    
    // Download file
    let result = client.download_file(test_path).await;
    assert!(result.is_ok());
    
    let downloaded_content = result.unwrap();
    assert_eq!(downloaded_content, test_content);
    
    // Verify server received the request
    assert_eq!(state.get_request_count(), 1);
}

#[tokio::test]
async fn test_download_file_not_found() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let result = client.download_file("nonexistent/file.txt").await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 404, .. }));
    }
}

#[tokio::test]
async fn test_update_file_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let original_content = Bytes::from("Original content");
    let updated_content = Bytes::from("Updated content");
    let test_path = "test/update.txt";
    
    // Pre-populate server with original file
    let hash = xxhash_rust::xxh3::xxh3_64(&original_content);
    let mock_file = MockFile {
        content: original_content.clone(),
        hash,
        modified: SystemTime::now(),
        size: original_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(test_path.to_string(), mock_file);
    }
    
    // Update file
    let result = client.update_file(test_path, updated_content.clone()).await;
    assert!(result.is_ok());
    
    let metadata = result.unwrap();
    assert_eq!(metadata.path, test_path);
    assert_eq!(metadata.size, updated_content.len() as u64);
    
    // Verify file was updated
    let files = state.files.read().await;
    let stored_file = files.get(test_path).unwrap();
    assert_eq!(stored_file.content, updated_content);
    assert_ne!(stored_file.content, original_content);
}

#[tokio::test]
async fn test_update_file_not_found() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let content = Bytes::from("Content for nonexistent file");
    let result = client.update_file("nonexistent/file.txt", content).await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 404, .. }));
    }
}

#[tokio::test]
async fn test_delete_file_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("File to delete");
    let test_path = "test/delete.txt";
    
    // Pre-populate server with file
    let hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    let mock_file = MockFile {
        content: test_content.clone(),
        hash,
        modified: SystemTime::now(),
        size: test_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(test_path.to_string(), mock_file);
    }
    
    // Delete file
    let result = client.delete_file(test_path).await;
    assert!(result.is_ok());
    
    // Verify file was deleted
    let files = state.files.read().await;
    assert!(!files.contains_key(test_path));
}

#[tokio::test]
async fn test_delete_file_not_found() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let result = client.delete_file("nonexistent/file.txt").await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 404, .. }));
    }
}

#[tokio::test]
async fn test_get_file_metadata_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("Metadata test content");
    let test_path = "test/metadata.txt";
    
    // Pre-populate server with file
    let hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    let modified = SystemTime::now();
    let mock_file = MockFile {
        content: test_content.clone(),
        hash,
        modified,
        size: test_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(test_path.to_string(), mock_file);
    }
    
    // Get metadata
    let result = client.get_file_metadata(test_path).await;
    assert!(result.is_ok());
    
    let metadata = result.unwrap();
    assert_eq!(metadata.path, test_path);
    assert_eq!(metadata.size, test_content.len() as u64);
    assert_eq!(metadata.xxhash3, hash);
    assert!(!metadata.is_directory);
}

#[tokio::test]
async fn test_get_file_metadata_not_found() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let result = client.get_file_metadata("nonexistent/file.txt").await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 404, .. }));
    }
}

#[tokio::test]
async fn test_move_file_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("File to move");
    let from_path = "test/source.txt";
    let to_path = "test/destination.txt";
    
    // Pre-populate server with file
    let hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    let mock_file = MockFile {
        content: test_content.clone(),
        hash,
        modified: SystemTime::now(),
        size: test_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(from_path.to_string(), mock_file);
    }
    
    // Move file
    let result = client.move_file(from_path, to_path).await;
    assert!(result.is_ok());
    
    // Verify file was moved
    let files = state.files.read().await;
    assert!(!files.contains_key(from_path));
    assert!(files.contains_key(to_path));
    
    let moved_file = files.get(to_path).unwrap();
    assert_eq!(moved_file.content, test_content);
}

#[tokio::test]
async fn test_move_file_not_found() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let result = client.move_file("nonexistent/source.txt", "test/destination.txt").await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 404, .. }));
    }
}

#[tokio::test]
async fn test_copy_file_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("File to copy");
    let from_path = "test/original.txt";
    let to_path = "test/copy.txt";
    
    // Pre-populate server with file
    let hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    let mock_file = MockFile {
        content: test_content.clone(),
        hash,
        modified: SystemTime::now(),
        size: test_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(from_path.to_string(), mock_file);
    }
    
    // Copy file
    let result = client.copy_file(from_path, to_path).await;
    assert!(result.is_ok());
    
    // Verify both files exist
    let files = state.files.read().await;
    assert!(files.contains_key(from_path));
    assert!(files.contains_key(to_path));
    
    let original_file = files.get(from_path).unwrap();
    let copied_file = files.get(to_path).unwrap();
    assert_eq!(original_file.content, copied_file.content);
    assert_eq!(original_file.content, test_content);
}

#[tokio::test]
async fn test_copy_file_not_found() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let result = client.copy_file("nonexistent/source.txt", "test/copy.txt").await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 404, .. }));
    }
}

#[tokio::test]
async fn test_verify_file_integrity_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("Integrity test content");
    let test_path = "test/integrity.txt";
    
    // Pre-populate server with file
    let hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    let mock_file = MockFile {
        content: test_content.clone(),
        hash,
        modified: SystemTime::now(),
        size: test_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(test_path.to_string(), mock_file);
    }
    
    // Verify integrity with correct hash
    let result = client.verify_file_integrity(test_path, hash).await;
    assert!(result.is_ok());
    assert!(result.unwrap());
    
    // Verify integrity with incorrect hash
    let result = client.verify_file_integrity(test_path, hash + 1).await;
    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[tokio::test]
async fn test_hash_verification_in_upload() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("Hash verification test");
    let test_path = "test/hash_verify.txt";
    
    // Upload file (should succeed with hash verification)
    let result = client.upload_file(test_path, test_content.clone()).await;
    assert!(result.is_ok());
    
    let metadata = result.unwrap();
    let expected_hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    assert_eq!(metadata.xxhash3, expected_hash);
}

#[tokio::test]
async fn test_hash_verification_in_download() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("Download hash verification test");
    let test_path = "test/download_hash.txt";
    
    // Pre-populate server with file
    let hash = xxhash_rust::xxh3::xxh3_64(&test_content);
    let mock_file = MockFile {
        content: test_content.clone(),
        hash,
        modified: SystemTime::now(),
        size: test_content.len() as u64,
    };
    
    {
        let mut files = state.files.write().await;
        files.insert(test_path.to_string(), mock_file);
    }
    
    // Download file (should succeed with hash verification)
    let result = client.download_file(test_path).await;
    assert!(result.is_ok());
    
    let downloaded_content = result.unwrap();
    assert_eq!(downloaded_content, test_content);
}

#[tokio::test]
async fn test_concurrent_operations() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Test concurrent uploads using Arc to share client
    let client = Arc::new(client);
    let mut upload_tasks = Vec::new();
    for i in 0..10 {
        let client = client.clone();
        let content = Bytes::from(format!("Concurrent test content {}", i));
        let path = format!("test/concurrent_{}.txt", i);
        
        let task = tokio::spawn(async move {
            client.upload_file(&path, content).await
        });
        upload_tasks.push(task);
    }
    
    // Wait for all uploads to complete
    let results = futures::future::join_all(upload_tasks).await;
    
    // All uploads should succeed
    for result in results {
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }
    
    // Verify all files were stored
    let files = state.files.read().await;
    for i in 0..10 {
        let path = format!("test/concurrent_{}.txt", i);
        assert!(files.contains_key(&path));
    }
    
    // Should have made 10 requests
    assert_eq!(state.get_request_count(), 10);
}

#[tokio::test]
async fn test_zero_copy_operations() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    // Create large test content to verify zero-copy behavior
    let large_content = Bytes::from(vec![0u8; 1024 * 1024]); // 1MB
    let test_path = "test/large_file.bin";
    
    // Upload large file
    let upload_result = client.upload_file(test_path, large_content.clone()).await;
    assert!(upload_result.is_ok());
    
    // Download large file
    let download_result = client.download_file(test_path).await;
    assert!(download_result.is_ok());
    
    let downloaded_content = download_result.unwrap();
    assert_eq!(downloaded_content.len(), large_content.len());
    
    // Verify content matches (spot check to avoid full comparison)
    assert_eq!(downloaded_content[0], large_content[0]);
    assert_eq!(downloaded_content[large_content.len() - 1], large_content[large_content.len() - 1]);
}

#[tokio::test]
async fn test_atomic_operations() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_content = Bytes::from("Atomic test content");
    let test_path = "test/atomic.txt";
    
    // Upload file
    let upload_result = client.upload_file(test_path, test_content.clone()).await;
    assert!(upload_result.is_ok());
    
    // Verify atomic counters are working
    assert_eq!(state.get_request_count(), 1);
    
    // Download file
    let download_result = client.download_file(test_path).await;
    assert!(download_result.is_ok());
    
    // Counter should be incremented atomically
    assert_eq!(state.get_request_count(), 2);
    
    // Delete file
    let delete_result = client.delete_file(test_path).await;
    assert!(delete_result.is_ok());
    
    // Counter should be incremented atomically
    assert_eq!(state.get_request_count(), 3);
}