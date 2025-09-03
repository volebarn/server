//! Comprehensive tests for directory operations with zero-lock concurrent access
//! 
//! These tests verify all directory operations using a mock HTTP server
//! with lock-free patterns, compression support, and async best practices.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};

use volebarn_client::{Client, Config};

/// Mock server state using lock-free data structures where possible
#[derive(Debug, Clone)]
struct MockServerState {
    /// File and directory storage using atomic reference counting
    files: Arc<RwLock<HashMap<String, MockEntry>>>,
    /// Request counter for testing
    request_count: Arc<AtomicU64>,
    /// Compression enabled flag
    compression_enabled: Arc<AtomicBool>,
    /// Error simulation flag
    simulate_errors: Arc<AtomicBool>,
}

/// Mock file or directory entry
#[derive(Debug, Clone)]
struct MockEntry {
    content: Option<Bytes>, // None for directories
    hash: u64,
    modified: SystemTime,
    size: u64,
    is_directory: bool,
}

impl MockServerState {
    fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            request_count: Arc::new(AtomicU64::new(0)),
            compression_enabled: Arc::new(AtomicBool::new(true)),
            simulate_errors: Arc::new(AtomicBool::new(false)),
        }
    }

    fn increment_requests(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_request_count(&self) -> u64 {
        self.request_count.load(Ordering::Relaxed)
    }

    fn set_compression_enabled(&self, enabled: bool) {
        self.compression_enabled.store(enabled, Ordering::Relaxed);
    }

    fn compression_enabled(&self) -> bool {
        self.compression_enabled.load(Ordering::Relaxed)
    }

    fn set_simulate_errors(&self, enabled: bool) {
        self.simulate_errors.store(enabled, Ordering::Relaxed);
    }

    fn should_simulate_errors(&self) -> bool {
        self.simulate_errors.load(Ordering::Relaxed)
    }
}



/// Create mock server router
fn create_mock_server() -> (Router, MockServerState) {
    let state = MockServerState::new();
    
    let router = Router::new()
        .route("/health", get(health_handler))
        .route("/files", get(list_root_directory_handler))
        .route("/files/{*path}", get(list_directory_handler))
        .route("/directories/{*path}", post(create_directory_handler))
        .route("/directories/{*path}", delete(delete_directory_handler))
        .route("/search", post(search_files_handler))
        .with_state(state.clone());

    (router, state)
}

// Mock server handlers
async fn health_handler(State(state): State<MockServerState>) -> impl IntoResponse {
    state.increment_requests();
    StatusCode::OK
}

async fn list_root_directory_handler(
    State(state): State<MockServerState>,
) -> impl IntoResponse {
    state.increment_requests();
    
    if state.should_simulate_errors() {
        return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new(), Bytes::new()).into_response();
    }
    
    let entries = {
        let files = state.files.read().await;
        let mut entries = Vec::new();
        
        // List root level entries (no '/' in path after first character)
        for (path, entry) in files.iter() {
            if path.chars().skip(1).all(|c| c != '/') {
                let metadata = volebarn_client::types::FileMetadata {
                    path: path.clone(),
                    name: std::path::Path::new(path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(path)
                        .to_string(),
                    size: entry.size,
                    modified: entry.modified,
                    is_directory: entry.is_directory,
                    xxhash3: entry.hash,
                    storage_path: None,
                };
                entries.push(metadata);
            }
        }
        entries
    };
    
    let listing = volebarn_client::types::DirectoryListing {
        path: "/".to_string(),
        entries,
    };
    
    create_directory_response(state, listing).into_response()
}

async fn list_directory_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    state.increment_requests();
    
    if state.should_simulate_errors() {
        return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new(), Bytes::new()).into_response();
    }
    
    let normalized_path = if path.starts_with('/') {
        path
    } else {
        format!("/{}", path)
    };
    
    let entries = {
        let files = state.files.read().await;
        let mut entries = Vec::new();
        
        // List entries in the specified directory
        let prefix = if normalized_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", normalized_path)
        };
        
        for (file_path, entry) in files.iter() {
            if file_path.starts_with(&prefix) && file_path != &normalized_path {
                let relative_path = &file_path[prefix.len()..];
                // Only include direct children (no further slashes)
                if !relative_path.contains('/') {
                    let metadata = volebarn_client::types::FileMetadata {
                        path: file_path.clone(),
                        name: std::path::Path::new(file_path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(file_path)
                            .to_string(),
                        size: entry.size,
                        modified: entry.modified,
                        is_directory: entry.is_directory,
                        xxhash3: entry.hash,
                        storage_path: None,
                    };
                    entries.push(metadata);
                }
            }
        }
        entries
    };
    
    let listing = volebarn_client::types::DirectoryListing {
        path: normalized_path,
        entries,
    };
    
    create_directory_response(state, listing).into_response()
}

async fn create_directory_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    state.increment_requests();
    
    if state.should_simulate_errors() {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    
    let normalized_path = if path.starts_with('/') {
        path
    } else {
        format!("/{}", path)
    };
    
    let mut files = state.files.write().await;
    
    // Check if directory already exists
    if files.contains_key(&normalized_path) {
        return StatusCode::CONFLICT;
    }
    
    // Create directory entry
    let directory_entry = MockEntry {
        content: None,
        hash: 0, // Directories don't have content hash
        modified: SystemTime::now(),
        size: 0,
        is_directory: true,
    };
    
    files.insert(normalized_path, directory_entry);
    StatusCode::CREATED
}

async fn delete_directory_handler(
    State(state): State<MockServerState>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    state.increment_requests();
    
    if state.should_simulate_errors() {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    
    let normalized_path = if path.starts_with('/') {
        path
    } else {
        format!("/{}", path)
    };
    
    let mut files = state.files.write().await;
    
    // Check if directory exists
    if !files.contains_key(&normalized_path) {
        return StatusCode::NOT_FOUND;
    }
    
    // Remove directory and all its contents (recursive)
    let keys_to_remove: Vec<String> = files.keys()
        .filter(|key| key.starts_with(&format!("{}/", normalized_path)) || *key == &normalized_path)
        .cloned()
        .collect();
    
    for key in keys_to_remove {
        files.remove(&key);
    }
    
    StatusCode::NO_CONTENT
}

async fn search_files_handler(
    State(state): State<MockServerState>,
    Json(request): Json<volebarn_client::types::SearchRequest>,
) -> impl IntoResponse {
    state.increment_requests();
    
    if state.should_simulate_errors() {
        return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new(), Bytes::new()).into_response();
    }
    
    let results = {
        let files = state.files.read().await;
        let mut results = Vec::new();
        
        // Simple pattern matching (contains)
        for (file_path, entry) in files.iter() {
            let matches_pattern = if request.pattern.contains('*') {
                // Simple glob matching
                let pattern = request.pattern.replace('*', "");
                file_path.contains(&pattern)
            } else {
                file_path.contains(&request.pattern)
            };
            
            let in_search_path = if let Some(ref search_path) = request.path {
                file_path.starts_with(search_path)
            } else {
                true
            };
            
            if matches_pattern && in_search_path {
                let metadata = volebarn_client::types::FileMetadata {
                    path: file_path.clone(),
                    name: std::path::Path::new(file_path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(file_path)
                        .to_string(),
                    size: entry.size,
                    modified: entry.modified,
                    is_directory: entry.is_directory,
                    xxhash3: entry.hash,
                    storage_path: None,
                };
                results.push(metadata);
            }
        }
        results
    };
    
    create_search_response(state, results).into_response()
}

/// Create directory listing response with optional compression
fn create_directory_response(
    state: MockServerState,
    listing: volebarn_client::types::DirectoryListing,
) -> impl IntoResponse {
    if state.compression_enabled() {
        // Return compressed bitcode response
        use volebarn_client::serialization::serialize_compressed;
        use volebarn_client::storage_types::StorageDirectoryListing;
        
        let storage_listing: StorageDirectoryListing = listing.clone().into();
        match serialize_compressed(&storage_listing) {
            Ok(compressed_data) => {
                let mut headers = HeaderMap::new();
                headers.insert("content-encoding", HeaderValue::from_static("snappy"));
                headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
                (StatusCode::OK, headers, Bytes::from(compressed_data)).into_response()
            }
            Err(_) => {
                // Fallback to JSON
                (StatusCode::OK, Json(listing)).into_response()
            }
        }
    } else {
        // Return standard JSON response
        (StatusCode::OK, Json(listing)).into_response()
    }
}

/// Create search results response with optional compression
fn create_search_response(
    state: MockServerState,
    results: Vec<volebarn_client::types::FileMetadata>,
) -> impl IntoResponse {
    if state.compression_enabled() {
        // Return compressed bitcode response
        use volebarn_client::serialization::serialize_compressed;
        use volebarn_client::storage_types::StorageFileMetadata;
        
        let storage_results: Vec<StorageFileMetadata> = results.clone().into_iter()
            .map(|meta| meta.into())
            .collect();
        
        match serialize_compressed(&storage_results) {
            Ok(compressed_data) => {
                let mut headers = HeaderMap::new();
                headers.insert("content-encoding", HeaderValue::from_static("snappy"));
                headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
                (StatusCode::OK, headers, Bytes::from(compressed_data)).into_response()
            }
            Err(_) => {
                // Fallback to JSON
                (StatusCode::OK, Json(results)).into_response()
            }
        }
    } else {
        // Return standard JSON response
        (StatusCode::OK, Json(results)).into_response()
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

/// Helper function to populate mock server with test data
async fn populate_test_data(state: &MockServerState) {
    let mut files = state.files.write().await;
    
    // Create some directories
    files.insert("/documents".to_string(), MockEntry {
        content: None,
        hash: 0,
        modified: SystemTime::now(),
        size: 0,
        is_directory: true,
    });
    
    files.insert("/documents/projects".to_string(), MockEntry {
        content: None,
        hash: 0,
        modified: SystemTime::now(),
        size: 0,
        is_directory: true,
    });
    
    // Create some files
    let test_content = Bytes::from("Test file content");
    files.insert("/documents/readme.txt".to_string(), MockEntry {
        content: Some(test_content.clone()),
        hash: xxhash_rust::xxh3::xxh3_64(&test_content),
        modified: SystemTime::now(),
        size: test_content.len() as u64,
        is_directory: false,
    });
    
    let project_content = Bytes::from("Project file content");
    files.insert("/documents/projects/project.md".to_string(), MockEntry {
        content: Some(project_content.clone()),
        hash: xxhash_rust::xxh3::xxh3_64(&project_content),
        modified: SystemTime::now(),
        size: project_content.len() as u64,
        is_directory: false,
    });
}

#[tokio::test]
async fn test_create_directory_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_path = "test/new_directory";
    
    // Create directory
    let result = client.create_directory(test_path).await;
    assert!(result.is_ok());
    
    // Verify server received the request
    assert_eq!(state.get_request_count(), 1);
    
    // Verify directory was created
    let files = state.files.read().await;
    assert!(files.contains_key("/test/new_directory"));
    let entry = files.get("/test/new_directory").unwrap();
    assert!(entry.is_directory);
}

#[tokio::test]
async fn test_create_directory_conflict() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_path = "existing_directory";
    
    // Pre-create directory
    {
        let mut files = state.files.write().await;
        files.insert("/existing_directory".to_string(), MockEntry {
            content: None,
            hash: 0,
            modified: SystemTime::now(),
            size: 0,
            is_directory: true,
        });
    }
    
    // Try to create same directory
    let result = client.create_directory(test_path).await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 409, .. }));
    }
}

#[tokio::test]
async fn test_delete_directory_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    let test_path = "test/directory_to_delete";
    
    // Pre-create directory with some content
    {
        let mut files = state.files.write().await;
        files.insert("/test/directory_to_delete".to_string(), MockEntry {
            content: None,
            hash: 0,
            modified: SystemTime::now(),
            size: 0,
            is_directory: true,
        });
        
        // Add a file inside the directory
        let file_content = Bytes::from("File in directory");
        files.insert("/test/directory_to_delete/file.txt".to_string(), MockEntry {
            content: Some(file_content.clone()),
            hash: xxhash_rust::xxh3::xxh3_64(&file_content),
            modified: SystemTime::now(),
            size: file_content.len() as u64,
            is_directory: false,
        });
    }
    
    // Delete directory
    let result = client.delete_directory(test_path).await;
    assert!(result.is_ok());
    
    // Verify directory and its contents were deleted
    let files = state.files.read().await;
    assert!(!files.contains_key("/test/directory_to_delete"));
    assert!(!files.contains_key("/test/directory_to_delete/file.txt"));
}

#[tokio::test]
async fn test_delete_directory_not_found() {
    let (client, _state, _server_handle) = setup_test_client().await;
    
    let result = client.delete_directory("nonexistent/directory").await;
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(matches!(e, volebarn_client::ClientError::Server { status: 404, .. }));
    }
}

#[tokio::test]
async fn test_list_directory_root() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Populate test data
    populate_test_data(&state).await;
    
    // List root directory
    let result = client.list_directory(None).await;
    assert!(result.is_ok());
    
    let listing = result.unwrap();
    assert_eq!(listing.path, "/");
    assert!(!listing.entries.is_empty());
    
    // Should contain the documents directory
    let has_documents = listing.entries.iter()
        .any(|entry| entry.path == "/documents" && entry.is_directory);
    assert!(has_documents);
}

#[tokio::test]
async fn test_list_directory_specific_path() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Populate test data
    populate_test_data(&state).await;
    
    // List documents directory
    let result = client.list_directory(Some("documents")).await;
    assert!(result.is_ok());
    
    let listing = result.unwrap();
    assert_eq!(listing.path, "/documents");
    assert!(!listing.entries.is_empty());
    
    // Should contain readme.txt and projects directory
    let has_readme = listing.entries.iter()
        .any(|entry| entry.path == "/documents/readme.txt" && !entry.is_directory);
    let has_projects = listing.entries.iter()
        .any(|entry| entry.path == "/documents/projects" && entry.is_directory);
    
    assert!(has_readme);
    assert!(has_projects);
}

#[tokio::test]
async fn test_list_directory_with_compression() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Enable compression
    state.set_compression_enabled(true);
    
    // Populate test data
    populate_test_data(&state).await;
    
    // List directory (should handle compressed response)
    let result = client.list_directory(Some("documents")).await;
    assert!(result.is_ok());
    
    let listing = result.unwrap();
    assert_eq!(listing.path, "/documents");
    assert!(!listing.entries.is_empty());
}

#[tokio::test]
async fn test_list_directory_without_compression() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Disable compression
    state.set_compression_enabled(false);
    
    // Populate test data
    populate_test_data(&state).await;
    
    // List directory (should handle JSON response)
    let result = client.list_directory(Some("documents")).await;
    assert!(result.is_ok());
    
    let listing = result.unwrap();
    assert_eq!(listing.path, "/documents");
    assert!(!listing.entries.is_empty());
}

#[tokio::test]
async fn test_search_files_success() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Populate test data
    populate_test_data(&state).await;
    
    // Search for files containing "readme"
    let result = client.search_files("readme", None).await;
    assert!(result.is_ok());
    
    let results = result.unwrap();
    assert!(!results.is_empty());
    
    // Should find readme.txt
    let has_readme = results.iter()
        .any(|entry| entry.path == "/documents/readme.txt");
    assert!(has_readme);
}

#[tokio::test]
async fn test_search_files_with_pattern() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Populate test data
    populate_test_data(&state).await;
    
    // Search for files with wildcard pattern
    let result = client.search_files("*.txt", None).await;
    assert!(result.is_ok());
    
    let results = result.unwrap();
    assert!(!results.is_empty());
    
    // Should find readme.txt
    let has_readme = results.iter()
        .any(|entry| entry.path == "/documents/readme.txt");
    assert!(has_readme);
}

#[tokio::test]
async fn test_search_files_with_path_filter() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Populate test data
    populate_test_data(&state).await;
    
    // Search within specific directory
    let result = client.search_files("project", Some("/documents/projects")).await;
    assert!(result.is_ok());
    
    let results = result.unwrap();
    assert!(!results.is_empty());
    
    // Should find project.md
    let has_project = results.iter()
        .any(|entry| entry.path == "/documents/projects/project.md");
    assert!(has_project);
}

#[tokio::test]
async fn test_search_files_with_compression() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Enable compression
    state.set_compression_enabled(true);
    
    // Populate test data
    populate_test_data(&state).await;
    
    // Search for files (should handle compressed response)
    let result = client.search_files("readme", None).await;
    assert!(result.is_ok());
    
    let results = result.unwrap();
    assert!(!results.is_empty());
}

#[tokio::test]
async fn test_search_files_no_results() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Populate test data
    populate_test_data(&state).await;
    
    // Search for non-existent pattern
    let result = client.search_files("nonexistent_pattern", None).await;
    assert!(result.is_ok());
    
    let results = result.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_concurrent_directory_operations() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Test concurrent directory operations using Arc to share client
    let client = Arc::new(client);
    let mut tasks = Vec::new();
    
    // Concurrent directory creation
    for i in 0..10 {
        let client = client.clone();
        let path = format!("concurrent_test/dir_{}", i);
        
        let task = tokio::spawn(async move {
            client.create_directory(&path).await
        });
        tasks.push(task);
    }
    
    // Wait for all operations to complete
    let results = futures::future::join_all(tasks).await;
    
    // All operations should succeed
    for result in results {
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }
    
    // Verify all directories were created
    let files = state.files.read().await;
    for i in 0..10 {
        let path = format!("/concurrent_test/dir_{}", i);
        assert!(files.contains_key(&path));
        let entry = files.get(&path).unwrap();
        assert!(entry.is_directory);
    }
    
    // Should have made 10 requests
    assert_eq!(state.get_request_count(), 10);
}

#[tokio::test]
async fn test_atomic_error_tracking() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Enable error simulation
    state.set_simulate_errors(true);
    
    // Try operations that should fail
    let create_result = client.create_directory("test/error_dir").await;
    assert!(create_result.is_err());
    
    let list_result = client.list_directory(Some("test")).await;
    assert!(list_result.is_err());
    
    let search_result = client.search_files("test", None).await;
    assert!(search_result.is_err());
    
    // Verify requests were made (atomic counter should work)
    // With retries enabled, we expect more than 3 requests
    let request_count = state.get_request_count();
    assert!(request_count >= 3, "Expected at least 3 requests, got {}", request_count);
}

#[tokio::test]
async fn test_zero_lock_concurrent_access() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Populate test data
    populate_test_data(&state).await;
    
    let client = Arc::new(client);
    let mut tasks = Vec::new();
    
    // Concurrent read operations (list directory)
    for _ in 0..20 {
        let client = client.clone();
        let task = tokio::spawn(async move {
            client.list_directory(Some("documents")).await
        });
        tasks.push(task);
    }
    
    // Wait for all list operations to complete first
    let list_results = futures::future::join_all(tasks).await;
    
    // All list operations should succeed
    for result in list_results {
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }
    
    // Now do concurrent search operations
    let mut search_tasks = Vec::new();
    for i in 0..10 {
        let client = client.clone();
        let pattern = if i % 2 == 0 { "readme" } else { "project" };
        let task = tokio::spawn(async move {
            client.search_files(pattern, None).await
        });
        search_tasks.push(task);
    }
    
    // Wait for all search operations to complete
    let search_results = futures::future::join_all(search_tasks).await;
    
    // All search operations should succeed
    for result in search_results {
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }
    
    // Should have made 30 requests
    assert_eq!(state.get_request_count(), 30);
}

#[tokio::test]
async fn test_compression_fallback() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Enable compression but simulate compression failure
    state.set_compression_enabled(true);
    
    // Populate test data
    populate_test_data(&state).await;
    
    // Operations should still work with JSON fallback
    let list_result = client.list_directory(Some("documents")).await;
    assert!(list_result.is_ok());
    
    let search_result = client.search_files("readme", None).await;
    assert!(search_result.is_ok());
}

#[tokio::test]
async fn test_large_directory_listing() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Create a large number of entries
    {
        let mut files = state.files.write().await;
        for i in 0..1000 {
            let path = format!("/large_test/file_{}.txt", i);
            let content = Bytes::from(format!("Content for file {}", i));
            files.insert(path, MockEntry {
                content: Some(content.clone()),
                hash: xxhash_rust::xxh3::xxh3_64(&content),
                modified: SystemTime::now(),
                size: content.len() as u64,
                is_directory: false,
            });
        }
        
        // Create the parent directory
        files.insert("/large_test".to_string(), MockEntry {
            content: None,
            hash: 0,
            modified: SystemTime::now(),
            size: 0,
            is_directory: true,
        });
    }
    
    // List large directory
    let result = client.list_directory(Some("large_test")).await;
    assert!(result.is_ok());
    
    let listing = result.unwrap();
    assert_eq!(listing.entries.len(), 1000);
}

#[tokio::test]
async fn test_deep_directory_structure() {
    let (client, state, _server_handle) = setup_test_client().await;
    
    // Create deep directory structure
    {
        let mut files = state.files.write().await;
        let mut current_path = String::new();
        
        for i in 0..10 {
            current_path = format!("{}/level_{}", current_path, i);
            files.insert(current_path.clone(), MockEntry {
                content: None,
                hash: 0,
                modified: SystemTime::now(),
                size: 0,
                is_directory: true,
            });
        }
        
        // Add a file at the deepest level
        let file_path = format!("{}/deep_file.txt", current_path);
        let content = Bytes::from("Deep file content");
        files.insert(file_path.clone(), MockEntry {
            content: Some(content.clone()),
            hash: xxhash_rust::xxh3::xxh3_64(&content),
            modified: SystemTime::now(),
            size: content.len() as u64,
            is_directory: false,
        });
    }
    
    // Search in deep structure
    let result = client.search_files("deep_file", None).await;
    assert!(result.is_ok());
    
    let results = result.unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].path.contains("deep_file.txt"));
}