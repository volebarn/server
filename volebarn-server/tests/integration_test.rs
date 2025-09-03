//! Integration tests for single file operation endpoints
//! 
//! These tests verify all single file operations with concurrent access scenarios:
//! - POST /files/*path - Upload files
//! - GET /files/*path - Download files  
//! - PUT /files/*path - Update files
//! - DELETE /files/*path - Delete files
//! - HEAD /files/*path - Get file metadata

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    response::Json,
    routing::get,
    Router,
};
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;
use volebarn_server::{
    handlers::AppState,
    types::FileMetadataResponse,
};

/// Helper function to create a test server with temporary storage
async fn create_test_server() -> (Router, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_path_buf();
    
    let app_state = AppState::new(&storage_path).await.unwrap();
    
    let app = Router::new()
        .route("/health", get(|| async {
            (StatusCode::OK, Json(json!({"status": "healthy"})))
        }))
        .route("/files/{*path}", axum::routing::post(volebarn_server::handlers::upload_file))
        .route("/files/{*path}", axum::routing::get(volebarn_server::handlers::download_file))
        .route("/files/{*path}", axum::routing::put(volebarn_server::handlers::update_file))
        .route("/files/{*path}", axum::routing::delete(volebarn_server::handlers::delete_file))
        .route("/files/{*path}", axum::routing::head(volebarn_server::handlers::get_file_metadata))
        .with_state(app_state);
    
    (app, temp_dir)
}

/// Test file upload endpoint
#[tokio::test]
async fn test_upload_file() {
    let (app, _temp_dir) = create_test_server().await;
    
    let file_content = b"Hello, World!";
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/test.txt")
        .body(Body::from(file_content.as_slice()))
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    
    assert_eq!(response.status(), StatusCode::CREATED);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let metadata: FileMetadataResponse = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(metadata.path, "test.txt");
    assert_eq!(metadata.name, "test.txt");
    assert_eq!(metadata.size, file_content.len() as u64);
    assert!(!metadata.is_directory);
    assert!(!metadata.hash.is_empty());
}

/// Test file download endpoint
#[tokio::test]
async fn test_download_file() {
    let (app, _temp_dir) = create_test_server().await;
    
    let file_content = b"Hello, World!";
    
    // First upload a file
    let upload_request = Request::builder()
        .method(Method::POST)
        .uri("/files/test.txt")
        .body(Body::from(file_content.as_slice()))
        .unwrap();
    
    let upload_response = app.clone().oneshot(upload_request).await.unwrap();
    assert_eq!(upload_response.status(), StatusCode::CREATED);
    
    // Then download it
    let download_request = Request::builder()
        .method(Method::GET)
        .uri("/files/test.txt")
        .body(Body::empty())
        .unwrap();
    
    let download_response = app.oneshot(download_request).await.unwrap();
    assert_eq!(download_response.status(), StatusCode::OK);
    
    // Check headers
    let headers = download_response.headers();
    assert!(headers.contains_key("X-File-Hash"));
    assert_eq!(headers.get("Content-Type").unwrap(), "application/octet-stream");
    
    // Check content
    let body = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), file_content);
}

/// Test file update endpoint
#[tokio::test]
async fn test_update_file() {
    let (app, _temp_dir) = create_test_server().await;
    
    let original_content = b"Original content";
    let updated_content = b"Updated content";
    
    // First upload a file
    let upload_request = Request::builder()
        .method(Method::POST)
        .uri("/files/test.txt")
        .body(Body::from(original_content.as_slice()))
        .unwrap();
    
    let upload_response = app.clone().oneshot(upload_request).await.unwrap();
    assert_eq!(upload_response.status(), StatusCode::CREATED);
    
    // Then update it
    let update_request = Request::builder()
        .method(Method::PUT)
        .uri("/files/test.txt")
        .body(Body::from(updated_content.as_slice()))
        .unwrap();
    
    let update_response = app.clone().oneshot(update_request).await.unwrap();
    assert_eq!(update_response.status(), StatusCode::OK);
    
    // Verify the update
    let download_request = Request::builder()
        .method(Method::GET)
        .uri("/files/test.txt")
        .body(Body::empty())
        .unwrap();
    
    let download_response = app.oneshot(download_request).await.unwrap();
    let body = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), updated_content);
}

/// Test file delete endpoint
#[tokio::test]
async fn test_delete_file() {
    let (app, _temp_dir) = create_test_server().await;
    
    let file_content = b"Hello, World!";
    
    // First upload a file
    let upload_request = Request::builder()
        .method(Method::POST)
        .uri("/files/test.txt")
        .body(Body::from(file_content.as_slice()))
        .unwrap();
    
    let upload_response = app.clone().oneshot(upload_request).await.unwrap();
    assert_eq!(upload_response.status(), StatusCode::CREATED);
    
    // Then delete it
    let delete_request = Request::builder()
        .method(Method::DELETE)
        .uri("/files/test.txt")
        .body(Body::empty())
        .unwrap();
    
    let delete_response = app.clone().oneshot(delete_request).await.unwrap();
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);
    
    // Verify it's gone
    let download_request = Request::builder()
        .method(Method::GET)
        .uri("/files/test.txt")
        .body(Body::empty())
        .unwrap();
    
    let download_response = app.oneshot(download_request).await.unwrap();
    assert_eq!(download_response.status(), StatusCode::NOT_FOUND);
}

/// Test file metadata endpoint (HEAD)
#[tokio::test]
async fn test_get_file_metadata() {
    let (app, _temp_dir) = create_test_server().await;
    
    let file_content = b"Hello, World!";
    
    // First upload a file
    let upload_request = Request::builder()
        .method(Method::POST)
        .uri("/files/test.txt")
        .body(Body::from(file_content.as_slice()))
        .unwrap();
    
    let upload_response = app.clone().oneshot(upload_request).await.unwrap();
    assert_eq!(upload_response.status(), StatusCode::CREATED);
    
    // Then get metadata
    let metadata_request = Request::builder()
        .method(Method::HEAD)
        .uri("/files/test.txt")
        .body(Body::empty())
        .unwrap();
    
    let metadata_response = app.oneshot(metadata_request).await.unwrap();
    assert_eq!(metadata_response.status(), StatusCode::OK);
    
    // Check headers
    let headers = metadata_response.headers();
    assert!(headers.contains_key("X-File-Size"));
    assert!(headers.contains_key("X-File-Hash"));
    assert!(headers.contains_key("X-Modified-Time"));
    
    let size = headers.get("X-File-Size").unwrap().to_str().unwrap();
    assert_eq!(size, file_content.len().to_string());
}

/// Test error cases
#[tokio::test]
async fn test_error_cases() {
    let (app, _temp_dir) = create_test_server().await;
    
    // Test downloading non-existent file
    let request = Request::builder()
        .method(Method::GET)
        .uri("/files/nonexistent.txt")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    
    // Test updating non-existent file
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/files/nonexistent.txt")
        .body(Body::from("content"))
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    
    // Test deleting non-existent file
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/files/nonexistent.txt")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    
    // Test metadata for non-existent file
    let request = Request::builder()
        .method(Method::HEAD)
        .uri("/files/nonexistent.txt")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    
    // Test uploading file that already exists
    let file_content = b"Hello, World!";
    
    // Upload once
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/duplicate.txt")
        .body(Body::from(file_content.as_slice()))
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    
    // Try to upload again
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/duplicate.txt")
        .body(Body::from(file_content.as_slice()))
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

/// Test concurrent file operations
#[tokio::test]
async fn test_concurrent_operations() {
    let (app, _temp_dir) = create_test_server().await;
    
    // Create multiple concurrent upload tasks
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let file_content = format!("Content for file {}", i);
            let file_path = format!("/files/concurrent_test_{}.txt", i);
            
            let request = Request::builder()
                .method(Method::POST)
                .uri(&file_path)
                .body(Body::from(file_content.clone()))
                .unwrap();
            
            let response = app_clone.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            
            // Verify we can download it back
            let download_request = Request::builder()
                .method(Method::GET)
                .uri(&file_path)
                .body(Body::empty())
                .unwrap();
            
            let download_response = app_clone.oneshot(download_request).await.unwrap();
            assert_eq!(download_response.status(), StatusCode::OK);
            
            let body = axum::body::to_bytes(download_response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(body.as_ref(), file_content.as_bytes());
        });
        
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }
}

/// Test file deduplication
#[tokio::test]
async fn test_file_deduplication() {
    let (app, _temp_dir) = create_test_server().await;
    
    let file_content = b"Identical content for deduplication test";
    
    // Upload the same content to different paths
    let paths = ["/files/file1.txt", "/files/file2.txt", "/files/subdir/file3.txt"];
    
    for path in &paths {
        let request = Request::builder()
            .method(Method::POST)
            .uri(*path)
            .body(Body::from(file_content.as_slice()))
            .unwrap();
        
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }
    
    // Verify all files can be downloaded and have the same content
    for path in &paths {
        let request = Request::builder()
            .method(Method::GET)
            .uri(*path)
            .body(Body::empty())
            .unwrap();
        
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), file_content);
    }
}

/// Test hash verification in headers
#[tokio::test]
async fn test_hash_verification() {
    let (app, _temp_dir) = create_test_server().await;
    
    let file_content = b"Content for hash verification";
    
    // Upload file
    let upload_request = Request::builder()
        .method(Method::POST)
        .uri("/files/hash_test.txt")
        .body(Body::from(file_content.as_slice()))
        .unwrap();
    
    let upload_response = app.clone().oneshot(upload_request).await.unwrap();
    assert_eq!(upload_response.status(), StatusCode::CREATED);
    
    // Get upload response hash
    let upload_body = axum::body::to_bytes(upload_response.into_body(), usize::MAX).await.unwrap();
    let upload_metadata: FileMetadataResponse = serde_json::from_slice(&upload_body).unwrap();
    let upload_hash = &upload_metadata.hash;
    
    // Download file and check hash header
    let download_request = Request::builder()
        .method(Method::GET)
        .uri("/files/hash_test.txt")
        .body(Body::empty())
        .unwrap();
    
    let download_response = app.clone().oneshot(download_request).await.unwrap();
    let download_hash = download_response.headers().get("X-File-Hash").unwrap().to_str().unwrap();
    
    assert_eq!(upload_hash, download_hash);
    
    // Get metadata and check hash header
    let metadata_request = Request::builder()
        .method(Method::HEAD)
        .uri("/files/hash_test.txt")
        .body(Body::empty())
        .unwrap();
    
    let metadata_response = app.oneshot(metadata_request).await.unwrap();
    let metadata_hash = metadata_response.headers().get("X-File-Hash").unwrap().to_str().unwrap();
    
    assert_eq!(upload_hash, metadata_hash);
}

/// Test server configuration and binding
#[tokio::test]
async fn test_server_binding() {
    use volebarn_server::server::{Server, ServerConfig};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    
    // Test with a specific configuration
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0, // Use port 0 to get any available port
        max_request_size: 1024 * 1024, // 1MB
        request_timeout: 10,
        storage_root: temp_dir.path().to_path_buf(),
        tls: volebarn_server::tls::TlsConfig::default(),
    };

    // Create server instance
    let server = Server::with_config(config).await.unwrap();
    
    // Verify configuration
    assert_eq!(server.config().host, "127.0.0.1");
    assert_eq!(server.config().port, 0);
    assert_eq!(server.config().max_request_size, 1024 * 1024);
    assert_eq!(server.config().request_timeout, 10);
    
    // Verify initial request count
    assert_eq!(server.request_count(), 0);
}

/// Test error handling for invalid configuration
#[test]
fn test_invalid_server_config() {
    use volebarn_server::server::ServerConfig;

    // Test invalid socket address
    let config = ServerConfig {
        host: "invalid-host-name-that-does-not-exist".to_string(),
        port: 8080,
        max_request_size: 1024,
        request_timeout: 10,
        storage_root: std::path::PathBuf::from("./storage"),
        tls: volebarn_server::tls::TlsConfig::default(),
    };

    // This should fail when trying to create socket address
    let result = config.socket_addr();
    assert!(result.is_err());
}

// ============================================================================
// Directory Operation Tests
// ============================================================================

/// Helper function to create a test server with directory endpoints
async fn create_test_server_with_directories() -> (Router, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_path_buf();
    
    let app_state = AppState::new(&storage_path).await.unwrap();
    
    let app = Router::new()
        .route("/health", get(|| async {
            (StatusCode::OK, Json(json!({"status": "healthy"})))
        }))
        // Directory listing endpoints
        .route("/files", axum::routing::get(volebarn_server::handlers::list_directory))
        .route("/files/", axum::routing::get(volebarn_server::handlers::list_directory))
        // File/directory GET endpoint
        .route("/files/{*path}", axum::routing::get(volebarn_server::handlers::get_file_or_directory))
        // Single file operation endpoints
        .route("/files/{*path}", axum::routing::post(volebarn_server::handlers::upload_file))
        .route("/files/{*path}", axum::routing::put(volebarn_server::handlers::update_file))
        .route("/files/{*path}", axum::routing::delete(volebarn_server::handlers::delete_file))
        .route("/files/{*path}", axum::routing::head(volebarn_server::handlers::get_file_metadata))
        // Directory operation endpoints
        .route("/directories/{*path}", axum::routing::post(volebarn_server::handlers::create_directory))
        .route("/directories/{*path}", axum::routing::delete(volebarn_server::handlers::delete_directory))
        // Search endpoint
        .route("/search", axum::routing::get(volebarn_server::handlers::search_files))
        .with_state(app_state);
    
    (app, temp_dir)
}

/// Test directory creation
#[tokio::test]
async fn test_create_directory() {
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create a directory
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/test_dir")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

/// Test directory creation with nested paths
#[tokio::test]
async fn test_create_nested_directory() {
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create parent directory first
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/parent")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    
    // Create nested directory
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/parent/child")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

/// Test directory listing (root)
#[tokio::test]
async fn test_list_root_directory() {
    use volebarn_server::types::DirectoryListing;
    
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create some files and directories
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/test_dir")
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/test_file.txt")
        .body(Body::from("test content"))
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    // List root directory
    let request = Request::builder()
        .method(Method::GET)
        .uri("/files")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listing: DirectoryListing = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(listing.path, "/");
    assert_eq!(listing.entries.len(), 2);
    
    // Check that we have both the directory and file
    let names: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"test_dir"));
    assert!(names.contains(&"test_file.txt"));
}

/// Test directory listing (specific directory)
#[tokio::test]
async fn test_list_specific_directory() {
    use volebarn_server::types::DirectoryListing;
    
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create a directory
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/test_dir")
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    // Create files in the directory
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/test_dir/file1.txt")
        .body(Body::from("content1"))
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/test_dir/file2.txt")
        .body(Body::from("content2"))
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    // List the directory
    let request = Request::builder()
        .method(Method::GET)
        .uri("/files/test_dir")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listing: DirectoryListing = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(listing.path, "/test_dir");
    assert_eq!(listing.entries.len(), 2);
    
    // Check file names
    let names: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"file1.txt"));
    assert!(names.contains(&"file2.txt"));
}

/// Test recursive directory deletion
#[tokio::test]
async fn test_delete_directory_recursive() {
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create a directory structure
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/test_dir")
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/test_dir/subdir")
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    // Add files
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/test_dir/file1.txt")
        .body(Body::from("content1"))
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/test_dir/subdir/file2.txt")
        .body(Body::from("content2"))
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    // Delete the directory recursively
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/directories/test_dir")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    
    // Verify directory is gone
    let request = Request::builder()
        .method(Method::GET)
        .uri("/files/test_dir")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Test file search functionality
#[tokio::test]
async fn test_search_files() {
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create directory structure with files
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/docs")
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/images")
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap();
    
    // Add various files
    let files = [
        ("/files/readme.txt", "readme content"),
        ("/files/docs/document.txt", "document content"),
        ("/files/docs/report.pdf", "pdf content"),
        ("/files/images/photo.jpg", "image content"),
        ("/files/images/logo.png", "png content"),
    ];
    
    for (path, content) in &files {
        let request = Request::builder()
            .method(Method::POST)
            .uri(*path)
            .body(Body::from(*content))
            .unwrap();
        app.clone().oneshot(request).await.unwrap();
    }
    
    // Search for .txt files
    let request = Request::builder()
        .method(Method::GET)
        .uri("/search?pattern=*.txt")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let results: Vec<FileMetadataResponse> = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(results.len(), 2); // readme.txt and document.txt
    let names: Vec<&str> = results.iter().map(|r| r.name.as_str()).collect();
    assert!(names.contains(&"readme.txt"));
    assert!(names.contains(&"document.txt"));
    
    // Search in specific directory
    let request = Request::builder()
        .method(Method::GET)
        .uri("/search?pattern=*&path=/images")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let results: Vec<FileMetadataResponse> = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(results.len(), 2); // photo.jpg and logo.png
}

/// Test concurrent directory operations
#[tokio::test]
async fn test_concurrent_directory_operations() {
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create multiple concurrent directory creation tasks
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let dir_path = format!("/directories/concurrent_dir_{}", i);
            
            // Create directory
            let request = Request::builder()
                .method(Method::POST)
                .uri(&dir_path)
                .body(Body::empty())
                .unwrap();
            
            let response = app_clone.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            
            // Add a file to the directory
            let file_path = format!("/files/concurrent_dir_{}/test_file.txt", i);
            let file_content = format!("Content for directory {}", i);
            
            let request = Request::builder()
                .method(Method::POST)
                .uri(&file_path)
                .body(Body::from(file_content))
                .unwrap();
            
            let response = app_clone.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            
            // List the directory to verify
            let list_path = format!("/files/concurrent_dir_{}", i);
            let request = Request::builder()
                .method(Method::GET)
                .uri(&list_path)
                .body(Body::empty())
                .unwrap();
            
            let response = app_clone.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        });
        
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify all directories were created by listing root
    let request = Request::builder()
        .method(Method::GET)
        .uri("/files")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let listing: volebarn_server::types::DirectoryListing = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(listing.entries.len(), 10);
}

/// Test directory error cases
#[tokio::test]
async fn test_directory_error_cases() {
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Test creating directory that already exists
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/test_dir")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    
    // Try to create it again
    let request = Request::builder()
        .method(Method::POST)
        .uri("/directories/test_dir")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    
    // Test deleting non-existent directory
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/directories/nonexistent")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    
    // Test listing non-existent directory
    let request = Request::builder()
        .method(Method::GET)
        .uri("/files/nonexistent")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    
    // Test trying to delete a file as a directory
    let request = Request::builder()
        .method(Method::POST)
        .uri("/files/test_file.txt")
        .body(Body::from("content"))
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/directories/test_file.txt")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Test search with various patterns
#[tokio::test]
async fn test_search_patterns() {
    let (app, _temp_dir) = create_test_server_with_directories().await;
    
    // Create files with different extensions and names
    let files = [
        ("/files/test.txt", "content"),
        ("/files/test.pdf", "content"),
        ("/files/document.txt", "content"),
        ("/files/image.jpg", "content"),
        ("/files/data.json", "content"),
        ("/files/readme.md", "content"),
    ];
    
    for (path, content) in &files {
        let request = Request::builder()
            .method(Method::POST)
            .uri(*path)
            .body(Body::from(*content))
            .unwrap();
        app.clone().oneshot(request).await.unwrap();
    }
    
    // Test wildcard pattern
    let request = Request::builder()
        .method(Method::GET)
        .uri("/search?pattern=test*")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let results: Vec<FileMetadataResponse> = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(results.len(), 2); // test.txt and test.pdf
    
    // Test single character wildcard
    let request = Request::builder()
        .method(Method::GET)
        .uri("/search?pattern=test.???")
        .body(Body::empty())
        .unwrap();
    
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let results: Vec<FileMetadataResponse> = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(results.len(), 2); // test.txt and test.pdf
    
    // Test exact match
    let request = Request::builder()
        .method(Method::GET)
        .uri("/search?pattern=readme.md")
        .body(Body::empty())
        .unwrap();
    
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let results: Vec<FileMetadataResponse> = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "readme.md");
}