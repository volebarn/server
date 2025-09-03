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
    };

    // This should fail when trying to create socket address
    let result = config.socket_addr();
    assert!(result.is_err());
}