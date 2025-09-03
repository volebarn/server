//! Integration tests for bulk upload functionality
//! 
//! Tests the POST /bulk/upload endpoint with multipart/form-data support,
//! concurrent operations, and persistent storage validation.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use bytes::Bytes;
use reqwest::multipart::{Form, Part};
use tempfile::TempDir;
use tower::ServiceExt;
use volebarn_server::{
    handlers::AppState,
    server::Server,
    types::BulkUploadResponse,
};

/// Test helper to create a test server with temporary storage
async fn create_test_server() -> (AppState, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let app_state = AppState::new(temp_dir.path())
        .await
        .expect("Failed to create app state");
    (app_state, temp_dir)
}

/// Test helper to create multipart form data with multiple files
fn create_multipart_form() -> Form {
    Form::new()
        .part(
            "files/document1.txt",
            Part::bytes(b"Hello, World! This is document 1.".to_vec())
                .file_name("document1.txt")
                .mime_str("text/plain")
                .unwrap(),
        )
        .part(
            "files/folder/document2.txt",
            Part::bytes(b"This is document 2 in a subfolder.".to_vec())
                .file_name("document2.txt")
                .mime_str("text/plain")
                .unwrap(),
        )
        .part(
            "files/data.json",
            Part::bytes(br#"{"key": "value", "number": 42}"#.to_vec())
                .file_name("data.json")
                .mime_str("application/json")
                .unwrap(),
        )
        .part(
            "files/deep/nested/path/file.txt",
            Part::bytes(b"Deep nested file content".to_vec())
                .file_name("file.txt")
                .mime_str("text/plain")
                .unwrap(),
        )
}

#[tokio::test]
async fn test_bulk_upload_success() {
    let (app_state, _temp_dir) = create_test_server().await;
    let app = Server::create_app(app_state.clone());

    // Create multipart form data
    let _form = create_multipart_form();
    
    // Convert form to bytes (this is a simplified approach for testing)
    // In a real test, we'd use the actual multipart boundary and format
    let test_files = vec![
        ("document1.txt", b"Hello, World! This is document 1.".as_slice()),
        ("folder/document2.txt", b"This is document 2 in a subfolder.".as_slice()),
        ("data.json", br#"{"key": "value", "number": 42}"#.as_slice()),
        ("deep/nested/path/file.txt", b"Deep nested file content".as_slice()),
    ];

    // Create a simple multipart body for testing
    let boundary = "----formdata-test-boundary";
    let mut body = String::new();
    
    for (path, content) in &test_files {
        body.push_str(&format!("--{}\r\n", boundary));
        body.push_str(&format!("Content-Disposition: form-data; name=\"files/{}\"; filename=\"{}\"\r\n", path, path.split('/').last().unwrap()));
        body.push_str("Content-Type: application/octet-stream\r\n\r\n");
        body.push_str(&String::from_utf8_lossy(content));
        body.push_str("\r\n");
    }
    body.push_str(&format!("--{}--\r\n", boundary));

    let request = Request::builder()
        .method("POST")
        .uri("/bulk/upload")
        .header("content-type", format!("multipart/form-data; boundary={}", boundary))
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let bulk_response: BulkUploadResponse = serde_json::from_slice(&body).unwrap();
    
    // Verify all files were uploaded successfully
    assert_eq!(bulk_response.success.len(), 4);
    assert_eq!(bulk_response.failed.len(), 0);
    
    // Verify files exist in storage
    for (path, _) in &test_files {
        let normalized_path = format!("/{}", path);
        let metadata = app_state.metadata_store
            .get_file_metadata(&normalized_path)
            .await
            .unwrap();
        assert!(metadata.is_some(), "File should exist: {}", path);
        
        let file_metadata = metadata.unwrap();
        assert!(!file_metadata.is_directory);
        assert!(file_metadata.size > 0);
    }
    
    // Verify directories were created
    let directories = vec!["/folder", "/deep", "/deep/nested", "/deep/nested/path"];
    for dir_path in &directories {
        let metadata = app_state.metadata_store
            .get_file_metadata(dir_path)
            .await
            .unwrap();
        assert!(metadata.is_some(), "Directory should exist: {}", dir_path);
        assert!(metadata.unwrap().is_directory);
    }
}

#[tokio::test]
async fn test_bulk_upload_empty_request() {
    let (app_state, _temp_dir) = create_test_server().await;
    let app = Server::create_app(app_state);

    // Create empty multipart form
    let boundary = "----formdata-test-boundary";
    let body = format!("--{}--\r\n", boundary);

    let request = Request::builder()
        .method("POST")
        .uri("/bulk/upload")
        .header("content-type", format!("multipart/form-data; boundary={}", boundary))
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    // Should return 400 Bad Request for empty upload
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_bulk_upload_duplicate_files() {
    let (app_state, _temp_dir) = create_test_server().await;
    let app = Server::create_app(app_state.clone());

    // First, upload a file normally
    let first_content = b"Original content";
    let metadata = app_state.file_storage
        .store_file("/test.txt", Bytes::from_static(first_content))
        .await
        .unwrap();
    assert_eq!(metadata.size, first_content.len() as u64);

    // Now try to bulk upload a file with the same path
    let boundary = "----formdata-test-boundary";
    let mut body = String::new();
    body.push_str(&format!("--{}\r\n", boundary));
    body.push_str("Content-Disposition: form-data; name=\"files/test.txt\"; filename=\"test.txt\"\r\n");
    body.push_str("Content-Type: application/octet-stream\r\n\r\n");
    body.push_str("Duplicate content");
    body.push_str("\r\n");
    body.push_str(&format!("--{}--\r\n", boundary));

    let request = Request::builder()
        .method("POST")
        .uri("/bulk/upload")
        .header("content-type", format!("multipart/form-data; boundary={}", boundary))
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let bulk_response: BulkUploadResponse = serde_json::from_slice(&body).unwrap();
    
    // Should have 0 successful uploads and 1 failed (duplicate)
    assert_eq!(bulk_response.success.len(), 0);
    assert_eq!(bulk_response.failed.len(), 1);
    assert_eq!(bulk_response.failed[0].path, "test.txt");
}

#[tokio::test]
async fn test_bulk_upload_mixed_success_failure() {
    let (app_state, _temp_dir) = create_test_server().await;
    let app = Server::create_app(app_state.clone());

    // Pre-create one file to cause a conflict
    let existing_content = b"Existing file";
    app_state.file_storage
        .store_file("/existing.txt", Bytes::from_static(existing_content))
        .await
        .unwrap();

    // Create multipart form with mix of new and existing files
    let boundary = "----formdata-test-boundary";
    let mut body = String::new();
    
    // New file - should succeed
    body.push_str(&format!("--{}\r\n", boundary));
    body.push_str("Content-Disposition: form-data; name=\"files/new.txt\"; filename=\"new.txt\"\r\n");
    body.push_str("Content-Type: application/octet-stream\r\n\r\n");
    body.push_str("New file content");
    body.push_str("\r\n");
    
    // Existing file - should fail
    body.push_str(&format!("--{}\r\n", boundary));
    body.push_str("Content-Disposition: form-data; name=\"files/existing.txt\"; filename=\"existing.txt\"\r\n");
    body.push_str("Content-Type: application/octet-stream\r\n\r\n");
    body.push_str("Conflicting content");
    body.push_str("\r\n");
    
    // Another new file - should succeed
    body.push_str(&format!("--{}\r\n", boundary));
    body.push_str("Content-Disposition: form-data; name=\"files/another.txt\"; filename=\"another.txt\"\r\n");
    body.push_str("Content-Type: application/octet-stream\r\n\r\n");
    body.push_str("Another new file");
    body.push_str("\r\n");
    
    body.push_str(&format!("--{}--\r\n", boundary));

    let request = Request::builder()
        .method("POST")
        .uri("/bulk/upload")
        .header("content-type", format!("multipart/form-data; boundary={}", boundary))
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let bulk_response: BulkUploadResponse = serde_json::from_slice(&body).unwrap();
    
    // Should have 2 successful uploads and 1 failed
    assert_eq!(bulk_response.success.len(), 2);
    assert_eq!(bulk_response.failed.len(), 1);
    
    // Verify successful files
    assert!(bulk_response.success.contains(&"new.txt".to_string()));
    assert!(bulk_response.success.contains(&"another.txt".to_string()));
    
    // Verify failed file
    assert_eq!(bulk_response.failed[0].path, "existing.txt");
}

#[tokio::test]
async fn test_bulk_upload_concurrent_operations() {
    let (app_state, _temp_dir) = create_test_server().await;
    
    // Create multiple concurrent bulk upload requests
    let mut handles = Vec::new();
    
    for i in 0..5 {
        let app_state_clone = app_state.clone();
        let handle = tokio::spawn(async move {
            let app = Server::create_app(app_state_clone);
            
            // Create unique files for each concurrent request
            let boundary = "----formdata-test-boundary";
            let mut body = String::new();
            
            for j in 0..3 {
                let filename = format!("concurrent_{}_{}.txt", i, j);
                let content = format!("Content for request {} file {}", i, j);
                
                body.push_str(&format!("--{}\r\n", boundary));
                body.push_str(&format!("Content-Disposition: form-data; name=\"files/{}\"; filename=\"{}\"\r\n", filename, filename));
                body.push_str("Content-Type: application/octet-stream\r\n\r\n");
                body.push_str(&content);
                body.push_str("\r\n");
            }
            body.push_str(&format!("--{}--\r\n", boundary));

            let request = Request::builder()
                .method("POST")
                .uri("/bulk/upload")
                .header("content-type", format!("multipart/form-data; boundary={}", boundary))
                .body(Body::from(body))
                .unwrap();

            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let bulk_response: BulkUploadResponse = serde_json::from_slice(&body).unwrap();
            
            (i, bulk_response)
        });
        handles.push(handle);
    }
    
    // Wait for all concurrent requests to complete
    let mut total_successful = 0;
    let mut total_failed = 0;
    
    for handle in handles {
        let (request_id, bulk_response) = handle.await.unwrap();
        println!("Request {} completed: {} successful, {} failed", 
                request_id, bulk_response.success.len(), bulk_response.failed.len());
        
        total_successful += bulk_response.success.len();
        total_failed += bulk_response.failed.len();
    }
    
    // Should have uploaded 15 files total (5 requests * 3 files each)
    assert_eq!(total_successful, 15);
    assert_eq!(total_failed, 0);
    
    // Verify all files exist in storage
    for i in 0..5 {
        for j in 0..3 {
            let filename = format!("/concurrent_{}_{}.txt", i, j);
            let metadata = app_state.metadata_store
                .get_file_metadata(&filename)
                .await
                .unwrap();
            assert!(metadata.is_some(), "File should exist: {}", filename);
        }
    }
}

#[tokio::test]
async fn test_bulk_upload_large_files() {
    let (app_state, _temp_dir) = create_test_server().await;
    let app = Server::create_app(app_state.clone());

    // Create a large file (1MB)
    let large_content = vec![b'A'; 1024 * 1024];
    
    let boundary = "----formdata-test-boundary";
    let mut body = String::new();
    
    body.push_str(&format!("--{}\r\n", boundary));
    body.push_str("Content-Disposition: form-data; name=\"files/large.txt\"; filename=\"large.txt\"\r\n");
    body.push_str("Content-Type: application/octet-stream\r\n\r\n");
    body.push_str(&String::from_utf8_lossy(&large_content));
    body.push_str("\r\n");
    body.push_str(&format!("--{}--\r\n", boundary));

    let request = Request::builder()
        .method("POST")
        .uri("/bulk/upload")
        .header("content-type", format!("multipart/form-data; boundary={}", boundary))
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let bulk_response: BulkUploadResponse = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(bulk_response.success.len(), 1);
    assert_eq!(bulk_response.failed.len(), 0);
    
    // Verify large file was stored correctly
    let metadata = app_state.metadata_store
        .get_file_metadata("/large.txt")
        .await
        .unwrap()
        .unwrap();
    
    assert_eq!(metadata.size, 1024 * 1024);
    assert!(!metadata.is_directory);
    
    // Verify file content can be retrieved
    let retrieved_content = app_state.file_storage
        .retrieve_file("/large.txt")
        .await
        .unwrap();
    
    assert_eq!(retrieved_content.len(), 1024 * 1024);
}

#[tokio::test]
async fn test_bulk_upload_directory_structure_preservation() {
    let (app_state, _temp_dir) = create_test_server().await;
    let app = Server::create_app(app_state.clone());

    // Create files with complex directory structure
    let test_files = vec![
        ("docs/readme.txt", "README content"),
        ("docs/api/endpoints.md", "API documentation"),
        ("src/main.rs", "fn main() {}"),
        ("src/lib/utils.rs", "pub fn helper() {}"),
        ("tests/integration/test1.rs", "integration test 1"),
        ("tests/unit/test2.rs", "unit test 2"),
    ];

    let boundary = "----formdata-test-boundary";
    let mut body = String::new();
    
    for (path, content) in &test_files {
        body.push_str(&format!("--{}\r\n", boundary));
        body.push_str(&format!("Content-Disposition: form-data; name=\"files/{}\"; filename=\"{}\"\r\n", path, path.split('/').last().unwrap()));
        body.push_str("Content-Type: application/octet-stream\r\n\r\n");
        body.push_str(content);
        body.push_str("\r\n");
    }
    body.push_str(&format!("--{}--\r\n", boundary));

    let request = Request::builder()
        .method("POST")
        .uri("/bulk/upload")
        .header("content-type", format!("multipart/form-data; boundary={}", boundary))
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let bulk_response: BulkUploadResponse = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(bulk_response.success.len(), 6);
    assert_eq!(bulk_response.failed.len(), 0);
    
    // Verify all files exist
    for (path, _) in &test_files {
        let normalized_path = format!("/{}", path);
        let metadata = app_state.metadata_store
            .get_file_metadata(&normalized_path)
            .await
            .unwrap();
        assert!(metadata.is_some(), "File should exist: {}", path);
    }
    
    // Verify directory structure was created
    let expected_dirs = vec![
        "/docs",
        "/docs/api", 
        "/src",
        "/src/lib",
        "/tests",
        "/tests/integration",
        "/tests/unit",
    ];
    
    for dir_path in &expected_dirs {
        let metadata = app_state.metadata_store
            .get_file_metadata(dir_path)
            .await
            .unwrap();
        assert!(metadata.is_some(), "Directory should exist: {}", dir_path);
        assert!(metadata.unwrap().is_directory, "Should be a directory: {}", dir_path);
    }
    
    // Verify directory listings work correctly
    let docs_listing = app_state.metadata_store
        .list_directory("/docs")
        .await
        .unwrap();
    
    assert_eq!(docs_listing.len(), 2); // readme.txt and api/ directory
    
    let api_listing = app_state.metadata_store
        .list_directory("/docs/api")
        .await
        .unwrap();
    
    assert_eq!(api_listing.len(), 1); // endpoints.md
}