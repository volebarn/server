//! Integration tests for bulk endpoints: download, delete, move, and copy
//! 
//! Tests the remaining bulk operations with persistent storage and concurrent scenarios

use bytes::Bytes;
use tempfile::TempDir;
use tower::ServiceExt;
use volebarn_server::{
    handlers::AppState,
    server::Server,
    types::{BulkDeleteRequest, BulkDownloadRequest, MoveRequest, CopyRequest, BulkDownloadResponse, BulkDeleteResponse},
};

/// Helper function to create test server with temporary storage
async fn create_test_server() -> (axum::Router, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let app_state = AppState::new(temp_dir.path()).await.expect("Failed to create app state");
    let app = Server::create_app(app_state);
    (app, temp_dir)
}

/// Helper function to upload test files
async fn upload_test_files(app: &axum::Router, files: Vec<(&str, Vec<u8>)>) {
    for (path, content) in files {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(&format!("/files{}", path))
            .header("content-type", "application/octet-stream")
            .body(axum::body::Body::from(Bytes::from(content)))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), 201, "Failed to upload file: {}", path);
    }
}

#[tokio::test]
async fn test_bulk_download_success() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload test files
    let test_files = vec![
        ("/test1.txt", b"Hello World 1".to_vec()),
        ("/test2.txt", b"Hello World 2".to_vec()),
    ];
    upload_test_files(&app, test_files).await;

    // Test bulk download
    let download_request = BulkDownloadRequest {
        paths: vec!["/test1.txt".to_string(), "/test2.txt".to_string()],
    };

    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/bulk/download")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&download_request).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let download_response: BulkDownloadResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(download_response.files.len(), 2);
}

#[tokio::test]
async fn test_bulk_delete_files_success() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload test files
    let test_files = vec![
        ("/test1.txt", b"Hello World 1".to_vec()),
        ("/test2.txt", b"Hello World 2".to_vec()),
    ];
    upload_test_files(&app, test_files).await;

    // Test bulk delete
    let delete_request = BulkDeleteRequest {
        paths: vec!["/test1.txt".to_string(), "/test2.txt".to_string()],
    };

    let request = axum::http::Request::builder()
        .method("DELETE")
        .uri("/bulk/delete")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&delete_request).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let delete_response: BulkDeleteResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(delete_response.success.len(), 2);
    assert_eq!(delete_response.failed.len(), 0);
}

#[tokio::test]
async fn test_move_file_success() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload test file
    upload_test_files(&app, vec![("/source.txt", b"Test content".to_vec())]).await;

    // Test move file
    let move_request = MoveRequest {
        from_path: "/source.txt".to_string(),
        to_path: "/destination.txt".to_string(),
    };

    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/files/move")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&move_request).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Verify source file is gone
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/files/source.txt")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 404);

    // Verify destination file exists with correct content
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/files/destination.txt")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"Test content");
}

#[tokio::test]
async fn test_copy_file_success() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload test file
    upload_test_files(&app, vec![("/source.txt", b"Test content".to_vec())]).await;

    // Test copy file
    let copy_request = CopyRequest {
        from_path: "/source.txt".to_string(),
        to_path: "/copy.txt".to_string(),
    };

    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/files/copy")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&copy_request).unwrap()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 201);

    // Verify source file still exists
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/files/source.txt")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Verify copy exists with correct content
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/files/copy.txt")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"Test content");
}