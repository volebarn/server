//! Integration tests for sync and manifest endpoints
//! 
//! Tests the async GET /bulk/manifest and POST /bulk/sync endpoints
//! with server as source of truth and persistent storage scenarios.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};

use std::collections::HashMap;
use tempfile::TempDir;
use tower::ServiceExt;
use volebarn_server::{
    handlers::AppState,
    server::Server,
    types::{FileManifestResponse, FileMetadataResponse, SyncRequestApi, SyncPlan, ConflictResolutionStrategy},
};

/// Helper to create test server with temporary storage
async fn create_test_server() -> (Router, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let app_state = AppState::new(temp_dir.path()).await.expect("Failed to create app state");
    let app = Server::create_app(app_state);
    (app, temp_dir)
}

/// Helper to upload a test file
async fn upload_test_file(app: &Router, path: &str, content: &str) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri(&format!("/files{}", path))
        .header("content-type", "application/octet-stream")
        .body(Body::from(content.to_string()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

/// Helper to create a directory
async fn create_test_directory(app: &Router, path: &str) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri(&format!("/directories{}", path))
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

#[tokio::test]
async fn test_get_manifest_empty_server() {
    let (app, _temp_dir) = create_test_server().await;

    // Get manifest from empty server
    let request = Request::builder()
        .method("GET")
        .uri("/bulk/manifest")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let manifest: FileManifestResponse = serde_json::from_slice(&body).unwrap();
    
    assert!(manifest.files.is_empty());
}

#[tokio::test]
async fn test_get_manifest_with_files() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload some test files
    assert_eq!(upload_test_file(&app, "/test1.txt", "content1").await, StatusCode::CREATED);
    assert_eq!(upload_test_file(&app, "/test2.txt", "content2").await, StatusCode::CREATED);
    assert_eq!(create_test_directory(&app, "/subdir").await, StatusCode::CREATED);
    assert_eq!(upload_test_file(&app, "/subdir/test3.txt", "content3").await, StatusCode::CREATED);

    // Get manifest
    let request = Request::builder()
        .method("GET")
        .uri("/bulk/manifest")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let manifest: FileManifestResponse = serde_json::from_slice(&body).unwrap();
    
    // Should have 4 entries: 3 files + 1 directory
    assert_eq!(manifest.files.len(), 4);
    assert!(manifest.files.contains_key("/test1.txt"));
    assert!(manifest.files.contains_key("/test2.txt"));
    assert!(manifest.files.contains_key("/subdir"));
    assert!(manifest.files.contains_key("/subdir/test3.txt"));
    
    // Check directory is marked correctly
    let subdir_metadata = &manifest.files["/subdir"];
    assert!(subdir_metadata.is_directory);
    
    // Check files have correct properties
    let file1_metadata = &manifest.files["/test1.txt"];
    assert!(!file1_metadata.is_directory);
    assert_eq!(file1_metadata.size, 8); // "content1".len()
    assert_ne!(file1_metadata.hash, "0000000000000000"); // Should have hash
}

#[tokio::test]
async fn test_sync_empty_client_with_server_files() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload files to server
    assert_eq!(upload_test_file(&app, "/server1.txt", "server content 1").await, StatusCode::CREATED);
    assert_eq!(upload_test_file(&app, "/server2.txt", "server content 2").await, StatusCode::CREATED);
    assert_eq!(create_test_directory(&app, "/serverdir").await, StatusCode::CREATED);

    // Create empty client manifest
    let client_manifest = FileManifestResponse {
        files: HashMap::new(),
    };

    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::PreferRemote,
    };

    // Send sync request
    let request = Request::builder()
        .method("POST")
        .uri("/bulk/sync")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&sync_request).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sync_plan: SyncPlan = serde_json::from_slice(&body).unwrap();
    
    // Client should download all server files (but not directories)
    assert_eq!(sync_plan.client_download.len(), 2); // 2 files only
    assert!(sync_plan.client_download.contains(&"/server1.txt".to_string()));
    assert!(sync_plan.client_download.contains(&"/server2.txt".to_string()));
    
    // Should create the directory
    assert_eq!(sync_plan.client_create_dirs.len(), 1);
    assert!(sync_plan.client_create_dirs.contains(&"/serverdir".to_string()));
    assert!(sync_plan.client_create_dirs.contains(&"/serverdir".to_string()));
    
    // No uploads or deletes needed
    assert!(sync_plan.client_upload.is_empty());
    assert!(sync_plan.client_delete.is_empty());
    assert!(sync_plan.conflicts.is_empty());
}

#[tokio::test]
async fn test_sync_client_files_not_on_server() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload one file to server
    assert_eq!(upload_test_file(&app, "/shared.txt", "shared content").await, StatusCode::CREATED);

    // Create client manifest with files not on server
    let mut client_files = HashMap::new();
    client_files.insert("/shared.txt".to_string(), FileMetadataResponse {
        path: "/shared.txt".to_string(),
        name: "shared.txt".to_string(),
        size: 13,
        modified: "1756863453".to_string(), // Unix timestamp as string
        is_directory: false,
        hash: "0000000000003039".to_string(), // Different hash to simulate different content
    });
    client_files.insert("/client_only.txt".to_string(), FileMetadataResponse {
        path: "/client_only.txt".to_string(),
        name: "client_only.txt".to_string(),
        size: 20,
        modified: "1756863453".to_string(), // Unix timestamp as string
        is_directory: false,
        hash: "000000000001092a".to_string(),
    });

    let client_manifest = FileManifestResponse {
        files: client_files,
    };

    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::PreferRemote,
    };

    // Send sync request
    let request = Request::builder()
        .method("POST")
        .uri("/bulk/sync")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&sync_request).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sync_plan: SyncPlan = serde_json::from_slice(&body).unwrap();
    
    // Client should delete the file that doesn't exist on server
    assert_eq!(sync_plan.client_delete.len(), 1);
    assert!(sync_plan.client_delete.contains(&"/client_only.txt".to_string()));
    
    // Client should download the shared file (server version wins due to hash mismatch)
    assert_eq!(sync_plan.client_download.len(), 1);
    assert!(sync_plan.client_download.contains(&"/shared.txt".to_string()));
    
    // No uploads or directory creation needed
    assert!(sync_plan.client_upload.is_empty());
    assert!(sync_plan.client_create_dirs.is_empty());
    assert!(sync_plan.conflicts.is_empty());
}

#[tokio::test]
async fn test_sync_conflict_resolution_prefer_local() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload file to server
    assert_eq!(upload_test_file(&app, "/conflict.txt", "server version").await, StatusCode::CREATED);

    // Create client manifest with different content for same file
    let mut client_files = HashMap::new();
    client_files.insert("/conflict.txt".to_string(), FileMetadataResponse {
        path: "/conflict.txt".to_string(),
        name: "conflict.txt".to_string(),
        size: 14, // "client version".len()
        modified: "1756863453".to_string(), // Unix timestamp as string
        is_directory: false,
        hash: "000000000001869f".to_string(), // Different hash to simulate different content
    });

    let client_manifest = FileManifestResponse {
        files: client_files,
    };

    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::PreferLocal,
    };

    // Send sync request
    let request = Request::builder()
        .method("POST")
        .uri("/bulk/sync")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&sync_request).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sync_plan: SyncPlan = serde_json::from_slice(&body).unwrap();
    
    // Client should upload its version (prefer local)
    assert_eq!(sync_plan.client_upload.len(), 1);
    assert!(sync_plan.client_upload.contains(&"/conflict.txt".to_string()));
    
    // No downloads, deletes, or conflicts
    assert!(sync_plan.client_download.is_empty());
    assert!(sync_plan.client_delete.is_empty());
    assert!(sync_plan.client_create_dirs.is_empty());
    assert!(sync_plan.conflicts.is_empty());
}

#[tokio::test]
async fn test_sync_conflict_resolution_manual() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload file to server
    assert_eq!(upload_test_file(&app, "/manual_conflict.txt", "server version").await, StatusCode::CREATED);

    // Create client manifest with different content for same file
    let mut client_files = HashMap::new();
    client_files.insert("/manual_conflict.txt".to_string(), FileMetadataResponse {
        path: "/manual_conflict.txt".to_string(),
        name: "manual_conflict.txt".to_string(),
        size: 14,
        modified: "1756863453".to_string(), // Unix timestamp as string
        is_directory: false,
        hash: "0000000000015b38".to_string(), // Different hash
    });

    let client_manifest = FileManifestResponse {
        files: client_files,
    };

    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::Manual,
    };

    // Send sync request
    let request = Request::builder()
        .method("POST")
        .uri("/bulk/sync")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&sync_request).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sync_plan: SyncPlan = serde_json::from_slice(&body).unwrap();
    
    // Should have a manual conflict
    assert_eq!(sync_plan.conflicts.len(), 1);
    assert_eq!(sync_plan.conflicts[0].path, "/manual_conflict.txt");
    
    // No automatic operations
    assert!(sync_plan.client_upload.is_empty());
    assert!(sync_plan.client_download.is_empty());
    assert!(sync_plan.client_delete.is_empty());
    assert!(sync_plan.client_create_dirs.is_empty());
}

#[tokio::test]
async fn test_sync_identical_files() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload file to server
    assert_eq!(upload_test_file(&app, "/identical.txt", "same content").await, StatusCode::CREATED);

    // Get the server manifest to get the exact hash
    let request = Request::builder()
        .method("GET")
        .uri("/bulk/manifest")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let server_manifest: FileManifestResponse = serde_json::from_slice(&body).unwrap();
    
    let server_file = &server_manifest.files["/identical.txt"];

    // Create client manifest with identical file (same hash)
    let mut client_files = HashMap::new();
    client_files.insert("/identical.txt".to_string(), FileMetadataResponse {
        path: "/identical.txt".to_string(),
        name: "identical.txt".to_string(),
        size: server_file.size,
        modified: server_file.modified.clone(),
        is_directory: false,
        hash: server_file.hash.clone(), // Same hash = identical content
    });

    let client_manifest = FileManifestResponse {
        files: client_files,
    };

    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::PreferRemote,
    };

    // Send sync request
    let request = Request::builder()
        .method("POST")
        .uri("/bulk/sync")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&sync_request).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sync_plan: SyncPlan = serde_json::from_slice(&body).unwrap();
    
    // No operations needed - files are identical
    assert!(sync_plan.client_upload.is_empty());
    assert!(sync_plan.client_download.is_empty());
    assert!(sync_plan.client_delete.is_empty());
    assert!(sync_plan.client_create_dirs.is_empty());
    assert!(sync_plan.conflicts.is_empty());
}

#[tokio::test]
async fn test_sync_with_persistent_storage() {
    let (app, _temp_dir) = create_test_server().await;

    // Upload multiple files and directories to test persistent storage
    assert_eq!(upload_test_file(&app, "/persistent1.txt", "persistent content 1").await, StatusCode::CREATED);
    assert_eq!(create_test_directory(&app, "/persistent_dir").await, StatusCode::CREATED);
    assert_eq!(upload_test_file(&app, "/persistent_dir/nested.txt", "nested content").await, StatusCode::CREATED);
    assert_eq!(create_test_directory(&app, "/persistent_dir/subdir").await, StatusCode::CREATED);
    assert_eq!(upload_test_file(&app, "/persistent_dir/subdir/deep.txt", "deep content").await, StatusCode::CREATED);

    // Get manifest to verify persistent storage
    let request = Request::builder()
        .method("GET")
        .uri("/bulk/manifest")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let manifest: FileManifestResponse = serde_json::from_slice(&body).unwrap();
    
    // Should have all files and directories
    assert_eq!(manifest.files.len(), 5); // 3 files + 2 directories
    assert!(manifest.files.contains_key("/persistent1.txt"));
    assert!(manifest.files.contains_key("/persistent_dir"));
    assert!(manifest.files.contains_key("/persistent_dir/nested.txt"));
    assert!(manifest.files.contains_key("/persistent_dir/subdir"));
    assert!(manifest.files.contains_key("/persistent_dir/subdir/deep.txt"));

    // Test sync with empty client - should download everything
    let client_manifest = FileManifestResponse {
        files: HashMap::new(),
    };

    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::PreferRemote,
    };

    let request = Request::builder()
        .method("POST")
        .uri("/bulk/sync")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&sync_request).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let sync_plan: SyncPlan = serde_json::from_slice(&body).unwrap();
    
    // Client should download all files (but not directories)
    assert_eq!(sync_plan.client_download.len(), 3); // 3 files only
    assert_eq!(sync_plan.client_create_dirs.len(), 2); // 2 directories
    
    // Verify directories are in create_dirs
    assert!(sync_plan.client_create_dirs.contains(&"/persistent_dir".to_string()));
    assert!(sync_plan.client_create_dirs.contains(&"/persistent_dir/subdir".to_string()));
}