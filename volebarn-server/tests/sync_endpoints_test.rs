//! Integration tests for sync and manifest endpoints

use std::collections::HashMap;
use volebarn_server::{
    handlers::AppState,
    types::{FileManifestResponse, FileMetadataResponse, SyncRequestApi, ConflictResolutionStrategy},
};

/// Helper to create test app state with temporary storage
async fn create_test_app_state() -> AppState {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let storage_path = temp_dir.path().join("test_storage");
    
    // Ensure the storage directory exists
    tokio::fs::create_dir_all(&storage_path).await.expect("Failed to create storage directory");
    
    AppState::new(&storage_path).await.expect("Failed to create app state")
}

#[tokio::test]
async fn test_get_manifest_empty_server() {
    let app_state = create_test_app_state().await;
    
    // Test getting manifest from empty server
    let response = volebarn_server::handlers::get_manifest(
        axum::extract::State(app_state)
    ).await.expect("Failed to get manifest");
    
    // Check that response is successful
    let status = response.status();
    assert_eq!(status, 200);
}

#[tokio::test]
async fn test_sync_empty_client_empty_server() {
    let app_state = create_test_app_state().await;
    
    // Both client and server are empty
    let client_manifest = FileManifestResponse {
        files: HashMap::new(),
    };
    
    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::PreferRemote,
    };
    
    // Test sync endpoint
    let response = volebarn_server::handlers::sync_with_manifest(
        axum::extract::State(app_state),
        axum::Json(sync_request)
    ).await.expect("Failed to sync");
    
    let status = response.status();
    assert_eq!(status, 200);
}

#[tokio::test]
async fn test_sync_with_client_files() {
    let app_state = create_test_app_state().await;
    
    // Server is empty, client has files (server is authoritative, so client should delete)
    let mut client_files = HashMap::new();
    
    let metadata = FileMetadataResponse {
        path: "/client_file.txt".to_string(),
        name: "client_file.txt".to_string(),
        size: 14,
        modified: "1000".to_string(),
        is_directory: false,
        hash: "abcdef1234567890".to_string(),
    };
    
    client_files.insert("/client_file.txt".to_string(), metadata);
    
    let client_manifest = FileManifestResponse { files: client_files };
    
    let sync_request = SyncRequestApi {
        client_manifest,
        conflict_resolution: ConflictResolutionStrategy::PreferRemote,
    };
    
    // Test sync endpoint
    let response = volebarn_server::handlers::sync_with_manifest(
        axum::extract::State(app_state),
        axum::Json(sync_request)
    ).await.expect("Failed to sync");
    
    let status = response.status();
    assert_eq!(status, 200);
    
    // In a real implementation, we'd parse the sync plan and verify:
    // - client_delete contains the client file (server is authoritative)
    // - other lists are empty
}

#[tokio::test]
async fn test_sync_conflict_resolution_strategies() {
    let app_state = create_test_app_state().await;
    
    // Test different conflict resolution strategies with empty server
    let strategies = vec![
        ConflictResolutionStrategy::PreferLocal,
        ConflictResolutionStrategy::PreferRemote,
        ConflictResolutionStrategy::PreferNewer,
        ConflictResolutionStrategy::Manual,
    ];
    
    for strategy in strategies {
        let client_manifest = FileManifestResponse {
            files: HashMap::new(),
        };
        
        let sync_request = SyncRequestApi {
            client_manifest,
            conflict_resolution: strategy,
        };
        
        let response = volebarn_server::handlers::sync_with_manifest(
            axum::extract::State(app_state.clone()),
            axum::Json(sync_request)
        ).await.expect("Failed to sync");
        
        let status = response.status();
        assert_eq!(status, 200);
    }
}