//! Integration tests for sync functionality
//! 
//! These tests demonstrate the sync methods working together with mock data.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;
use volebarn_client::{Client, ClientError};
use volebarn_client::types::*;

/// Mock file system for testing sync operations
#[derive(Debug, Clone)]
struct MockFileSystem {
    files: Arc<Mutex<HashMap<String, Bytes>>>,
    directories: Arc<Mutex<HashMap<String, bool>>>,
    operation_count: Arc<AtomicUsize>,
}

impl MockFileSystem {
    fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
            directories: Arc::new(Mutex::new(HashMap::new())),
            operation_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn upload_file(&self, path: String, content: Bytes) -> Result<(), std::io::Error> {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let mut files = self.files.lock().await;
        files.insert(path, content);
        Ok(())
    }

    async fn download_file(&self, path: String, content: Bytes) -> Result<(), std::io::Error> {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let mut files = self.files.lock().await;
        files.insert(path, content);
        Ok(())
    }

    async fn delete_file(&self, path: String) -> Result<(), std::io::Error> {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let mut files = self.files.lock().await;
        files.remove(&path);
        Ok(())
    }

    async fn create_directory(&self, path: String) -> Result<(), std::io::Error> {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let mut directories = self.directories.lock().await;
        directories.insert(path, true);
        Ok(())
    }

    async fn get_operation_count(&self) -> usize {
        self.operation_count.load(Ordering::Relaxed)
    }

    async fn file_exists(&self, path: &str) -> bool {
        let files = self.files.lock().await;
        files.contains_key(path)
    }

    async fn directory_exists(&self, path: &str) -> bool {
        let directories = self.directories.lock().await;
        directories.contains_key(path)
    }
}

#[tokio::test]
async fn test_sync_functionality_with_mock_fs() {
    // Create client (will fail to connect to server, but we can test the sync logic structure)
    let client = Client::with_defaults("https://localhost:8080".to_string()).await.unwrap();
    
    // Create mock file system
    let mock_fs = MockFileSystem::new();
    
    // Create a local manifest with some test files
    let mut local_files = HashMap::new();
    local_files.insert(
        "test1.txt".to_string(),
        FileMetadata::new(
            "test1.txt".to_string(),
            100,
            SystemTime::now(),
            12345,
        ),
    );
    local_files.insert(
        "test2.txt".to_string(),
        FileMetadata::new(
            "test2.txt".to_string(),
            200,
            SystemTime::now(),
            67890,
        ),
    );
    
    let local_manifest = FileManifest { files: local_files };
    
    // Test download_missing_files with empty list
    let result = client.download_missing_files(&[]).await.unwrap();
    assert!(result.is_empty());
    
    // Test delete_extra_local_files with empty list
    let mock_fs_clone = mock_fs.clone();
    let delete_fn = move |path: String| {
        let fs = mock_fs_clone.clone();
        async move { fs.delete_file(path).await }
    };
    
    let result = client.delete_extra_local_files(&[], delete_fn).await.unwrap();
    assert!(result.is_empty());
    
    // Verify no operations were performed on empty lists
    assert_eq!(mock_fs.get_operation_count().await, 0);
    
    // Test sync plan creation (will fail due to no server, but tests the method signature)
    let conflict_resolution = ConflictResolutionStrategy::PreferNewer;
    let result = client.sync_plan(local_manifest.clone(), conflict_resolution.clone()).await;
    
    // Should fail with network error since no server is running
    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Network(_) | ClientError::Connection { .. } | ClientError::Timeout { .. } | ClientError::CircuitBreakerOpen => {
            // Expected - no server running
        }
        other => panic!("Unexpected error type: {:?}", other),
    }
    
    // Test full_sync (will fail due to no server, but tests the method signature and structure)
    let mock_fs_upload = mock_fs.clone();
    let upload_fn = move |path: String, content: Bytes| {
        let fs = mock_fs_upload.clone();
        async move { fs.upload_file(path, content).await }
    };
    
    let mock_fs_download = mock_fs.clone();
    let download_fn = move |path: String, content: Bytes| {
        let fs = mock_fs_download.clone();
        async move { fs.download_file(path, content).await }
    };
    
    let mock_fs_delete = mock_fs.clone();
    let delete_fn = move |path: String| {
        let fs = mock_fs_delete.clone();
        async move { fs.delete_file(path).await }
    };
    
    let mock_fs_create_dir = mock_fs.clone();
    let create_dir_fn = move |path: String| {
        let fs = mock_fs_create_dir.clone();
        async move { fs.create_directory(path).await }
    };
    
    let result = client.full_sync(
        local_manifest,
        conflict_resolution,
        upload_fn,
        download_fn,
        delete_fn,
        create_dir_fn,
    ).await;
    
    // Should fail with network error since no server is running
    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Network(_) | ClientError::Connection { .. } | ClientError::Timeout { .. } | ClientError::CircuitBreakerOpen => {
            // Expected - no server running
        }
        other => panic!("Unexpected error type: {:?}", other),
    }
}

#[tokio::test]
async fn test_sync_data_structures() {
    // Test SyncPlan creation and manipulation
    let mut sync_plan = SyncPlan::new();
    assert!(sync_plan.is_empty());
    assert_eq!(sync_plan.total_operations(), 0);
    
    // Add operations
    sync_plan.client_upload.push("upload1.txt".to_string());
    sync_plan.client_upload.push("upload2.txt".to_string());
    sync_plan.client_download.push("download1.txt".to_string());
    sync_plan.client_delete.push("delete1.txt".to_string());
    sync_plan.client_create_dirs.push("new_dir".to_string());
    
    // Add a conflict
    sync_plan.conflicts.push(SyncConflict {
        path: "conflict.txt".to_string(),
        local_modified: SystemTime::now(),
        remote_modified: SystemTime::now(),
        resolution: ConflictResolution::UseNewer,
    });
    
    assert!(!sync_plan.is_empty());
    assert_eq!(sync_plan.total_operations(), 6); // 2 uploads + 1 download + 1 delete + 1 create_dir + 1 conflict
    
    // Test SyncResult creation and manipulation
    let mut sync_result = SyncResult::new();
    assert!(sync_result.is_success());
    assert_eq!(sync_result.success_count(), 0);
    assert_eq!(sync_result.error_count(), 0);
    
    // Add successful operations
    sync_result.uploaded.push("uploaded1.txt".to_string());
    sync_result.downloaded.push("downloaded1.txt".to_string());
    sync_result.deleted_local.push("deleted1.txt".to_string());
    sync_result.created_dirs.push("created_dir".to_string());
    sync_result.conflicts_resolved.push("resolved_conflict.txt".to_string());
    
    assert!(sync_result.is_success());
    assert_eq!(sync_result.success_count(), 5);
    assert_eq!(sync_result.error_count(), 0);
    
    // Add errors
    sync_result.errors.push(("error1.txt".to_string(), "Failed to upload".to_string()));
    sync_result.errors.push(("error2.txt".to_string(), "Failed to download".to_string()));
    
    assert!(!sync_result.is_success());
    assert_eq!(sync_result.success_count(), 5);
    assert_eq!(sync_result.error_count(), 2);
}

#[tokio::test]
async fn test_conflict_resolution_strategies() {
    // Test different conflict resolution strategies
    let strategies = vec![
        ConflictResolutionStrategy::PreferLocal,
        ConflictResolutionStrategy::PreferRemote,
        ConflictResolutionStrategy::PreferNewer,
        ConflictResolutionStrategy::Manual,
    ];
    
    for strategy in strategies {
        let sync_request = SyncRequest {
            client_manifest: FileManifest { files: HashMap::new() },
            conflict_resolution: strategy.clone(),
        };
        
        // Verify the request can be created and the strategy is preserved
        assert_eq!(sync_request.conflict_resolution, strategy);
    }
    
    // Test conflict resolution enum values
    let resolutions = vec![
        ConflictResolution::UseLocal,
        ConflictResolution::UseRemote,
        ConflictResolution::UseNewer,
        ConflictResolution::Manual,
    ];
    
    for resolution in resolutions {
        let conflict = SyncConflict {
            path: "test.txt".to_string(),
            local_modified: SystemTime::now(),
            remote_modified: SystemTime::now(),
            resolution: resolution.clone(),
        };
        
        // Verify the conflict can be created and the resolution is preserved
        assert_eq!(conflict.resolution, resolution);
    }
}

#[tokio::test]
async fn test_file_manifest_operations() {
    let now = SystemTime::now();
    
    // Create file metadata
    let file1 = FileMetadata::new("file1.txt".to_string(), 100, now, 12345);
    let file2 = FileMetadata::new("dir/file2.txt".to_string(), 200, now, 67890);
    let dir1 = FileMetadata::new_directory("dir".to_string(), now);
    
    // Create manifest
    let mut manifest = FileManifest { files: HashMap::new() };
    manifest.files.insert(file1.path.clone(), file1.clone());
    manifest.files.insert(file2.path.clone(), file2.clone());
    manifest.files.insert(dir1.path.clone(), dir1.clone());
    
    // Test manifest operations
    assert_eq!(manifest.files.len(), 3);
    assert!(manifest.files.contains_key("file1.txt"));
    assert!(manifest.files.contains_key("dir/file2.txt"));
    assert!(manifest.files.contains_key("dir"));
    
    // Test file metadata properties
    assert_eq!(file1.name, "file1.txt");
    assert_eq!(file2.name, "file2.txt");
    assert_eq!(dir1.name, "dir");
    
    assert!(!file1.is_directory);
    assert!(!file2.is_directory);
    assert!(dir1.is_directory);
    
    // Test parent path functionality
    assert_eq!(file1.parent_path(), Some("".to_string()));
    assert_eq!(file2.parent_path(), Some("dir".to_string()));
    assert_eq!(dir1.parent_path(), Some("".to_string()));
    
    // Test directory membership
    assert!(file2.is_in_directory("dir"));
    assert!(!file1.is_in_directory("dir"));
    assert!(!dir1.is_in_directory("dir"));
}