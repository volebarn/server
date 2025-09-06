//! Integration tests for real-time file sync functionality

use crate::{App, Config};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

/// Create a test configuration with temporary directories
async fn create_test_config() -> (Config, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let local_folder = temp_dir.path().to_path_buf();
    
    let mut config = Config::default();
    config.local_folder = local_folder;
    config.server_url = "http://localhost:8080".to_string();
    config.verify_tls = false;
    config.debounce_delay_ms = 50; // Faster for testing
    config.max_concurrent_operations = 5;
    
    (config, temp_dir)
}

#[tokio::test]
async fn test_app_initialization() {
    let (config, _temp_dir) = create_test_config().await;
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    
    // Test app creation
    let app = App::new(config, shutdown_flag).await;
    assert!(app.is_ok(), "App should initialize successfully");
    
    let app = app.unwrap();
    assert!(!app.is_shutdown_requested(), "Shutdown should not be requested initially");
}

#[tokio::test]
async fn test_file_event_processing_setup() {
    let (config, _temp_dir) = create_test_config().await;
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    
    let app = App::new(config, shutdown_flag.clone()).await.unwrap();
    
    // Test that the app has the necessary components
    assert!(app.file_watcher.is_some(), "File watcher should be initialized");
    assert!(app.event_receiver.is_some(), "Event receiver should be initialized");
    
    // Test shutdown flag
    shutdown_flag.store(true, Ordering::SeqCst);
    assert!(app.is_shutdown_requested(), "Shutdown should be requested after setting flag");
}

#[tokio::test]
async fn test_real_time_sync_stats() {
    use crate::app::RealTimeSyncStats;
    
    let stats = RealTimeSyncStats::default();
    
    // Test initial state
    assert_eq!(stats.events_processed.load(Ordering::Relaxed), 0);
    assert_eq!(stats.files_uploaded.load(Ordering::Relaxed), 0);
    assert_eq!(stats.sync_errors.load(Ordering::Relaxed), 0);
    
    // Test incrementing stats
    stats.increment_events_processed();
    stats.increment_files_uploaded();
    stats.increment_sync_errors();
    
    assert_eq!(stats.events_processed.load(Ordering::Relaxed), 1);
    assert_eq!(stats.files_uploaded.load(Ordering::Relaxed), 1);
    assert_eq!(stats.sync_errors.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_file_creation_simulation() {
    let (config, temp_dir) = create_test_config().await;
    
    // Create a test file in the monitored directory
    let test_file = temp_dir.path().join("test_file.txt");
    fs::write(&test_file, "test content").await.unwrap();
    
    // Verify file exists
    assert!(test_file.exists(), "Test file should exist");
    
    // Verify it's in the monitored directory
    assert!(test_file.starts_with(&config.local_folder), "Test file should be in monitored directory");
}

#[tokio::test]
async fn test_directory_operations_simulation() {
    let (config, temp_dir) = create_test_config().await;
    
    // Create a test directory
    let test_dir = temp_dir.path().join("test_directory");
    fs::create_dir(&test_dir).await.unwrap();
    
    // Create a file in the directory
    let test_file = test_dir.join("nested_file.txt");
    fs::write(&test_file, "nested content").await.unwrap();
    
    // Verify directory and file exist
    assert!(test_dir.exists(), "Test directory should exist");
    assert!(test_file.exists(), "Nested file should exist");
    
    // Verify they're in the monitored directory
    assert!(test_dir.starts_with(&config.local_folder), "Test directory should be in monitored directory");
    assert!(test_file.starts_with(&config.local_folder), "Nested file should be in monitored directory");
}

#[tokio::test]
async fn test_config_validation() {
    let (mut config, _temp_dir) = create_test_config().await;
    
    // Test valid config
    assert!(config.validate().is_ok(), "Valid config should pass validation");
    
    // Test invalid server URL
    config.server_url = "invalid-url".to_string();
    assert!(config.validate().is_err(), "Invalid server URL should fail validation");
    
    // Reset to valid URL
    config.server_url = "http://localhost:8080".to_string();
    
    // Test invalid poll interval
    config.poll_interval_secs = 0;
    assert!(config.validate().is_err(), "Zero poll interval should fail validation");
}

#[tokio::test]
async fn test_graceful_shutdown() {
    let (config, _temp_dir) = create_test_config().await;
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    
    let app = App::new(config, shutdown_flag.clone()).await.unwrap();
    
    // Simulate shutdown request
    shutdown_flag.store(true, Ordering::SeqCst);
    
    // Test cleanup (this should not panic or hang)
    let cleanup_result = app.cleanup().await;
    assert!(cleanup_result.is_ok(), "Cleanup should succeed");
}

#[tokio::test]
async fn test_concurrent_operations_config() {
    let (config, _temp_dir) = create_test_config().await;
    
    // Test that concurrent operations config is reasonable
    assert!(config.max_concurrent_operations > 0, "Max concurrent operations should be positive");
    assert!(config.max_concurrent_operations <= 100, "Max concurrent operations should be reasonable");
    
    // Test debounce delay
    assert!(config.debounce_delay_ms > 0, "Debounce delay should be positive");
    assert!(config.debounce_delay_ms < 10000, "Debounce delay should be reasonable");
}