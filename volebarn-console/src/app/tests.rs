//! App tests

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_app_creation_valid_config() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let app = App::new(config, shutdown_flag).await;
    
    assert!(app.is_ok());
}

#[tokio::test]
async fn test_app_creation_nonexistent_folder() {
    let mut config = Config::default();
    config.local_folder = PathBuf::from("/nonexistent/folder");
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let result = App::new(config, shutdown_flag).await;
    
    assert!(result.is_err());
    if let Err(ConsoleError::Config(msg)) = result {
        assert!(msg.contains("does not exist"));
    } else {
        panic!("Expected Config error");
    }
}

#[tokio::test]
async fn test_app_creation_file_instead_of_folder() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("not_a_folder.txt");
    tokio::fs::write(&file_path, "test content").await.unwrap();
    
    let mut config = Config::default();
    config.local_folder = file_path;
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let result = App::new(config, shutdown_flag).await;
    
    assert!(result.is_err());
    if let Err(ConsoleError::Config(msg)) = result {
        assert!(msg.contains("not a directory"));
    } else {
        panic!("Expected Config error");
    }
}

#[tokio::test]
async fn test_app_graceful_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    config.poll_interval_secs = 1; // Short interval for faster test
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_flag_clone = shutdown_flag.clone();
    
    let app = App::new(config, shutdown_flag).await.unwrap();
    
    // Start the app in a background task
    let app_handle = tokio::spawn(async move {
        app.run().await
    });
    
    // Let it run for a short time
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Signal shutdown
    shutdown_flag_clone.store(true, Ordering::SeqCst);
    
    // Wait for graceful shutdown with timeout
    let result = timeout(Duration::from_secs(5), app_handle).await;
    
    assert!(result.is_ok());
    assert!(result.unwrap().is_ok());
}

#[tokio::test]
async fn test_app_config_access() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    config.server_url = "https://test.example.com".to_string();
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let app = App::new(config, shutdown_flag).await.unwrap();
    
    assert_eq!(app.config().server_url, "https://test.example.com");
    assert_eq!(app.config().local_folder, temp_dir.path());
}

#[tokio::test]
async fn test_app_shutdown_flag_check() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let app = App::new(config, shutdown_flag.clone()).await.unwrap();
    
    assert!(!app.is_shutdown_requested());
    
    shutdown_flag.store(true, Ordering::SeqCst);
    assert!(app.is_shutdown_requested());
}

#[tokio::test]
async fn test_app_immediate_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    
    let shutdown_flag = Arc::new(AtomicBool::new(true)); // Already set to shutdown
    let app = App::new(config, shutdown_flag).await.unwrap();
    
    // Should exit immediately
    let result = timeout(Duration::from_secs(1), app.run()).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_ok());
}

#[tokio::test]
async fn test_app_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let app = App::new(config, shutdown_flag).await.unwrap();
    
    // Test cleanup method directly
    let result = app.cleanup().await;
    assert!(result.is_ok());
}