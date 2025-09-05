//! Configuration tests

use super::*;
use std::env;
use tempfile::TempDir;
use tokio::fs;

#[tokio::test]
async fn test_default_config() {
    let config = Config::default();
    
    assert_eq!(config.server_url, "https://localhost:8080");
    assert_eq!(config.poll_interval_secs, 1);
    assert_eq!(config.max_retry_attempts, 5);
    assert_eq!(config.initial_retry_delay_ms, 1000);
    assert_eq!(config.max_retry_delay_ms, 30000);
    assert_eq!(config.connection_timeout_secs, 30);
    assert_eq!(config.request_timeout_secs, 60);
    assert!(config.verify_tls);
    assert_eq!(config.log_level, "info");
}

#[tokio::test]
async fn test_config_validation_valid() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    
    assert!(config.validate().is_ok());
}

#[tokio::test]
async fn test_config_validation_invalid_server_url() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    config.server_url = "invalid-url".to_string();
    
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn test_config_validation_empty_server_url() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    config.server_url = "".to_string();
    
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn test_config_validation_nonexistent_folder() {
    let mut config = Config::default();
    config.local_folder = PathBuf::from("/nonexistent/folder");
    
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn test_config_validation_invalid_timing() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    
    // Test zero poll interval
    config.poll_interval_secs = 0;
    assert!(config.validate().is_err());
    
    // Reset and test zero retry attempts
    config.poll_interval_secs = 1;
    config.max_retry_attempts = 0;
    assert!(config.validate().is_err());
    
    // Reset and test invalid retry delays
    config.max_retry_attempts = 5;
    config.max_retry_delay_ms = 500;
    config.initial_retry_delay_ms = 1000;
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn test_config_validation_invalid_log_level() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.local_folder = temp_dir.path().to_path_buf();
    config.log_level = "invalid".to_string();
    
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn test_config_load_from_json_file() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test_config.json");
    
    let config_content = r#"{
        "server_url": "https://example.com:9090",
        "local_folder": "/tmp/test",
        "poll_interval_secs": 5,
        "max_retry_attempts": 3,
        "initial_retry_delay_ms": 500,
        "max_retry_delay_ms": 10000,
        "connection_timeout_secs": 15,
        "request_timeout_secs": 30,
        "verify_tls": false,
        "log_level": "debug"
    }"#;
    
    fs::write(&config_path, config_content).await.unwrap();
    
    let config = Config::load_from_file(&config_path.to_string_lossy()).await.unwrap();
    
    assert_eq!(config.server_url, "https://example.com:9090");
    assert_eq!(config.local_folder, PathBuf::from("/tmp/test"));
    assert_eq!(config.poll_interval_secs, 5);
    assert_eq!(config.max_retry_attempts, 3);
    assert_eq!(config.initial_retry_delay_ms, 500);
    assert_eq!(config.max_retry_delay_ms, 10000);
    assert_eq!(config.connection_timeout_secs, 15);
    assert_eq!(config.request_timeout_secs, 30);
    assert!(!config.verify_tls);
    assert_eq!(config.log_level, "debug");
}

#[tokio::test]
async fn test_config_load_from_toml_file() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test_config.toml");
    
    let config_content = r#"
server_url = "https://example.com:9090"
local_folder = "/tmp/test"
poll_interval_secs = 5
max_retry_attempts = 3
initial_retry_delay_ms = 500
max_retry_delay_ms = 10000
connection_timeout_secs = 15
request_timeout_secs = 30
verify_tls = false
log_level = "debug"
"#;
    
    fs::write(&config_path, config_content).await.unwrap();
    
    let config = Config::load_from_file(&config_path.to_string_lossy()).await.unwrap();
    
    assert_eq!(config.server_url, "https://example.com:9090");
    assert_eq!(config.local_folder, PathBuf::from("/tmp/test"));
    assert_eq!(config.poll_interval_secs, 5);
    assert_eq!(config.max_retry_attempts, 3);
}

#[tokio::test]
async fn test_config_load_from_env() {
    let temp_dir = TempDir::new().unwrap();
    
    // Set environment variables
    env::set_var("VOLEBARN_SERVER_URL", "https://env.example.com");
    env::set_var("VOLEBARN_LOCAL_FOLDER", temp_dir.path().to_string_lossy().as_ref());
    env::set_var("VOLEBARN_POLL_INTERVAL_SECS", "10");
    env::set_var("VOLEBARN_MAX_RETRY_ATTEMPTS", "7");
    env::set_var("VOLEBARN_VERIFY_TLS", "false");
    env::set_var("VOLEBARN_LOG_LEVEL", "trace");
    
    let config = Config::load(None, None, None).await.unwrap();
    
    assert_eq!(config.server_url, "https://env.example.com");
    assert_eq!(config.local_folder, temp_dir.path());
    assert_eq!(config.poll_interval_secs, 10);
    assert_eq!(config.max_retry_attempts, 7);
    assert!(!config.verify_tls);
    assert_eq!(config.log_level, "trace");
    
    // Clean up environment variables
    env::remove_var("VOLEBARN_SERVER_URL");
    env::remove_var("VOLEBARN_LOCAL_FOLDER");
    env::remove_var("VOLEBARN_POLL_INTERVAL_SECS");
    env::remove_var("VOLEBARN_MAX_RETRY_ATTEMPTS");
    env::remove_var("VOLEBARN_VERIFY_TLS");
    env::remove_var("VOLEBARN_LOG_LEVEL");
}

#[tokio::test]
async fn test_config_cli_override() {
    let temp_dir = TempDir::new().unwrap();
    
    let config = Config::load(
        None,
        Some("https://cli.example.com".to_string()),
        Some(temp_dir.path().to_string_lossy().to_string()),
    ).await.unwrap();
    
    assert_eq!(config.server_url, "https://cli.example.com");
    assert_eq!(config.local_folder, temp_dir.path());
}

#[tokio::test]
async fn test_config_precedence() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("precedence_test.json");
    
    // Create config file
    let file_config = r#"{
        "server_url": "https://file.example.com",
        "local_folder": "/tmp/file",
        "poll_interval_secs": 2,
        "max_retry_attempts": 5,
        "initial_retry_delay_ms": 1000,
        "max_retry_delay_ms": 30000,
        "connection_timeout_secs": 30,
        "request_timeout_secs": 60,
        "verify_tls": true,
        "log_level": "warn"
    }"#;
    
    fs::write(&config_path, file_config).await.unwrap();
    
    // Set environment variable (should override file)
    env::set_var("VOLEBARN_SERVER_URL", "https://env.example.com");
    env::set_var("VOLEBARN_POLL_INTERVAL_SECS", "3");
    
    // CLI arguments should override both file and env
    let config = Config::load(
        Some(&config_path.to_string_lossy()),
        Some("https://cli.example.com".to_string()),
        Some(temp_dir.path().to_string_lossy().to_string()),
    ).await.unwrap();
    
    // CLI should win
    assert_eq!(config.server_url, "https://cli.example.com");
    assert_eq!(config.local_folder, temp_dir.path());
    
    // Env should override file
    assert_eq!(config.poll_interval_secs, 3);
    
    // File value should be used when not overridden
    assert_eq!(config.log_level, "warn");
    
    // Clean up
    env::remove_var("VOLEBARN_SERVER_URL");
    env::remove_var("VOLEBARN_POLL_INTERVAL_SECS");
}

#[tokio::test]
async fn test_config_save_and_load_json() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("save_test.json");
    
    let mut original_config = Config::default();
    original_config.server_url = "https://save.example.com".to_string();
    original_config.poll_interval_secs = 15;
    original_config.verify_tls = false;
    
    // Save config
    original_config.save_to_file(&config_path.to_string_lossy()).await.unwrap();
    
    // Load config
    let loaded_config = Config::load_from_file(&config_path.to_string_lossy()).await.unwrap();
    
    assert_eq!(loaded_config.server_url, original_config.server_url);
    assert_eq!(loaded_config.poll_interval_secs, original_config.poll_interval_secs);
    assert_eq!(loaded_config.verify_tls, original_config.verify_tls);
}

#[tokio::test]
async fn test_config_save_and_load_toml() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("save_test.toml");
    
    let mut original_config = Config::default();
    original_config.server_url = "https://save.example.com".to_string();
    original_config.poll_interval_secs = 15;
    original_config.verify_tls = false;
    
    // Save config
    original_config.save_to_file(&config_path.to_string_lossy()).await.unwrap();
    
    // Load config
    let loaded_config = Config::load_from_file(&config_path.to_string_lossy()).await.unwrap();
    
    assert_eq!(loaded_config.server_url, original_config.server_url);
    assert_eq!(loaded_config.poll_interval_secs, original_config.poll_interval_secs);
    assert_eq!(loaded_config.verify_tls, original_config.verify_tls);
}

#[tokio::test]
async fn test_config_duration_helpers() {
    let config = Config::default();
    
    assert_eq!(config.poll_interval(), Duration::from_secs(1));
    assert_eq!(config.initial_retry_delay(), Duration::from_millis(1000));
    assert_eq!(config.max_retry_delay(), Duration::from_millis(30000));
    assert_eq!(config.connection_timeout(), Duration::from_secs(30));
    assert_eq!(config.request_timeout(), Duration::from_secs(60));
}

#[tokio::test]
async fn test_config_load_invalid_json() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("invalid.json");
    
    fs::write(&config_path, "{ invalid json }").await.unwrap();
    
    let result = Config::load_from_file(&config_path.to_string_lossy()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_config_load_nonexistent_file() {
    let result = Config::load_from_file("/nonexistent/config.json").await;
    assert!(result.is_err());
}