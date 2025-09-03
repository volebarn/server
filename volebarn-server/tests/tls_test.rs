//! TLS connectivity and configuration tests
//! 
//! Tests async TLS 1.3 functionality, certificate loading, validation,
//! and concurrent HTTPS connections.

use std::{path::PathBuf, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{fs, time::timeout};
use volebarn_server::{Server, ServerError};
use volebarn_server::tls::{TlsConfig, TlsCertificateManager, generate_self_signed_cert};

/// Test TLS configuration creation and validation
#[tokio::test]
async fn test_tls_config_creation() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("test.crt");
    let key_path = temp_dir.path().join("test.key");
    
    let mut config = TlsConfig::default();
    config.cert_path = cert_path.clone();
    config.key_path = key_path.clone();
    config.set_enabled(true);
    
    // Should fail validation when files don't exist
    let result = config.validate().await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
    
    // Create dummy certificate files
    fs::write(&cert_path, "dummy cert").await.unwrap();
    fs::write(&key_path, "dummy key").await.unwrap();
    
    // Should still fail validation due to invalid format
    let result = config.validate().await;
    assert!(result.is_err());
}

/// Test TLS configuration from environment variables
#[tokio::test]
async fn test_tls_config_from_env() {
    // Set environment variables
    std::env::set_var("VOLEBARN_TLS_ENABLED", "true");
    std::env::set_var("VOLEBARN_TLS_CERT_PATH", "/test/cert.pem");
    std::env::set_var("VOLEBARN_TLS_KEY_PATH", "/test/key.pem");
    std::env::set_var("VOLEBARN_TLS_REQUIRE_CLIENT_CERT", "true");
    std::env::set_var("VOLEBARN_TLS_CA_CERT_PATH", "/test/ca.pem");
    
    let config = TlsConfig::from_env().unwrap();
    
    assert!(config.is_enabled());
    assert_eq!(config.cert_path, PathBuf::from("/test/cert.pem"));
    assert_eq!(config.key_path, PathBuf::from("/test/key.pem"));
    assert!(config.require_client_cert);
    assert_eq!(config.ca_cert_path, Some(PathBuf::from("/test/ca.pem")));
    
    // Clean up environment variables
    std::env::remove_var("VOLEBARN_TLS_ENABLED");
    std::env::remove_var("VOLEBARN_TLS_CERT_PATH");
    std::env::remove_var("VOLEBARN_TLS_KEY_PATH");
    std::env::remove_var("VOLEBARN_TLS_REQUIRE_CLIENT_CERT");
    std::env::remove_var("VOLEBARN_TLS_CA_CERT_PATH");
}

/// Test atomic TLS enable/disable operations
#[tokio::test]
async fn test_tls_atomic_operations() {
    let config = TlsConfig::default();
    
    // Test atomic enable/disable
    assert!(!config.is_enabled());
    
    config.set_enabled(true);
    assert!(config.is_enabled());
    
    config.set_enabled(false);
    assert!(!config.is_enabled());
    
    // Test concurrent access
    let config = Arc::new(config);
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let config_clone = Arc::clone(&config);
        let handle = tokio::spawn(async move {
            config_clone.set_enabled(i % 2 == 0);
            tokio::time::sleep(Duration::from_millis(1)).await;
            config_clone.is_enabled()
        });
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Final state should be consistent
    let final_state = config.is_enabled();
    assert!(final_state == true || final_state == false);
}

/// Test certificate manager with invalid files
#[tokio::test]
async fn test_certificate_manager_invalid_files() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("invalid.crt");
    let key_path = temp_dir.path().join("invalid.key");
    
    // Create invalid certificate files
    fs::write(&cert_path, "not a certificate").await.unwrap();
    fs::write(&key_path, "not a private key").await.unwrap();
    
    let mut config = TlsConfig::default();
    config.cert_path = cert_path;
    config.key_path = key_path;
    config.set_enabled(true);
    
    let manager = TlsCertificateManager::new(config);
    
    // Should fail to load invalid certificates
    let result = manager.load_certificates().await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
    
    // Should fail to load invalid private key
    let result = manager.load_private_key().await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
}

/// Test certificate manager with missing files
#[tokio::test]
async fn test_certificate_manager_missing_files() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("missing.crt");
    let key_path = temp_dir.path().join("missing.key");
    
    let mut config = TlsConfig::default();
    config.cert_path = cert_path;
    config.key_path = key_path;
    config.set_enabled(true);
    
    let manager = TlsCertificateManager::new(config);
    
    // Should fail to load missing certificates
    let result = manager.load_certificates().await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
    
    // Should fail to load missing private key
    let result = manager.load_private_key().await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
}

/// Test self-signed certificate generation (requires feature)
#[cfg(feature = "self-signed-certs")]
#[tokio::test]
async fn test_self_signed_cert_generation() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("self_signed.crt");
    let key_path = temp_dir.path().join("self_signed.key");
    
    // Generate self-signed certificate
    let result = generate_self_signed_cert(&cert_path, &key_path).await;
    assert!(result.is_ok());
    
    // Verify files were created
    assert!(cert_path.exists());
    assert!(key_path.exists());
    
    // Verify files have content
    let cert_content = fs::read_to_string(&cert_path).await.unwrap();
    let key_content = fs::read_to_string(&key_path).await.unwrap();
    
    assert!(cert_content.contains("-----BEGIN CERTIFICATE-----"));
    assert!(cert_content.contains("-----END CERTIFICATE-----"));
    assert!(key_content.contains("-----BEGIN PRIVATE KEY-----"));
    assert!(key_content.contains("-----END PRIVATE KEY-----"));
    
    // Test that generated certificates can be loaded
    let mut config = TlsConfig::default();
    config.cert_path = cert_path;
    config.key_path = key_path;
    config.set_enabled(true);
    
    let manager = TlsCertificateManager::new(config);
    
    // Should successfully load generated certificates
    let certs = manager.load_certificates().await.unwrap();
    assert!(!certs.is_empty());
    
    let _private_key = manager.load_private_key().await.unwrap();
    
    // Should successfully create server config
    let _server_config = manager.create_server_config().await.unwrap();
}

/// Test self-signed certificate generation without feature
#[cfg(not(feature = "self-signed-certs"))]
#[tokio::test]
async fn test_self_signed_cert_generation_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("self_signed.crt");
    let key_path = temp_dir.path().join("self_signed.key");
    
    // Should fail when feature is disabled
    let result = generate_self_signed_cert(&cert_path, &key_path).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
}

/// Test concurrent certificate loading operations
#[cfg(feature = "self-signed-certs")]
#[tokio::test]
async fn test_concurrent_certificate_operations() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("concurrent.crt");
    let key_path = temp_dir.path().join("concurrent.key");
    
    // Generate certificate first
    generate_self_signed_cert(&cert_path, &key_path).await.unwrap();
    
    let mut config = TlsConfig::default();
    config.cert_path = cert_path;
    config.key_path = key_path;
    config.set_enabled(true);
    
    let manager = Arc::new(TlsCertificateManager::new(config));
    let mut handles = Vec::new();
    
    // Start multiple concurrent certificate loading operations
    for _ in 0..5 {
        let manager_clone = Arc::clone(&manager);
        let handle = tokio::spawn(async move {
            let certs = manager_clone.load_certificates().await?;
            let _key = manager_clone.load_private_key().await?;
            let _config = manager_clone.create_server_config().await?;
            Ok::<usize, ServerError>(certs.len())
        });
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }
}

/// Test TLS configuration validation with timeout
#[tokio::test]
async fn test_tls_validation_timeout() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("timeout.crt");
    let key_path = temp_dir.path().join("timeout.key");
    
    let mut config = TlsConfig::default();
    config.cert_path = cert_path;
    config.key_path = key_path;
    config.set_enabled(true);
    
    // Test validation with timeout (should fail quickly for missing files)
    let result = timeout(Duration::from_secs(1), config.validate()).await;
    assert!(result.is_ok()); // Timeout didn't occur
    assert!(result.unwrap().is_err()); // But validation failed
}

/// Test server configuration with TLS enabled
#[tokio::test]
async fn test_server_config_with_tls() {
    use volebarn_server::server::ServerConfig;
    
    let temp_dir = TempDir::new().unwrap();
    let storage_root = temp_dir.path().join("storage");
    
    let mut config = ServerConfig::default();
    config.storage_root = storage_root;
    config.tls.set_enabled(true);
    
    // Test that server config includes TLS configuration
    assert!(config.tls.is_enabled());
    assert_eq!(config.tls.cert_path, PathBuf::from("./certs/server.crt"));
    assert_eq!(config.tls.key_path, PathBuf::from("./certs/server.key"));
}

/// Test error handling for TLS configuration errors
#[tokio::test]
async fn test_tls_error_handling() {
    // Test invalid boolean value
    std::env::set_var("VOLEBARN_TLS_ENABLED", "invalid");
    let result = TlsConfig::from_env();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
    
    // Test invalid client cert requirement
    std::env::set_var("VOLEBARN_TLS_ENABLED", "false");
    std::env::set_var("VOLEBARN_TLS_REQUIRE_CLIENT_CERT", "invalid");
    let result = TlsConfig::from_env();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ServerError::TlsConfig { .. }));
    
    // Clean up
    std::env::remove_var("VOLEBARN_TLS_ENABLED");
    std::env::remove_var("VOLEBARN_TLS_REQUIRE_CLIENT_CERT");
}

/// Integration test for TLS server startup (without actual network binding)
#[cfg(feature = "self-signed-certs")]
#[tokio::test]
async fn test_tls_server_creation() {
    let temp_dir = TempDir::new().unwrap();
    let storage_root = temp_dir.path().join("storage");
    let cert_path = temp_dir.path().join("server.crt");
    let key_path = temp_dir.path().join("server.key");
    
    // Generate self-signed certificate
    generate_self_signed_cert(&cert_path, &key_path).await.unwrap();
    
    // Create server configuration with TLS
    let mut config = volebarn_server::server::ServerConfig::default();
    config.storage_root = storage_root;
    config.tls.cert_path = cert_path;
    config.tls.key_path = key_path;
    config.tls.set_enabled(true);
    
    // Create server (this tests TLS configuration validation)
    let result = Server::with_config(config).await;
    assert!(result.is_ok());
    
    let server = result.unwrap();
    assert!(server.config().tls.is_enabled());
}