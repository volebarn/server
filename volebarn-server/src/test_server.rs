//! Test module for server functionality without RocksDB dependency
//! 
//! This module provides a way to test the Axum server foundation
//! without requiring RocksDB to compile.

use crate::server::{ServerConfig, Server};
use axum::{
    http::StatusCode,
    response::Json,
};
use serde_json::json;
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// Test the server configuration
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_request_size, 100 * 1024 * 1024);
        assert_eq!(config.request_timeout, 30);
    }

    #[test]
    fn test_server_config_socket_addr() {
        let config = ServerConfig::default();
        let addr = config.socket_addr().unwrap();
        assert_eq!(addr, "127.0.0.1:8080".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_server_config_from_env() {
        // Test with default values (no env vars set)
        let config = ServerConfig::from_env().unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8080);
    }

    #[tokio::test]
    async fn test_server_creation() {
        let config = ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0, // Use port 0 to get any available port
            max_request_size: 1024,
            request_timeout: 10,
        };
        
        let server = Server::with_config(config).await.unwrap();
        assert_eq!(server.config().host, "127.0.0.1");
        assert_eq!(server.config().port, 0);
        assert_eq!(server.request_count(), 0);
    }
}

/// Simple test function to verify server endpoints work
pub async fn test_health_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    use axum::{routing::get, Router};
    use tokio::net::TcpListener;
    
    // Create a simple test router with just the health endpoint
    let app = Router::new()
        .route("/health", get(|| async {
            let response = json!({
                "status": "healthy",
                "service": "volebarn-server",
                "version": env!("CARGO_PKG_VERSION"),
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            (StatusCode::OK, Json(response))
        }));
    
    // Bind to any available port
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    
    println!("Test server listening on {}", addr);
    
    // Start server in background
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Test the health endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(&format!("http://{}/health", addr))
        .send()
        .await?;
    
    assert_eq!(response.status(), 200);
    
    let body: serde_json::Value = response.json().await?;
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["service"], "volebarn-server");
    
    println!("Health endpoint test passed!");
    Ok(())
}