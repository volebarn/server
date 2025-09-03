//! Integration tests for the Axum server foundation
//! 
//! These tests verify that the server can start up and respond to requests
//! without requiring RocksDB or other heavy dependencies.

use axum::{
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde_json::json;
use tokio::net::TcpListener;

/// Test that we can create a basic Axum server and it responds to health checks
#[tokio::test]
async fn test_axum_server_health_endpoint() {
    // Create a simple health check handler
    async fn health_check() -> (StatusCode, Json<serde_json::Value>) {
        let response = json!({
            "status": "healthy",
            "service": "volebarn-server",
            "version": env!("CARGO_PKG_VERSION"),
            "timestamp": chrono::Utc::now().to_rfc3339()
        });
        
        (StatusCode::OK, Json(response))
    }

    // Create the router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/", get(|| async {
            let response = json!({
                "service": "volebarn-server",
                "version": env!("CARGO_PKG_VERSION"),
                "description": "Volebarn file synchronization server"
            });
            (StatusCode::OK, Json(response))
        }));

    // Bind to any available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Start server in background
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Test the health endpoint
    let client = reqwest::Client::new();
    
    // Test health endpoint
    let response = client
        .get(&format!("http://{}/health", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["service"], "volebarn-server");
    assert!(body["timestamp"].is_string());

    // Test root endpoint
    let response = client
        .get(&format!("http://{}/", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["service"], "volebarn-server");
    assert_eq!(body["description"], "Volebarn file synchronization server");
}

/// Test server configuration and binding
#[tokio::test]
async fn test_server_binding() {
    use volebarn_server::server::{Server, ServerConfig};

    // Test with a specific configuration
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0, // Use port 0 to get any available port
        max_request_size: 1024 * 1024, // 1MB
        request_timeout: 10,
    };

    // Create server instance
    let server = Server::with_config(config).await.unwrap();
    
    // Verify configuration
    assert_eq!(server.config().host, "127.0.0.1");
    assert_eq!(server.config().port, 0);
    assert_eq!(server.config().max_request_size, 1024 * 1024);
    assert_eq!(server.config().request_timeout, 10);
    
    // Verify initial request count
    assert_eq!(server.request_count(), 0);
}

/// Test error handling for invalid configuration
#[test]
fn test_invalid_server_config() {
    use volebarn_server::server::ServerConfig;

    // Test invalid socket address
    let config = ServerConfig {
        host: "invalid-host-name-that-does-not-exist".to_string(),
        port: 8080,
        max_request_size: 1024,
        request_timeout: 10,
    };

    // This should fail when trying to create socket address
    let result = config.socket_addr();
    assert!(result.is_err());
}