//! Main server implementation with Axum and Tokio
//! 
//! Provides the core server functionality with async HTTP handling,
//! configurable binding, error handling middleware, and health checks.

use crate::{Result, ServerError};
use axum::{
    extract::MatchedPath,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;
use std::{net::SocketAddr, sync::atomic::{AtomicU64, Ordering}, time::Instant};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// IP address to bind to
    pub host: String,
    /// Port to bind to
    pub port: u16,
    /// Maximum request size in bytes
    pub max_request_size: usize,
    /// Request timeout in seconds
    pub request_timeout: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            max_request_size: 100 * 1024 * 1024, // 100MB
            request_timeout: 30,
        }
    }
}

impl ServerConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let host = std::env::var("VOLEBARN_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("VOLEBARN_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .map_err(|e| ServerError::Config {
                field: "port".to_string(),
                error: format!("Invalid port number: {}", e),
            })?;
        
        let max_request_size = std::env::var("VOLEBARN_MAX_REQUEST_SIZE")
            .unwrap_or_else(|_| "104857600".to_string()) // 100MB default
            .parse::<usize>()
            .map_err(|e| ServerError::Config {
                field: "max_request_size".to_string(),
                error: format!("Invalid max request size: {}", e),
            })?;
            
        let request_timeout = std::env::var("VOLEBARN_REQUEST_TIMEOUT")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<u64>()
            .map_err(|e| ServerError::Config {
                field: "request_timeout".to_string(),
                error: format!("Invalid request timeout: {}", e),
            })?;

        Ok(Self {
            host,
            port,
            max_request_size,
            request_timeout,
        })
    }
    
    /// Get the socket address for binding
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        format!("{}:{}", self.host, self.port)
            .parse()
            .map_err(|e| ServerError::Config {
                field: "address".to_string(),
                error: format!("Invalid address {}:{}: {}", self.host, self.port, e),
            })
    }
}

/// Main server struct with Axum application
pub struct Server {
    config: ServerConfig,
    app: Router,
    request_counter: AtomicU64,
}

impl Server {
    /// Create a new server instance with default configuration
    pub async fn new() -> Result<Self> {
        let config = ServerConfig::from_env()?;
        Self::with_config(config).await
    }
    
    /// Create a new server instance with custom configuration
    pub async fn with_config(config: ServerConfig) -> Result<Self> {
        info!("Initializing server with config: {:?}", config);
        
        let app = Self::create_app();
        
        Ok(Self {
            config,
            app,
            request_counter: AtomicU64::new(0),
        })
    }
    
    /// Create the Axum application with routing and middleware
    fn create_app() -> Router {
        Router::new()
            // Health check endpoint
            .route("/health", get(health_check))
            .route("/", get(root_handler))
            // Add error handling middleware
            .layer(middleware::from_fn(error_handling_middleware))
            // Add request logging middleware
            .layer(middleware::from_fn(request_logging_middleware))
    }
    
    /// Start the server and listen for connections
    pub async fn run(self) -> Result<()> {
        let addr = self.config.socket_addr()?;
        
        info!("Starting Volebarn server on {}", addr);
        
        // Create TCP listener
        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            ServerError::Config {
                field: "bind".to_string(),
                error: format!("Failed to bind to {}: {}", addr, e),
            }
        })?;
        
        info!("Server listening on {}", addr);
        
        // Start serving requests
        axum::serve(listener, self.app)
            .await
            .map_err(|e| ServerError::Internal {
                context: format!("Server error: {}", e),
            })?;
        
        Ok(())
    }
    
    /// Get server configuration
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }
    
    /// Get current request count
    pub fn request_count(&self) -> u64 {
        self.request_counter.load(Ordering::Relaxed)
    }
}

/// Health check endpoint handler
async fn health_check() -> impl IntoResponse {
    let response = json!({
        "status": "healthy",
        "service": "volebarn-server",
        "version": env!("CARGO_PKG_VERSION"),
        "timestamp": chrono::Utc::now().to_rfc3339()
    });
    
    (StatusCode::OK, Json(response))
}

/// Root endpoint handler
async fn root_handler() -> impl IntoResponse {
    let response = json!({
        "service": "volebarn-server",
        "version": env!("CARGO_PKG_VERSION"),
        "description": "Volebarn file synchronization server",
        "endpoints": {
            "health": "/health",
            "files": "/files/*path",
            "directories": "/directories/*path",
            "bulk": "/bulk/*operation"
        }
    });
    
    (StatusCode::OK, Json(response))
}

/// Error handling middleware that converts ServerError to HTTP responses
async fn error_handling_middleware(
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let response = next.run(request).await;
    
    // If the response is already an error, let it pass through
    if response.status().is_client_error() || response.status().is_server_error() {
        return response;
    }
    
    response
}

/// Request logging middleware
async fn request_logging_middleware(
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = request.method().clone();
    let uri = request.uri().clone();
    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned());
    
    let response = next.run(request).await;
    
    let duration = start.elapsed();
    let status = response.status();
    
    if status.is_client_error() || status.is_server_error() {
        warn!(
            method = %method,
            path = %matched_path,
            status = %status,
            duration_ms = duration.as_millis(),
            "Request completed with error"
        );
    } else {
        info!(
            method = %method,
            path = %matched_path,
            status = %status,
            duration_ms = duration.as_millis(),
            "Request completed"
        );
    }
    
    response
}

/// Convert ServerError to HTTP response
impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status_code = StatusCode::from_u16(self.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        
        let error_response = json!({
            "error": self.to_string(),
            "error_code": self.error_code(),
            "retryable": self.is_retryable(),
            "timestamp": chrono::Utc::now().to_rfc3339()
        });
        
        error!("HTTP error response: {} - {}", status_code, self);
        
        (status_code, Json(error_response)).into_response()
    }
}