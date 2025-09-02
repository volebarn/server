//! Comprehensive error types for Volebarn client
//! 
//! This module provides detailed error types with proper context and error codes
//! for all client operations, following Rust best practices with thiserror.

use crate::types::ErrorCode;
use thiserror::Error;

/// Main client error type with detailed context and error codes
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("File not found: {path}")]
    FileNotFound { path: String },

    #[error("Server error: {status} - {message}")]
    Server { status: u16, message: String },

    #[error("Hash mismatch for {path}: expected {expected}, got {actual}")]
    HashMismatch {
        path: String,
        expected: String,
        actual: String,
    },

    #[error("TLS error: {error}")]
    Tls { error: String },

    #[error("TLS handshake failed: {error}")]
    TlsHandshake { error: String },

    #[error("Timeout after {duration}s for operation: {operation}")]
    Timeout { duration: u64, operation: String },

    #[error("Rate limited: retry after {retry_after}s")]
    RateLimited { retry_after: u64 },

    #[error("Partial failure: {successful} succeeded, {failed} failed")]
    PartialFailure {
        successful: usize,
        failed: usize,
        errors: Vec<String>,
    },

    #[error("Configuration error: {field} - {error}")]
    Config { field: String, error: String },

    #[error("Serialization error: {operation} - {error}")]
    Serialization { operation: String, error: String },

    #[error("Deserialization error: {operation} - {error}")]
    Deserialization { operation: String, error: String },

    #[error("I/O error for {path}: {error}")]
    Io { path: String, error: String },

    #[error("Invalid response format: {error}")]
    InvalidResponse { error: String },

    #[error("Authentication failed: {error}")]
    Authentication { error: String },

    #[error("Permission denied for {path}: {reason}")]
    PermissionDenied { path: String, reason: String },

    #[error("Resource limit exceeded: {resource} - {limit}")]
    ResourceLimit { resource: String, limit: String },

    #[error("Connection failed: {error}")]
    Connection { error: String },

    #[error("Circuit breaker open: too many failures")]
    CircuitBreakerOpen,

    #[error("Retry limit exceeded: {attempts} attempts failed")]
    RetryLimitExceeded { attempts: u32 },

    #[error("Invalid path: {path} - {reason}")]
    InvalidPath { path: String, reason: String },

    #[error("Sync conflict for {path}: requires manual resolution")]
    SyncConflict { path: String },

    #[error("Local file error for {path}: {error}")]
    LocalFile { path: String, error: String },
}

impl ClientError {
    /// Get the structured error code for this error
    pub fn error_code(&self) -> ErrorCode {
        match self {
            ClientError::FileNotFound { .. } => ErrorCode::FileNotFound,
            ClientError::PermissionDenied { .. } => ErrorCode::PermissionDenied,
            ClientError::HashMismatch { .. } => ErrorCode::HashMismatch,
            ClientError::Network(_) | ClientError::Connection { .. } => ErrorCode::NetworkError,
            ClientError::Server { .. } => ErrorCode::ServerError,
            ClientError::InvalidPath { .. } => ErrorCode::InvalidPath,
            ClientError::ResourceLimit { .. } => ErrorCode::ResourceLimit,
            ClientError::Tls { .. } | ClientError::TlsHandshake { .. } => ErrorCode::TlsError,
            ClientError::Timeout { .. } => ErrorCode::Timeout,
            ClientError::RateLimited { .. } => ErrorCode::RateLimited,
            ClientError::Config { .. } => ErrorCode::ConfigError,
            _ => ErrorCode::NetworkError,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::Network(_) => true,
            ClientError::Server { status, .. } => *status >= 500,
            ClientError::Timeout { .. } => true,
            ClientError::Connection { .. } => true,
            ClientError::HashMismatch { .. } => true, // Can retry with re-download
            _ => false,
        }
    }

    /// Check if this error should trigger circuit breaker
    pub fn should_trigger_circuit_breaker(&self) -> bool {
        match self {
            ClientError::Network(_) => true,
            ClientError::Server { status, .. } => *status >= 500,
            ClientError::Timeout { .. } => true,
            ClientError::Connection { .. } => true,
            _ => false,
        }
    }

    /// Get retry delay in seconds for exponential backoff
    pub fn retry_delay(&self, attempt: u32) -> u64 {
        let base_delay = match self {
            ClientError::RateLimited { retry_after } => *retry_after,
            ClientError::Server { status, .. } if *status == 429 => 60, // Rate limited
            ClientError::Server { status, .. } if *status >= 500 => 1,  // Server error
            ClientError::Network(_) | ClientError::Connection { .. } => 1,
            ClientError::Timeout { .. } => 2,
            _ => 1,
        };

        // Exponential backoff with jitter: base * 2^attempt + random(0, base)
        let exponential = base_delay * (2_u64.pow(attempt.min(5))); // Cap at 2^5 = 32
        let jitter = fastrand::u64(0..=base_delay);
        (exponential + jitter).min(300) // Cap at 5 minutes
    }
}

// Implement From traits for common error types
impl From<std::io::Error> for ClientError {
    fn from(err: std::io::Error) -> Self {
        ClientError::Io {
            path: "unknown".to_string(),
            error: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(err: serde_json::Error) -> Self {
        ClientError::Serialization {
            operation: "json".to_string(),
            error: err.to_string(),
        }
    }
}

/// Result type alias for client operations
pub type ClientResult<T> = Result<T, ClientError>;