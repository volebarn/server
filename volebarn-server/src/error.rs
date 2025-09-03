//! Comprehensive error types for Volebarn server
//! 
//! This module provides detailed error types with proper context and error codes
//! for all server operations, following Rust best practices with thiserror.

use crate::types::ErrorCode;
use thiserror::Error;

/// Main server error type with detailed context and error codes
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("File not found: {path}")]
    FileNotFound { path: String },

    #[error("Directory not found: {path}")]
    DirectoryNotFound { path: String },

    #[error("File already exists: {path}")]
    FileAlreadyExists { path: String },

    #[error("Directory already exists: {path}")]
    DirectoryAlreadyExists { path: String },

    #[error("Storage error for {path}: {error}")]
    Storage { path: String, error: String },

    #[error("RocksDB error: {operation} - {error}")]
    Database { operation: String, error: String },

    #[error("Hash verification failed for {path}: expected {expected}, got {actual}")]
    HashVerificationFailed {
        path: String,
        expected: String,
        actual: String,
    },

    #[error("Invalid path: {path} - {reason}")]
    InvalidPath { path: String, reason: String },

    #[error("Invalid request: {field} - {error}")]
    InvalidRequest { field: String, error: String },

    #[error("Permission denied for {path}: {reason}")]
    PermissionDenied { path: String, reason: String },

    #[error("Resource limit exceeded: {resource} - {limit}")]
    ResourceLimit { resource: String, limit: String },

    #[error("Concurrent modification detected for {path}")]
    ConcurrentModification { path: String },

    #[error("TLS configuration error: {error}")]
    TlsConfig { error: String },

    #[error("TLS handshake failed: {error}")]
    TlsHandshake { error: String },

    #[error("Serialization error: {operation} - {error}")]
    Serialization { operation: String, error: String },

    #[error("Deserialization error: {operation} - {error}")]
    Deserialization { operation: String, error: String },

    #[error("I/O error for {path}: {error}")]
    Io { path: String, error: String },

    #[error("Invalid timestamp: {timestamp}")]
    InvalidTimestamp { timestamp: String },

    #[error("Invalid hash format: {hash}")]
    InvalidHash { hash: String },

    #[error("Configuration error: {field} - {error}")]
    Config { field: String, error: String },

    #[error("HTTP error: {status} - {message}")]
    Http { status: u16, message: String },

    #[error("Multipart parsing error: {error}")]
    MultipartParsing { error: String },

    #[error("Request timeout after {duration}s for operation: {operation}")]
    Timeout { duration: u64, operation: String },

    #[error("Rate limit exceeded: {limit} requests per {window}s")]
    RateLimit { limit: u32, window: u32 },

    #[error("Internal server error: {context}")]
    Internal { context: String },
}

impl ServerError {
    /// Get the structured error code for this error
    pub fn error_code(&self) -> ErrorCode {
        match self {
            ServerError::FileNotFound { .. } | ServerError::DirectoryNotFound { .. } => {
                ErrorCode::FileNotFound
            }
            ServerError::PermissionDenied { .. } => ErrorCode::PermissionDenied,
            ServerError::HashVerificationFailed { .. } => ErrorCode::HashMismatch,
            ServerError::Storage { .. }
            | ServerError::Database { .. }
            | ServerError::Io { .. }
            | ServerError::Serialization { .. }
            | ServerError::Deserialization { .. } => ErrorCode::StorageError,
            ServerError::InvalidPath { .. } | ServerError::InvalidRequest { .. } => ErrorCode::InvalidPath,
            ServerError::ResourceLimit { .. } => ErrorCode::ResourceLimit,
            ServerError::ConcurrentModification { .. } => ErrorCode::ConcurrentModification,
            ServerError::TlsConfig { .. } | ServerError::TlsHandshake { .. } => ErrorCode::TlsError,
            ServerError::Timeout { .. } => ErrorCode::Timeout,
            ServerError::RateLimit { .. } => ErrorCode::RateLimited,
            ServerError::Config { .. } => ErrorCode::ConfigError,
            _ => ErrorCode::ServerError,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ServerError::Storage { .. }
                | ServerError::Database { .. }
                | ServerError::Io { .. }
                | ServerError::Timeout { .. }
                | ServerError::Internal { .. }
        )
    }

    /// Get HTTP status code for this error
    pub fn http_status(&self) -> u16 {
        match self {
            ServerError::FileNotFound { .. } | ServerError::DirectoryNotFound { .. } => 404,
            ServerError::FileAlreadyExists { .. } | ServerError::DirectoryAlreadyExists { .. } => {
                409
            }
            ServerError::PermissionDenied { .. } => 403,
            ServerError::InvalidPath { .. }
            | ServerError::InvalidTimestamp { .. }
            | ServerError::InvalidHash { .. }
            | ServerError::MultipartParsing { .. } => 400,
            ServerError::ResourceLimit { .. } => 413,
            ServerError::RateLimit { .. } => 429,
            ServerError::Timeout { .. } => 408,
            _ => 500,
        }
    }
}

// Implement From traits for common error types
impl From<std::io::Error> for ServerError {
    fn from(err: std::io::Error) -> Self {
        ServerError::Io {
            path: "unknown".to_string(),
            error: err.to_string(),
        }
    }
}

impl From<rocksdb::Error> for ServerError {
    fn from(err: rocksdb::Error) -> Self {
        ServerError::Database {
            operation: "unknown".to_string(),
            error: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(err: serde_json::Error) -> Self {
        ServerError::Serialization {
            operation: "json".to_string(),
            error: err.to_string(),
        }
    }
}

impl From<bitcode::Error> for ServerError {
    fn from(err: bitcode::Error) -> Self {
        // bitcode::Error is a simple error type, we'll treat all as deserialization errors
        // since bitcode::encode doesn't return a Result
        ServerError::Deserialization {
            operation: "bitcode_decode".to_string(),
            error: err.to_string(),
        }
    }
}

/// Result type alias for server operations
pub type ServerResult<T> = Result<T, ServerError>;