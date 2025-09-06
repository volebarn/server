//! Volebarn Client Library
//! 
//! A high-performance async client library for the Volebarn file synchronization system.
//! Features zero-copy operations, retry logic with circuit breaker, and TLS support.

pub mod client;
pub mod error;
pub mod hash;
pub mod config;
pub mod retry;
pub mod types;
pub mod serialization;
pub mod time_utils;
pub mod storage_types;

// Resilience and error handling modules
pub mod offline_queue;
pub mod chunked_upload;
pub mod graceful_degradation;
pub mod metrics;

#[cfg(test)]
pub mod resilience_tests;

pub use client::Client;
pub use error::ClientError;
pub use config::Config;
pub use types::*;

/// Client result type
pub type Result<T> = std::result::Result<T, ClientError>;