//! Volebarn Server Library
//! 
//! A high-performance file synchronization server built with Tokio and Axum.
//! Features zero-copy operations, lock-free data structures, and persistent storage.

pub mod error;
pub mod storage;
pub mod handlers;
pub mod server;
pub mod types;
pub mod hash;
pub mod tls;

#[cfg(test)]
pub mod test_server;

pub use error::ServerError;
pub use server::Server;
pub use types::*;

/// Server result type
pub type Result<T> = std::result::Result<T, ServerError>;