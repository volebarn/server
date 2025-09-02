//! Volebarn Console Library
//! 
//! A console application library for file synchronization with real-time monitoring.
//! Features lock-free file indexing, async file watching, and resilient sync operations.

pub mod app;
pub mod config;
pub mod error;
pub mod file_index;
pub mod file_watcher;
pub mod local_file_manager;
pub mod sync_manager;
pub mod conflict_resolver;

pub use app::App;
pub use config::Config;
pub use error::ConsoleError;

/// Console result type
pub type Result<T> = std::result::Result<T, ConsoleError>;