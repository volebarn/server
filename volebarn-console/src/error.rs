//! Console application error types

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConsoleError {
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("File system error: {0}")]
    FileSystem(#[from] std::io::Error),
    
    #[error("Client error: {0}")]
    Client(#[from] volebarn_client::ClientError),
    
    #[error("File watcher error: {0}")]
    FileWatcher(String),
    
    #[error("Sync error: {0}")]
    Sync(String),
    
    #[error("Index error: {0}")]
    Index(String),
}