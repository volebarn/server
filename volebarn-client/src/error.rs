//! Client error types

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
}