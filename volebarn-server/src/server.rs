//! Main server implementation

use crate::{Result, ServerError};

pub struct Server;

impl Server {
    pub async fn new() -> Result<Self> {
        Ok(Server)
    }
    
    pub async fn run(self) -> Result<()> {
        Err(ServerError::Config("Not implemented".to_string()))
    }
}