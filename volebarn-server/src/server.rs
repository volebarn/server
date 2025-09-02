//! Main server implementation

use crate::{Result, ServerError};

pub struct Server;

impl Server {
    pub async fn new() -> Result<Self> {
        Ok(Server)
    }
    
    pub async fn run(self) -> Result<()> {
        Err(ServerError::Config { 
            field: "server".to_string(), 
            error: "Not implemented".to_string() 
        })
    }
}