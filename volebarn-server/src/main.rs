//! Volebarn Server Binary
//! 
//! Main entry point for the Volebarn file synchronization server.

use tracing::{info, error};
use volebarn_server::{Server, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Volebarn server...");

    // Create and start server
    let server = Server::new().await?;
    
    if let Err(e) = server.run().await {
        error!("Server error: {}", e);
        return Err(e);
    }

    Ok(())
}