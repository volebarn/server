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
    
    // Display startup information
    let config = server.config();
    info!("Volebarn Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Listening on {}:{}", config.host, config.port);
    
    if config.tls.is_enabled() {
        info!("TLS 1.3 enabled with rustls");
        info!("Certificate: {}", config.tls.cert_path.display());
        if config.tls.require_client_cert {
            info!("Client certificate verification required");
        }
    } else {
        info!("TLS disabled - using HTTP");
    }
    
    info!("Storage location: {}", config.storage_root.display());
    info!("Available endpoints:");
    info!("  GET  /health - Health check");
    info!("  GET  /files - List files");
    info!("  POST /files/{{*path}} - Upload file");
    info!("  GET  /files/{{*path}} - Download file");
    info!("  PUT  /files/{{*path}} - Update file");
    info!("  DELETE /files/{{*path}} - Delete file");
    info!("  HEAD /files/{{*path}} - Get file metadata");
    info!("  POST /directories/{{*path}} - Create directory");
    info!("  DELETE /directories/{{*path}} - Delete directory");
    info!("  POST /bulk/upload - Bulk upload");
    info!("  POST /bulk/download - Bulk download");
    info!("  DELETE /bulk/delete - Bulk delete");
    info!("  GET  /bulk/manifest - Get file manifest");
    info!("  POST /bulk/sync - Sync with manifest");
    info!("  POST /files/move - Move file/directory");
    info!("  POST /files/copy - Copy file/directory");
    info!("  GET  /search - Search files");
    
    if let Err(e) = server.run().await {
        error!("Server error: {}", e);
        return Err(e);
    }

    Ok(())
}