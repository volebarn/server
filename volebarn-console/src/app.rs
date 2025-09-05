//! Main console application

use crate::{Config, Result, ConsoleError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, debug};

/// Main console application
pub struct App {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
}

impl App {
    /// Create a new console application instance
    pub async fn new(config: Config, shutdown_flag: Arc<AtomicBool>) -> Result<Self> {
        // Validate that the local folder exists and is accessible
        if !config.local_folder.exists() {
            return Err(ConsoleError::Config(format!(
                "Local folder does not exist: {}", 
                config.local_folder.display()
            )));
        }
        
        if !config.local_folder.is_dir() {
            return Err(ConsoleError::Config(format!(
                "Local folder path is not a directory: {}", 
                config.local_folder.display()
            )));
        }
        
        // Test folder accessibility by trying to read it
        let _read_dir = tokio::fs::read_dir(&config.local_folder).await
            .map_err(|e| ConsoleError::Config(format!(
                "Cannot access local folder '{}': {}", 
                config.local_folder.display(), 
                e
            )))?;
        
        info!("Console application initialized");
        debug!("Monitoring folder: {}", config.local_folder.display());
        debug!("Server URL: {}", config.server_url);
        debug!("Poll interval: {:?}", config.poll_interval());
        
        Ok(App { 
            config,
            shutdown_flag,
        })
    }
    
    /// Run the console application main loop
    pub async fn run(self) -> Result<()> {
        info!("Starting console application main loop");
        
        // Main application loop
        let mut iteration = 0u64;
        
        while !self.shutdown_flag.load(Ordering::SeqCst) {
            iteration += 1;
            
            debug!("Main loop iteration {}", iteration);
            
            // TODO: Implement actual file monitoring and sync logic
            // For now, just demonstrate the async loop with graceful shutdown
            
            // Check if we should shut down
            if self.shutdown_flag.load(Ordering::SeqCst) {
                info!("Shutdown requested, breaking main loop");
                break;
            }
            
            // Sleep for the configured poll interval
            sleep(self.config.poll_interval()).await;
            
            // Periodic status logging
            if iteration % 60 == 0 {
                debug!("Console application running normally (iteration {})", iteration);
            }
        }
        
        info!("Performing graceful shutdown cleanup...");
        
        // Perform cleanup operations
        self.cleanup().await?;
        
        info!("Console application shutdown complete");
        Ok(())
    }
    
    /// Perform cleanup operations during shutdown
    async fn cleanup(&self) -> Result<()> {
        debug!("Starting cleanup operations");
        
        // TODO: Implement actual cleanup logic:
        // - Stop file watchers
        // - Complete pending sync operations
        // - Save state if needed
        // - Close connections
        
        // Simulate cleanup time
        sleep(Duration::from_millis(100)).await;
        
        debug!("Cleanup operations completed");
        Ok(())
    }
    
    /// Get the current configuration
    pub fn config(&self) -> &Config {
        &self.config
    }
    
    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests;