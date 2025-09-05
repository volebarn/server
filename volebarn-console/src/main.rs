//! Volebarn Console Application
//! 
//! Main entry point for the Volebarn file synchronization console client.

use clap::Parser;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal;
use tracing::{info, error, warn, debug};
use tracing_subscriber::EnvFilter;
use volebarn_console::{App, Config, Result};

#[derive(Parser)]
#[command(name = "volebarn-console")]
#[command(about = "Volebarn file synchronization console client")]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    /// Server URL
    #[arg(short, long)]
    server: Option<String>,
    
    /// Local folder to monitor
    #[arg(short, long)]
    folder: Option<String>,
    
    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,
    
    /// Log level (trace, debug, info, warn, error)
    #[arg(long)]
    log_level: Option<String>,
    
    /// Generate a sample configuration file
    #[arg(long)]
    generate_config: Option<String>,
    
    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,
    
    /// Disable TLS certificate verification
    #[arg(long)]
    no_verify_tls: bool,
    
    /// Connection timeout in seconds
    #[arg(long)]
    connection_timeout: Option<u64>,
    
    /// Request timeout in seconds
    #[arg(long)]
    request_timeout: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Handle config generation
    if let Some(config_path) = cli.generate_config {
        return generate_sample_config(&config_path).await;
    }
    
    // Initialize logging with appropriate level
    let env_log_level = std::env::var("VOLEBARN_LOG_LEVEL").ok();
    let log_level = cli.log_level
        .as_deref()
        .or_else(|| env_log_level.as_deref())
        .unwrap_or("info");
    
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level));
    
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    info!("Starting Volebarn Console Client v{}", env!("CARGO_PKG_VERSION"));
    debug!("Command line arguments parsed successfully");

    // Load configuration asynchronously
    let mut config = Config::load(
        cli.config.as_deref(),
        cli.server.clone(),
        cli.folder.clone(),
    ).await?;
    
    // Apply CLI overrides
    apply_cli_overrides(&mut config, &cli);
    
    info!("Configuration loaded successfully");
    debug!("Server URL: {}", config.server_url);
    debug!("Local folder: {}", config.local_folder.display());
    debug!("Poll interval: {}s", config.poll_interval_secs);
    
    // Handle config validation
    if cli.validate_config {
        info!("Configuration is valid");
        return Ok(());
    }
    
    // Set up graceful shutdown handling
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_flag_clone = shutdown_flag.clone();
    
    // Spawn shutdown signal handler
    tokio::spawn(async move {
        if let Err(e) = wait_for_shutdown_signal().await {
            error!("Error waiting for shutdown signal: {}", e);
        }
        
        info!("Shutdown signal received, initiating graceful shutdown...");
        shutdown_flag_clone.store(true, Ordering::SeqCst);
    });
    
    // Create and run app with shutdown handling
    let app = App::new(config, shutdown_flag.clone()).await?;
    
    info!("Console client initialized, starting file monitoring...");
    
    match app.run().await {
        Ok(()) => {
            info!("Console client shut down gracefully");
            Ok(())
        }
        Err(e) => {
            error!("Console client error: {}", e);
            Err(e)
        }
    }
}

/// Apply command line argument overrides to configuration
fn apply_cli_overrides(config: &mut Config, cli: &Cli) {
    if let Some(ref log_level) = cli.log_level {
        config.log_level = log_level.clone();
    }
    
    if cli.no_verify_tls {
        config.verify_tls = false;
        warn!("TLS certificate verification disabled");
    }
    
    if let Some(timeout) = cli.connection_timeout {
        config.connection_timeout_secs = timeout;
    }
    
    if let Some(timeout) = cli.request_timeout {
        config.request_timeout_secs = timeout;
    }
}

/// Wait for shutdown signals (SIGINT, SIGTERM)
async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use signal::unix::{signal, SignalKind};
        
        let mut sigint = signal(SignalKind::interrupt())
            .map_err(|e| volebarn_console::ConsoleError::Config(format!("Failed to register SIGINT handler: {}", e)))?;
        let mut sigterm = signal(SignalKind::terminate())
            .map_err(|e| volebarn_console::ConsoleError::Config(format!("Failed to register SIGTERM handler: {}", e)))?;
        
        tokio::select! {
            _ = sigint.recv() => {
                debug!("Received SIGINT");
            }
            _ = sigterm.recv() => {
                debug!("Received SIGTERM");
            }
        }
    }
    
    #[cfg(windows)]
    {
        let mut ctrl_c = signal::ctrl_c()
            .map_err(|e| volebarn_console::ConsoleError::Config(format!("Failed to register Ctrl+C handler: {}", e)))?;
        
        ctrl_c.await
            .map_err(|e| volebarn_console::ConsoleError::Config(format!("Error waiting for Ctrl+C: {}", e)))?;
        
        debug!("Received Ctrl+C");
    }
    
    Ok(())
}

/// Generate a sample configuration file
async fn generate_sample_config(path: &str) -> Result<()> {
    let config = Config::default();
    
    config.save_to_file(path).await?;
    
    info!("Sample configuration file generated: {}", path);
    println!("Sample configuration file generated: {}", path);
    
    Ok(())
}