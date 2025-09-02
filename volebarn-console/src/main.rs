//! Volebarn Console Application
//! 
//! Main entry point for the Volebarn file synchronization console client.

use clap::Parser;
use tracing::{info, error};
use volebarn_console::{App, Config, Result};

#[derive(Parser)]
#[command(name = "volebarn-console")]
#[command(about = "Volebarn file synchronization console client")]
struct Cli {
    /// Server URL
    #[arg(short, long, default_value = "https://localhost:8080")]
    server: String,
    
    /// Local folder to monitor
    #[arg(short, long)]
    folder: Option<String>,
    
    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    info!("Starting Volebarn console client...");

    // Load configuration
    let config = Config::load(cli.config.as_deref(), cli.server, cli.folder)?;
    
    // Create and run app
    let app = App::new(config).await?;
    
    if let Err(e) = app.run().await {
        error!("Console app error: {}", e);
        return Err(e);
    }

    Ok(())
}