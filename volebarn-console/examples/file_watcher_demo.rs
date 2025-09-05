//! File watcher demonstration
//! 
//! This example shows how to use the FileWatcher to monitor a directory
//! for file system changes in real-time.

use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
use volebarn_console::file_watcher::{FileWatcher, FileWatcherConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    // Get the directory to watch from command line args or use current directory
    let watch_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap());
    
    println!("Starting file watcher for: {}", watch_path.display());
    
    // Create watcher configuration
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(100),
        ignore_patterns: vec![
            "*.tmp".to_string(),
            "*.swp".to_string(),
            ".git/*".to_string(),
            "target/*".to_string(),
        ],
        watch_hidden: false,
        ..Default::default()
    };
    
    // Create the file watcher
    let (watcher, mut receiver) = FileWatcher::new(&watch_path, config);
    
    // Start watching
    watcher.start().await?;
    
    println!("File watcher started. Make some changes to files in the directory...");
    println!("Press Ctrl+C to stop.");
    
    // Handle events
    let event_handler = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            match event {
                volebarn_console::file_watcher::FileEvent::Created { path } => {
                    println!("📄 File created: {}", path.display());
                }
                volebarn_console::file_watcher::FileEvent::Modified { path } => {
                    println!("✏️  File modified: {}", path.display());
                }
                volebarn_console::file_watcher::FileEvent::Deleted { path } => {
                    println!("🗑️  File deleted: {}", path.display());
                }
                volebarn_console::file_watcher::FileEvent::Moved { from, to } => {
                    println!("📦 File moved: {} -> {}", from.display(), to.display());
                }
                volebarn_console::file_watcher::FileEvent::DirectoryCreated { path } => {
                    println!("📁 Directory created: {}", path.display());
                }
                volebarn_console::file_watcher::FileEvent::DirectoryDeleted { path } => {
                    println!("📂 Directory deleted: {}", path.display());
                }
            }
        }
    });
    
    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    
    println!("\nStopping file watcher...");
    watcher.stop().await;
    
    // Give some time for cleanup
    sleep(Duration::from_millis(100)).await;
    
    // Cancel the event handler
    event_handler.abort();
    
    // Print final statistics
    let stats = watcher.stats();
    println!("Final statistics:");
    println!("  Raw events received: {}", stats.raw_events_received);
    println!("  Events filtered: {}", stats.events_filtered);
    println!("  Events debounced: {}", stats.events_debounced);
    println!("  Events processed: {}", stats.events_processed);
    println!("  Processing errors: {}", stats.processing_errors);
    println!("  Watcher errors: {}", stats.watcher_errors);
    
    println!("File watcher stopped.");
    Ok(())
}