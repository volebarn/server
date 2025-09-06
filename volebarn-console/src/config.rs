//! Console application configuration

use crate::{Result, ConsoleError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;

/// Console application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server URL for the Volebarn backend
    pub server_url: String,
    
    /// Local folder to monitor and sync
    pub local_folder: PathBuf,
    
    /// Polling interval for file system changes (in seconds)
    pub poll_interval_secs: u64,
    
    /// Maximum retry attempts for failed operations
    pub max_retry_attempts: u32,
    
    /// Initial retry delay in milliseconds
    pub initial_retry_delay_ms: u64,
    
    /// Maximum retry delay in milliseconds
    pub max_retry_delay_ms: u64,
    
    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
    
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
    
    /// Enable TLS certificate verification
    pub verify_tls: bool,
    
    /// Log level (trace, debug, info, warn, error)
    pub log_level: String,
    
    /// Maximum concurrent operations for sync
    pub max_concurrent_operations: usize,
    
    /// Debounce delay for file watcher in milliseconds
    pub debounce_delay_ms: u64,
    
    /// Maximum event queue size for file watcher
    pub max_event_queue_size: usize,
    
    /// File patterns to ignore during watching
    pub ignore_patterns: Vec<String>,
    
    /// Whether to watch hidden files and directories
    pub watch_hidden_files: bool,
    
    /// Periodic sync interval in seconds (fallback mechanism)
    pub periodic_sync_interval_secs: u64,
    
    /// Maximum retries for client operations
    pub max_retries: u32,
    
    /// Retry delay in milliseconds for client operations
    pub retry_delay_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_url: "https://localhost:8080".to_string(),
            local_folder: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            poll_interval_secs: 1,
            max_retry_attempts: 5,
            initial_retry_delay_ms: 1000,
            max_retry_delay_ms: 30000,
            connection_timeout_secs: 30,
            request_timeout_secs: 60,
            verify_tls: true,
            log_level: "info".to_string(),
            max_concurrent_operations: 10,
            debounce_delay_ms: 100,
            max_event_queue_size: 10000,
            ignore_patterns: vec![
                "*.tmp".to_string(),
                "*.swp".to_string(),
                "*.swo".to_string(),
                "*~".to_string(),
                ".DS_Store".to_string(),
                "Thumbs.db".to_string(),
                ".git/*".to_string(),
                ".svn/*".to_string(),
                "node_modules/*".to_string(),
                "target/*".to_string(),
            ],
            watch_hidden_files: false,
            periodic_sync_interval_secs: 300, // 5 minutes
            max_retries: 3,
            retry_delay_ms: 1000,
        }
    }
}

impl Config {
    /// Load configuration from multiple sources with precedence:
    /// 1. Command line arguments (highest priority)
    /// 2. Environment variables
    /// 3. Configuration file
    /// 4. Default values (lowest priority)
    pub async fn load(
        config_path: Option<&str>,
        cli_server_url: Option<String>,
        cli_local_folder: Option<String>,
    ) -> Result<Self> {
        // Start with default configuration
        let mut config = Self::default();
        
        // Load from configuration file if provided
        if let Some(path) = config_path {
            config = Self::load_from_file(path).await?;
        } else {
            // Try to load from default locations
            if let Ok(file_config) = Self::try_load_default_config().await {
                config = file_config;
            }
        }
        
        // Override with environment variables
        config.load_from_env();
        
        // Override with command line arguments (highest priority)
        if let Some(server_url) = cli_server_url {
            config.server_url = server_url;
        }
        
        if let Some(local_folder) = cli_local_folder {
            config.local_folder = PathBuf::from(local_folder);
        }
        
        // Validate configuration
        config.validate()?;
        
        Ok(config)
    }
    
    /// Load configuration from a specific file
    pub async fn load_from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).await
            .map_err(|e| ConsoleError::Config(format!("Failed to read config file '{}': {}", path, e)))?;
        
        let config: Config = if path.ends_with(".json") {
            serde_json::from_str(&content)
                .map_err(|e| ConsoleError::Config(format!("Failed to parse JSON config: {}", e)))?
        } else if path.ends_with(".toml") {
            toml::from_str(&content)
                .map_err(|e| ConsoleError::Config(format!("Failed to parse TOML config: {}", e)))?
        } else {
            // Default to JSON
            serde_json::from_str(&content)
                .map_err(|e| ConsoleError::Config(format!("Failed to parse config file: {}", e)))?
        };
        
        Ok(config)
    }
    
    /// Try to load configuration from default locations
    async fn try_load_default_config() -> Result<Self> {
        let default_paths = [
            "volebarn.json",
            "volebarn.toml",
            ".volebarn.json",
            ".volebarn.toml",
        ];
        
        // Try user config directory
        if let Some(config_dir) = dirs::config_dir() {
            let user_config_path = config_dir.join("volebarn").join("config.json");
            if user_config_path.exists() {
                if let Ok(config) = Self::load_from_file(&user_config_path.to_string_lossy()).await {
                    return Ok(config);
                }
            }
        }
        
        // Try current directory
        for path in &default_paths {
            if PathBuf::from(path).exists() {
                if let Ok(config) = Self::load_from_file(path).await {
                    return Ok(config);
                }
            }
        }
        
        Err(ConsoleError::Config("No default configuration file found".to_string()))
    }
    
    /// Load configuration overrides from environment variables
    fn load_from_env(&mut self) {
        if let Ok(server_url) = std::env::var("VOLEBARN_SERVER_URL") {
            self.server_url = server_url;
        }
        
        if let Ok(local_folder) = std::env::var("VOLEBARN_LOCAL_FOLDER") {
            self.local_folder = PathBuf::from(local_folder);
        }
        
        if let Ok(poll_interval) = std::env::var("VOLEBARN_POLL_INTERVAL_SECS") {
            if let Ok(interval) = poll_interval.parse() {
                self.poll_interval_secs = interval;
            }
        }
        
        if let Ok(max_retries) = std::env::var("VOLEBARN_MAX_RETRY_ATTEMPTS") {
            if let Ok(retries) = max_retries.parse() {
                self.max_retry_attempts = retries;
            }
        }
        
        if let Ok(initial_delay) = std::env::var("VOLEBARN_INITIAL_RETRY_DELAY_MS") {
            if let Ok(delay) = initial_delay.parse() {
                self.initial_retry_delay_ms = delay;
            }
        }
        
        if let Ok(max_delay) = std::env::var("VOLEBARN_MAX_RETRY_DELAY_MS") {
            if let Ok(delay) = max_delay.parse() {
                self.max_retry_delay_ms = delay;
            }
        }
        
        if let Ok(conn_timeout) = std::env::var("VOLEBARN_CONNECTION_TIMEOUT_SECS") {
            if let Ok(timeout) = conn_timeout.parse() {
                self.connection_timeout_secs = timeout;
            }
        }
        
        if let Ok(req_timeout) = std::env::var("VOLEBARN_REQUEST_TIMEOUT_SECS") {
            if let Ok(timeout) = req_timeout.parse() {
                self.request_timeout_secs = timeout;
            }
        }
        
        if let Ok(verify_tls) = std::env::var("VOLEBARN_VERIFY_TLS") {
            self.verify_tls = verify_tls.to_lowercase() == "true" || verify_tls == "1";
        }
        
        if let Ok(log_level) = std::env::var("VOLEBARN_LOG_LEVEL") {
            self.log_level = log_level;
        }
    }
    
    /// Validate configuration values
    pub fn validate(&self) -> Result<()> {
        // Validate server URL
        if self.server_url.is_empty() {
            return Err(ConsoleError::Config("Server URL cannot be empty".to_string()));
        }
        
        if !self.server_url.starts_with("http://") && !self.server_url.starts_with("https://") {
            return Err(ConsoleError::Config("Server URL must start with http:// or https://".to_string()));
        }
        
        // Validate local folder
        if !self.local_folder.exists() {
            return Err(ConsoleError::Config(format!(
                "Local folder does not exist: {}", 
                self.local_folder.display()
            )));
        }
        
        if !self.local_folder.is_dir() {
            return Err(ConsoleError::Config(format!(
                "Local folder path is not a directory: {}", 
                self.local_folder.display()
            )));
        }
        
        // Validate timing values
        if self.poll_interval_secs == 0 {
            return Err(ConsoleError::Config("Poll interval must be greater than 0".to_string()));
        }
        
        if self.max_retry_attempts == 0 {
            return Err(ConsoleError::Config("Max retry attempts must be greater than 0".to_string()));
        }
        
        if self.initial_retry_delay_ms == 0 {
            return Err(ConsoleError::Config("Initial retry delay must be greater than 0".to_string()));
        }
        
        if self.max_retry_delay_ms < self.initial_retry_delay_ms {
            return Err(ConsoleError::Config("Max retry delay must be >= initial retry delay".to_string()));
        }
        
        if self.connection_timeout_secs == 0 {
            return Err(ConsoleError::Config("Connection timeout must be greater than 0".to_string()));
        }
        
        if self.request_timeout_secs == 0 {
            return Err(ConsoleError::Config("Request timeout must be greater than 0".to_string()));
        }
        
        // Validate log level
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.log_level.to_lowercase().as_str()) {
            return Err(ConsoleError::Config(format!(
                "Invalid log level '{}'. Valid levels: {}", 
                self.log_level,
                valid_levels.join(", ")
            )));
        }
        
        Ok(())
    }
    
    /// Get poll interval as Duration
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }
    
    /// Get initial retry delay as Duration
    pub fn initial_retry_delay(&self) -> Duration {
        Duration::from_millis(self.initial_retry_delay_ms)
    }
    
    /// Get max retry delay as Duration
    pub fn max_retry_delay(&self) -> Duration {
        Duration::from_millis(self.max_retry_delay_ms)
    }
    
    /// Get connection timeout as Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_secs(self.connection_timeout_secs)
    }
    
    /// Get request timeout as Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }
    
    /// Save configuration to file
    pub async fn save_to_file(&self, path: &str) -> Result<()> {
        let content = if path.ends_with(".toml") {
            toml::to_string_pretty(self)
                .map_err(|e| ConsoleError::Config(format!("Failed to serialize TOML config: {}", e)))?
        } else {
            // Default to JSON
            serde_json::to_string_pretty(self)
                .map_err(|e| ConsoleError::Config(format!("Failed to serialize JSON config: {}", e)))?
        };
        
        fs::write(path, content).await
            .map_err(|e| ConsoleError::Config(format!("Failed to write config file '{}': {}", path, e)))?;
        
        Ok(())
    }
    
    /// Get file index configuration
    pub fn file_index_config(&self) -> crate::FileIndexConfig {
        crate::FileIndexConfig {
            persistence_path: None, // For now, use in-memory only
            enable_compression: true,
        }
    }
    
    /// Get default conflict resolution strategy
    pub fn default_conflict_resolution(&self) -> volebarn_client::types::ConflictResolutionStrategy {
        volebarn_client::types::ConflictResolutionStrategy::PreferLocal
    }
}

#[cfg(test)]
mod tests;