//! Async file system monitoring with lock-free event handling
//! 
//! This module provides real-time file system monitoring using the notify crate
//! with async event handling, debouncing, and lock-free concurrent operations.

use crate::{Result, ConsoleError};
use dashmap::DashMap;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{error, info, trace};

/// File system event types that we care about
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileEvent {
    /// File was created
    Created { path: PathBuf },
    /// File was modified
    Modified { path: PathBuf },
    /// File was deleted
    Deleted { path: PathBuf },
    /// File was moved/renamed
    Moved { from: PathBuf, to: PathBuf },
    /// Directory was created
    DirectoryCreated { path: PathBuf },
    /// Directory was deleted
    DirectoryDeleted { path: PathBuf },
}

impl FileEvent {
    /// Get the primary path affected by this event
    pub fn path(&self) -> &Path {
        match self {
            FileEvent::Created { path } => path,
            FileEvent::Modified { path } => path,
            FileEvent::Deleted { path } => path,
            FileEvent::Moved { to, .. } => to,
            FileEvent::DirectoryCreated { path } => path,
            FileEvent::DirectoryDeleted { path } => path,
        }
    }
    
    /// Check if this event affects a directory
    pub fn is_directory_event(&self) -> bool {
        matches!(self, FileEvent::DirectoryCreated { .. } | FileEvent::DirectoryDeleted { .. })
    }
}

/// Statistics for file watcher operations
#[derive(Debug, Default)]
pub struct WatcherStats {
    /// Total events received from notify
    pub raw_events_received: AtomicU64,
    /// Events filtered out (ignored files, duplicates, etc.)
    pub events_filtered: AtomicU64,
    /// Events debounced (rapid changes)
    pub events_debounced: AtomicU64,
    /// Events successfully processed
    pub events_processed: AtomicU64,
    /// Errors encountered during processing
    pub processing_errors: AtomicU64,
    /// Watcher errors (file system issues)
    pub watcher_errors: AtomicU64,
}

impl WatcherStats {
    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> WatcherStatsSnapshot {
        WatcherStatsSnapshot {
            raw_events_received: self.raw_events_received.load(Ordering::Relaxed),
            events_filtered: self.events_filtered.load(Ordering::Relaxed),
            events_debounced: self.events_debounced.load(Ordering::Relaxed),
            events_processed: self.events_processed.load(Ordering::Relaxed),
            processing_errors: self.processing_errors.load(Ordering::Relaxed),
            watcher_errors: self.watcher_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of watcher statistics at a point in time
#[derive(Debug, Clone)]
pub struct WatcherStatsSnapshot {
    pub raw_events_received: u64,
    pub events_filtered: u64,
    pub events_debounced: u64,
    pub events_processed: u64,
    pub processing_errors: u64,
    pub watcher_errors: u64,
}

/// Configuration for the file watcher
#[derive(Debug, Clone)]
pub struct FileWatcherConfig {
    /// Debounce delay for rapid file changes
    pub debounce_delay: Duration,
    /// Maximum number of events to queue before dropping
    pub max_event_queue_size: usize,
    /// Patterns to ignore (e.g., temporary files, hidden files)
    pub ignore_patterns: Vec<String>,
    /// Whether to watch hidden files and directories
    pub watch_hidden: bool,
    /// Polling interval for fallback polling mode
    pub poll_interval: Duration,
}

impl Default for FileWatcherConfig {
    fn default() -> Self {
        Self {
            debounce_delay: Duration::from_millis(100),
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
            watch_hidden: false,
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Debounce entry for tracking rapid file changes
#[derive(Debug)]
struct DebounceEntry {
    /// Last time this path was modified
    last_modified: AtomicU64,
    /// Number of pending events for this path
    pending_count: AtomicUsize,
}

impl DebounceEntry {
    fn new() -> Self {
        Self {
            last_modified: AtomicU64::new(0),
            pending_count: AtomicUsize::new(0),
        }
    }
    
    /// Update the last modified time and increment pending count
    fn touch(&self) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        self.last_modified.store(now, Ordering::Relaxed);
        self.pending_count.fetch_add(1, Ordering::Relaxed)
    }
    
    /// Check if enough time has passed since last modification
    fn should_process(&self, debounce_delay: Duration) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        let last_modified = self.last_modified.load(Ordering::Relaxed);
        let elapsed = Duration::from_millis(now.saturating_sub(last_modified));
        
        elapsed >= debounce_delay
    }
    

}

/// Async file system watcher with lock-free event handling
pub struct FileWatcher {
    /// Configuration for the watcher
    config: FileWatcherConfig,
    /// Path being watched
    watch_path: PathBuf,
    /// Channel for sending processed events
    event_sender: mpsc::UnboundedSender<FileEvent>,
    /// Statistics for monitoring performance
    stats: Arc<WatcherStats>,
    /// Debounce tracking for rapid changes
    debounce_map: Arc<DashMap<PathBuf, DebounceEntry>>,
    /// Whether the watcher is currently running
    is_running: Arc<std::sync::atomic::AtomicBool>,
}

impl FileWatcher {
    /// Create a new file watcher for the specified path
    pub fn new(
        watch_path: impl Into<PathBuf>,
        config: FileWatcherConfig,
    ) -> (Self, mpsc::UnboundedReceiver<FileEvent>) {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        
        let watcher = Self {
            config,
            watch_path: watch_path.into(),
            event_sender,
            stats: Arc::new(WatcherStats::default()),
            debounce_map: Arc::new(DashMap::new()),
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        
        (watcher, event_receiver)
    }
    
    /// Start watching the file system asynchronously
    pub async fn start(&self) -> Result<()> {
        if self.is_running.swap(true, Ordering::Relaxed) {
            return Err(ConsoleError::Config("File watcher is already running".to_string()));
        }
        
        info!("Starting file watcher for path: {}", self.watch_path.display());
        
        // Validate watch path
        if !self.watch_path.exists() {
            self.is_running.store(false, Ordering::Relaxed);
            return Err(ConsoleError::Config(format!(
                "Watch path does not exist: {}", 
                self.watch_path.display()
            )));
        }
        
        if !self.watch_path.is_dir() {
            self.is_running.store(false, Ordering::Relaxed);
            return Err(ConsoleError::Config(format!(
                "Watch path is not a directory: {}", 
                self.watch_path.display()
            )));
        }
        
        // Create notify watcher with async event handling
        let (notify_sender, mut notify_receiver) = mpsc::unbounded_channel();
        
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<Event>| {
                if let Err(e) = notify_sender.send(result) {
                    error!("Failed to send notify event: {}", e);
                }
            },
            Config::default(),
        ).map_err(|e| {
            self.is_running.store(false, Ordering::Relaxed);
            ConsoleError::Config(format!("Failed to create file watcher: {}", e))
        })?;
        
        // Start watching the directory recursively
        watcher.watch(&self.watch_path, RecursiveMode::Recursive)
            .map_err(|e| {
                self.is_running.store(false, Ordering::Relaxed);
                ConsoleError::Config(format!("Failed to start watching: {}", e))
            })?;
        
        // Clone necessary data for the async task
        let event_sender = self.event_sender.clone();
        let stats = Arc::clone(&self.stats);
        let debounce_map = Arc::clone(&self.debounce_map);
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        
        // Spawn async task to handle notify events
        tokio::spawn(async move {
            // Keep the watcher alive by moving it into the task
            let _watcher = watcher;
            
            while let Some(result) = notify_receiver.recv().await {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                
                match result {
                    Ok(event) => {
                        stats.raw_events_received.fetch_add(1, Ordering::Relaxed);
                        
                        if let Err(e) = Self::process_notify_event(
                            event,
                            &event_sender,
                            &stats,
                            &debounce_map,
                            &config,
                        ).await {
                            error!("Error processing file system event: {}", e);
                            stats.processing_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        error!("File system watcher error: {}", e);
                        stats.watcher_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            
            info!("File watcher event processing task stopped");
        });
        
        // Start debounce cleanup task
        self.start_debounce_cleanup().await;
        
        info!("File watcher started successfully");
        Ok(())
    }
    
    /// Stop the file watcher
    pub async fn stop(&self) {
        if !self.is_running.swap(false, Ordering::Relaxed) {
            return;
        }
        
        info!("Stopping file watcher");
        
        // Give some time for pending events to be processed
        sleep(Duration::from_millis(100)).await;
        
        // Clear debounce map
        self.debounce_map.clear();
        
        info!("File watcher stopped");
    }
    
    /// Get current statistics
    pub fn stats(&self) -> WatcherStatsSnapshot {
        self.stats.snapshot()
    }
    
    /// Check if the watcher is currently running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
    
    /// Process a notify event and convert it to our FileEvent format
    async fn process_notify_event(
        event: Event,
        event_sender: &mpsc::UnboundedSender<FileEvent>,
        stats: &WatcherStats,
        debounce_map: &DashMap<PathBuf, DebounceEntry>,
        config: &FileWatcherConfig,
    ) -> Result<()> {
        trace!("Processing notify event: {:?}", event);
        
        // Convert notify event to our FileEvent format
        let file_events = Self::convert_notify_event(event)?;
        
        for file_event in file_events {
            // Apply filtering
            if Self::should_ignore_event(&file_event, config) {
                stats.events_filtered.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            
            // Apply debouncing for rapid changes
            if Self::should_debounce_event(&file_event, debounce_map, config) {
                stats.events_debounced.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            
            // Send the processed event
            if let Err(e) = event_sender.send(file_event) {
                error!("Failed to send file event: {}", e);
                return Err(ConsoleError::Config("Event channel closed".to_string()));
            }
            
            stats.events_processed.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(())
    }
    
    /// Convert notify Event to our FileEvent format
    fn convert_notify_event(event: Event) -> Result<Vec<FileEvent>> {
        let mut file_events = Vec::new();
        
        match event.kind {
            EventKind::Create(create_kind) => {
                for path in event.paths {
                    match create_kind {
                        notify::event::CreateKind::File => {
                            file_events.push(FileEvent::Created { path });
                        }
                        notify::event::CreateKind::Folder => {
                            file_events.push(FileEvent::DirectoryCreated { path });
                        }
                        _ => {
                            // For other create kinds, determine based on path existence and type
                            if path.exists() {
                                if path.is_dir() {
                                    file_events.push(FileEvent::DirectoryCreated { path });
                                } else {
                                    file_events.push(FileEvent::Created { path });
                                }
                            } else {
                                // If path doesn't exist, assume it's a file creation
                                file_events.push(FileEvent::Created { path });
                            }
                        }
                    }
                }
            }
            EventKind::Modify(modify_kind) => {
                match modify_kind {
                    notify::event::ModifyKind::Data(_) => {
                        for path in event.paths {
                            if !path.is_dir() {
                                file_events.push(FileEvent::Modified { path });
                            }
                        }
                    }
                    notify::event::ModifyKind::Name(rename_mode) => {
                        match rename_mode {
                            notify::event::RenameMode::Both => {
                                if event.paths.len() >= 2 {
                                    let from = event.paths[0].clone();
                                    let to = event.paths[1].clone();
                                    file_events.push(FileEvent::Moved { from, to });
                                }
                            }
                            notify::event::RenameMode::From => {
                                for path in event.paths {
                                    if path.is_dir() {
                                        file_events.push(FileEvent::DirectoryDeleted { path });
                                    } else {
                                        file_events.push(FileEvent::Deleted { path });
                                    }
                                }
                            }
                            notify::event::RenameMode::To => {
                                for path in event.paths {
                                    if path.is_dir() {
                                        file_events.push(FileEvent::DirectoryCreated { path });
                                    } else {
                                        file_events.push(FileEvent::Created { path });
                                    }
                                }
                            }
                            _ => {
                                // Handle other rename modes as modifications
                                for path in event.paths {
                                    if !path.is_dir() {
                                        file_events.push(FileEvent::Modified { path });
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        // Handle other modify kinds as modifications
                        for path in event.paths {
                            if !path.is_dir() {
                                file_events.push(FileEvent::Modified { path });
                            }
                        }
                    }
                }
            }
            EventKind::Remove(remove_kind) => {
                for path in event.paths {
                    match remove_kind {
                        notify::event::RemoveKind::File => {
                            file_events.push(FileEvent::Deleted { path });
                        }
                        notify::event::RemoveKind::Folder => {
                            file_events.push(FileEvent::DirectoryDeleted { path });
                        }
                        _ => {
                            // For other remove kinds, we can't easily determine if it was a file or directory
                            // since the path no longer exists. We'll make a best guess based on the path.
                            // If it has an extension, assume it's a file, otherwise assume directory.
                            if path.extension().is_some() {
                                file_events.push(FileEvent::Deleted { path });
                            } else {
                                // Could be either, but default to file for safety
                                file_events.push(FileEvent::Deleted { path });
                            }
                        }
                    }
                }
            }
            _ => {
                // Handle other event kinds as generic modifications
                for path in event.paths {
                    if !path.is_dir() {
                        file_events.push(FileEvent::Modified { path });
                    }
                }
            }
        }
        
        Ok(file_events)
    }
    
    /// Check if an event should be ignored based on configuration
    fn should_ignore_event(event: &FileEvent, config: &FileWatcherConfig) -> bool {
        let path = event.path();
        
        // Check if we should ignore hidden files
        if !config.watch_hidden {
            if let Some(file_name) = path.file_name() {
                if let Some(name_str) = file_name.to_str() {
                    if name_str.starts_with('.') {
                        return true;
                    }
                }
            }
            
            // Check if any parent directory is hidden
            for component in path.components() {
                if let Some(name) = component.as_os_str().to_str() {
                    if name.starts_with('.') && name != "." && name != ".." {
                        return true;
                    }
                }
            }
        }
        
        // Check ignore patterns
        let path_str = path.to_string_lossy();
        for pattern in &config.ignore_patterns {
            if Self::matches_pattern(&path_str, pattern) {
                return true;
            }
        }
        
        false
    }
    
    /// Simple pattern matching for ignore patterns
    fn matches_pattern(path: &str, pattern: &str) -> bool {
        if pattern.contains('*') {
            // Simple glob pattern matching
            if pattern.starts_with('*') && pattern.ends_with('*') {
                let middle = &pattern[1..pattern.len()-1];
                path.contains(middle)
            } else if pattern.starts_with('*') {
                let suffix = &pattern[1..];
                path.ends_with(suffix)
            } else if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len()-1];
                path.starts_with(prefix)
            } else {
                // Pattern with * in the middle - more complex matching
                let parts: Vec<&str> = pattern.split('*').collect();
                if parts.len() == 2 {
                    path.starts_with(parts[0]) && path.ends_with(parts[1])
                } else {
                    false
                }
            }
        } else {
            path == pattern || path.ends_with(&format!("/{}", pattern))
        }
    }
    
    /// Check if an event should be debounced due to rapid changes
    fn should_debounce_event(
        event: &FileEvent,
        debounce_map: &DashMap<PathBuf, DebounceEntry>,
        config: &FileWatcherConfig,
    ) -> bool {
        // Skip debouncing for directory events and deletions
        match event {
            FileEvent::DirectoryCreated { .. } | 
            FileEvent::DirectoryDeleted { .. } |
            FileEvent::Deleted { .. } => return false,
            _ => {}
        }
        
        let path = event.path().to_path_buf();
        
        // Get or create debounce entry
        let entry = debounce_map.entry(path).or_insert_with(DebounceEntry::new);
        
        // Check if enough time has passed since last modification
        let should_process = entry.should_process(config.debounce_delay);
        
        // Update the entry timestamp
        entry.touch();
        
        // Return true if we should debounce (i.e., not process)
        !should_process
    }
    
    /// Start the debounce cleanup task
    async fn start_debounce_cleanup(&self) {
        let debounce_map = Arc::clone(&self.debounce_map);
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        
        tokio::spawn(async move {
            let mut cleanup_interval = interval(config.debounce_delay * 2);
            
            while is_running.load(Ordering::Relaxed) {
                cleanup_interval.tick().await;
                
                // Remove entries that haven't been touched recently
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                
                let cleanup_threshold = config.debounce_delay.as_millis() as u64 * 5;
                
                debounce_map.retain(|_, entry| {
                    let last_modified = entry.last_modified.load(Ordering::Relaxed);
                    let elapsed = now.saturating_sub(last_modified);
                    elapsed < cleanup_threshold
                });
            }
        });
    }
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        // Ensure the watcher is stopped when dropped
        self.is_running.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;