//! Main console application

use crate::{Config, Result, ConsoleError, FileIndex, LocalFileManager, SyncManager};
use crate::file_watcher::{FileWatcher, FileWatcherConfig, FileEvent};
use crate::conflict_resolver::ConflictResolver;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, interval};
use tracing::{info, debug, warn, error, instrument};
use volebarn_client::{Client, Config as ClientConfig};
use volebarn_client::types::ConflictResolutionStrategy;

/// Statistics for real-time sync operations
#[derive(Debug, Default)]
pub struct RealTimeSyncStats {
    /// Total file events processed
    pub events_processed: AtomicU64,
    /// Files uploaded in real-time
    pub files_uploaded: AtomicU64,
    /// Files downloaded in real-time
    pub files_downloaded: AtomicU64,
    /// Files deleted in real-time
    pub files_deleted: AtomicU64,
    /// Directories created in real-time
    pub directories_created: AtomicU64,
    /// Directories deleted in real-time
    pub directories_deleted: AtomicU64,
    /// Move/rename operations processed
    pub move_operations: AtomicU64,
    /// Sync errors encountered
    pub sync_errors: AtomicU64,
}

impl RealTimeSyncStats {
    pub fn increment_events_processed(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_files_uploaded(&self) {
        self.files_uploaded.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_files_downloaded(&self) {
        self.files_downloaded.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_files_deleted(&self) {
        self.files_deleted.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_directories_created(&self) {
        self.directories_created.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_directories_deleted(&self) {
        self.directories_deleted.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_move_operations(&self) {
        self.move_operations.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_sync_errors(&self) {
        self.sync_errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Main console application with integrated file watching and real-time sync
pub struct App {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    client: Arc<Client>,
    file_index: Arc<FileIndex>,
    local_file_manager: Arc<LocalFileManager>,
    sync_manager: Arc<SyncManager>,
    conflict_resolver: Arc<ConflictResolver>,
    file_watcher: Option<FileWatcher>,
    event_receiver: Option<mpsc::UnboundedReceiver<FileEvent>>,
    realtime_stats: Arc<RealTimeSyncStats>,
}

impl App {
    /// Create a new console application instance with integrated components
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
        
        info!("Initializing console application components...");
        
        // Initialize client
        let client_config = ClientConfig::new(config.server_url.clone());
        client_config.tls_verify.store(config.verify_tls, std::sync::atomic::Ordering::Relaxed);
        client_config.connect_timeout.store(config.connection_timeout_secs, std::sync::atomic::Ordering::Relaxed);
        client_config.request_timeout.store(config.request_timeout_secs, std::sync::atomic::Ordering::Relaxed);
        client_config.max_retries.store(config.max_retries, std::sync::atomic::Ordering::Relaxed);
        client_config.initial_retry_delay.store(config.retry_delay_ms, std::sync::atomic::Ordering::Relaxed);
        
        let client = Arc::new(Client::new(client_config).await?);
        debug!("Client initialized");
        
        // Initialize local file manager
        let local_file_manager = Arc::new(LocalFileManager::new(config.local_folder.clone()).await?);
        debug!("Local file manager initialized");
        
        // Initialize file index
        let file_index = Arc::new(FileIndex::new(config.file_index_config()).await?);
        debug!("File index initialized");
        
        // Initialize sync manager
        let sync_manager = Arc::new(SyncManager::new(
            client.clone(),
            local_file_manager.clone(),
            file_index.clone(),
            config.max_concurrent_operations,
        ));
        debug!("Sync manager initialized");
        
        // Initialize conflict resolver
        let conflict_resolver = Arc::new(ConflictResolver::new(
            config.default_conflict_resolution(),
        ));
        debug!("Conflict resolver initialized");
        
        // Initialize file watcher
        let watcher_config = FileWatcherConfig {
            debounce_delay: Duration::from_millis(config.debounce_delay_ms),
            max_event_queue_size: config.max_event_queue_size,
            ignore_patterns: config.ignore_patterns.clone(),
            watch_hidden: config.watch_hidden_files,
            poll_interval: config.poll_interval(),
        };
        
        let (file_watcher, event_receiver) = FileWatcher::new(
            config.local_folder.clone(),
            watcher_config,
        );
        debug!("File watcher initialized");
        
        // Initialize real-time sync statistics
        let realtime_stats = Arc::new(RealTimeSyncStats::default());
        
        info!("Console application initialized successfully");
        debug!("Monitoring folder: {}", config.local_folder.display());
        debug!("Server URL: {}", config.server_url);
        debug!("Poll interval: {:?}", config.poll_interval());
        debug!("Max concurrent operations: {}", config.max_concurrent_operations);
        
        Ok(App { 
            config,
            shutdown_flag,
            client,
            file_index,
            local_file_manager,
            sync_manager,
            conflict_resolver,
            file_watcher: Some(file_watcher),
            event_receiver: Some(event_receiver),
            realtime_stats,
        })
    }
    
    /// Run the console application with integrated file watching and real-time sync
    pub async fn run(mut self) -> Result<()> {
        info!("Starting console application with real-time file synchronization");
        
        // Perform initial sync to establish baseline
        info!("Performing initial sync to establish baseline...");
        match self.sync_manager.full_sync(self.conflict_resolver.default_strategy()).await {
            Ok(result) => {
                info!("Initial sync completed: {} uploaded, {} downloaded, {} deleted, {} errors",
                      result.uploaded.len(), result.downloaded.len(), 
                      result.deleted_local.len(), result.errors.len());
            }
            Err(e) => {
                warn!("Initial sync failed, continuing with real-time monitoring: {}", e);
            }
        }
        
        // Start file watcher
        if let Some(file_watcher) = &self.file_watcher {
            file_watcher.start().await?;
            info!("File watcher started successfully");
        }
        
        // Take ownership of event receiver
        let mut event_receiver = self.event_receiver.take()
            .ok_or_else(|| ConsoleError::Config("Event receiver not available".to_string()))?;
        
        // Start real-time sync event processing task
        let sync_manager = self.sync_manager.clone();
        let conflict_resolver = self.conflict_resolver.clone();
        let file_index = self.file_index.clone();
        let realtime_stats = self.realtime_stats.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        
        let sync_task = tokio::spawn(async move {
            Self::process_file_events(
                &mut event_receiver,
                sync_manager,
                conflict_resolver,
                file_index,
                realtime_stats,
                shutdown_flag,
            ).await
        });
        
        // Start periodic status reporting
        let stats_task = self.start_status_reporting().await;
        
        // Start periodic full sync (fallback mechanism)
        let periodic_sync_task = self.start_periodic_sync().await;
        
        // Main application loop - monitor tasks and handle shutdown
        let mut iteration = 0u64;
        let mut sync_task_finished = false;
        
        while !self.shutdown_flag.load(Ordering::SeqCst) && !sync_task_finished {
            iteration += 1;
            
            // Check if any critical tasks have failed
            if sync_task.is_finished() {
                match sync_task.await {
                    Ok(Ok(())) => {
                        info!("File event processing task completed normally");
                    }
                    Ok(Err(e)) => {
                        error!("File event processing task failed: {}", e);
                        return Err(e);
                    }
                    Err(e) => {
                        error!("File event processing task panicked: {}", e);
                        return Err(ConsoleError::Config("File event processing task panicked".to_string()));
                    }
                }
                sync_task_finished = true;
                break;
            }
            
            // Sleep for a short interval to avoid busy waiting
            sleep(Duration::from_millis(100)).await;
            
            // Periodic status logging
            if iteration % 600 == 0 { // Every 60 seconds (600 * 100ms)
                debug!("Console application running normally (iteration {})", iteration);
                self.log_status().await;
            }
        }
        
        info!("Shutdown requested, stopping all tasks...");
        
        // Stop file watcher
        if let Some(file_watcher) = &self.file_watcher {
            file_watcher.stop().await;
        }
        
        // Cancel tasks if they haven't finished
        if !sync_task_finished {
            // sync_task was already awaited above, so we can't abort it here
        }
        stats_task.abort();
        periodic_sync_task.abort();
        
        // Perform cleanup operations
        self.cleanup().await?;
        
        info!("Console application shutdown complete");
        Ok(())
    }
    
    /// Process file system events and trigger immediate sync operations
    #[instrument(skip(event_receiver, sync_manager, conflict_resolver, file_index, realtime_stats, shutdown_flag))]
    async fn process_file_events(
        event_receiver: &mut mpsc::UnboundedReceiver<FileEvent>,
        sync_manager: Arc<SyncManager>,
        conflict_resolver: Arc<ConflictResolver>,
        file_index: Arc<FileIndex>,
        realtime_stats: Arc<RealTimeSyncStats>,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Result<()> {
        info!("Starting real-time file event processing");
        
        while !shutdown_flag.load(Ordering::SeqCst) {
            tokio::select! {
                // Process file events
                event = event_receiver.recv() => {
                    match event {
                        Some(file_event) => {
                            realtime_stats.increment_events_processed();
                            
                            if let Err(e) = Self::handle_file_event(
                                file_event,
                                &sync_manager,
                                &conflict_resolver,
                                &file_index,
                                &realtime_stats,
                            ).await {
                                error!("Error handling file event: {}", e);
                                realtime_stats.increment_sync_errors();
                            }
                        }
                        None => {
                            debug!("File event channel closed");
                            break;
                        }
                    }
                }
                
                // Check for shutdown
                _ = sleep(Duration::from_millis(100)) => {
                    if shutdown_flag.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }
        
        info!("File event processing stopped");
        Ok(())
    }
    
    /// Handle a single file system event with immediate sync
    #[instrument(skip(sync_manager, conflict_resolver, file_index, realtime_stats))]
    async fn handle_file_event(
        event: FileEvent,
        sync_manager: &Arc<SyncManager>,
        conflict_resolver: &Arc<ConflictResolver>,
        file_index: &Arc<FileIndex>,
        realtime_stats: &Arc<RealTimeSyncStats>,
    ) -> Result<()> {
        debug!("Handling file event: {:?}", event);
        
        match event {
            FileEvent::Created { path } => {
                Self::handle_file_created(path, sync_manager, file_index, realtime_stats).await?;
            }
            FileEvent::Modified { path } => {
                Self::handle_file_modified(path, sync_manager, conflict_resolver, file_index, realtime_stats).await?;
            }
            FileEvent::Deleted { path } => {
                Self::handle_file_deleted(path, sync_manager, file_index, realtime_stats).await?;
            }
            FileEvent::Moved { from, to } => {
                Self::handle_file_moved(from, to, sync_manager, file_index, realtime_stats).await?;
            }
            FileEvent::DirectoryCreated { path } => {
                Self::handle_directory_created(path, sync_manager, file_index, realtime_stats).await?;
            }
            FileEvent::DirectoryDeleted { path } => {
                Self::handle_directory_deleted(path, sync_manager, file_index, realtime_stats).await?;
            }
        }
        
        Ok(())
    }
    
    /// Handle file creation event
    #[instrument(skip(sync_manager, file_index, realtime_stats))]
    async fn handle_file_created(
        path: PathBuf,
        sync_manager: &Arc<SyncManager>,
        file_index: &Arc<FileIndex>,
        realtime_stats: &Arc<RealTimeSyncStats>,
    ) -> Result<()> {
        debug!("File created: {}", path.display());
        
        // Update file index
        if let Err(e) = file_index.add_file(&path, &path).await {
            warn!("Failed to update file index for created file {}: {}", path.display(), e);
        }
        
        // Convert to relative path for server operations
        let relative_path = path.to_string_lossy().to_string();
        
        // Upload the new file to server
        match sync_manager.upload_single_file(&relative_path).await {
            Ok(()) => {
                debug!("Successfully uploaded created file: {}", relative_path);
                file_index.mark_synced(&path);
                realtime_stats.increment_files_uploaded();
            }
            Err(e) => {
                warn!("Failed to upload created file {}: {}", relative_path, e);
                file_index.mark_modified(&path);
                realtime_stats.increment_sync_errors();
            }
        }
        
        Ok(())
    }
    
    /// Handle file modification event
    #[instrument(skip(sync_manager, conflict_resolver, file_index, realtime_stats))]
    async fn handle_file_modified(
        path: PathBuf,
        sync_manager: &Arc<SyncManager>,
        conflict_resolver: &Arc<ConflictResolver>,
        file_index: &Arc<FileIndex>,
        realtime_stats: &Arc<RealTimeSyncStats>,
    ) -> Result<()> {
        debug!("File modified: {}", path.display());
        
        // Update file index
        if let Err(e) = file_index.add_file(&path, &path).await {
            warn!("Failed to update file index for modified file {}: {}", path.display(), e);
        }
        
        // Mark as modified in index
        file_index.mark_modified(&path);
        
        // Convert to relative path for server operations
        let relative_path = path.to_string_lossy().to_string();
        
        // Handle the modification based on conflict resolution strategy
        match conflict_resolver.default_strategy() {
            ConflictResolutionStrategy::PreferLocal => {
                // Always upload local changes
                match sync_manager.upload_single_file(&relative_path).await {
                    Ok(()) => {
                        debug!("Successfully uploaded modified file: {}", relative_path);
                        file_index.mark_synced(&path);
                        realtime_stats.increment_files_uploaded();
                    }
                    Err(e) => {
                        warn!("Failed to upload modified file {}: {}", relative_path, e);
                        realtime_stats.increment_sync_errors();
                    }
                }
            }
            _ => {
                // For other strategies, we need to check server state first
                // This is more complex and might be handled in periodic sync
                debug!("Deferring conflict resolution for modified file: {}", relative_path);
            }
        }
        
        Ok(())
    }
    
    /// Handle file deletion event
    #[instrument(skip(sync_manager, file_index, realtime_stats))]
    async fn handle_file_deleted(
        path: PathBuf,
        sync_manager: &Arc<SyncManager>,
        file_index: &Arc<FileIndex>,
        realtime_stats: &Arc<RealTimeSyncStats>,
    ) -> Result<()> {
        debug!("File deleted: {}", path.display());
        
        // Remove from file index
        if let Err(e) = file_index.remove_file(&path).await {
            warn!("Failed to remove file from index {}: {}", path.display(), e);
        }
        
        // Convert to relative path for server operations
        let relative_path = path.to_string_lossy().to_string();
        
        // Delete the file from server
        match sync_manager.delete_single_file(&relative_path).await {
            Ok(()) => {
                debug!("Successfully deleted file from server: {}", relative_path);
                realtime_stats.increment_files_deleted();
            }
            Err(e) => {
                warn!("Failed to delete file from server {}: {}", relative_path, e);
                realtime_stats.increment_sync_errors();
            }
        }
        
        Ok(())
    }
    
    /// Handle file move/rename event
    #[instrument(skip(sync_manager, file_index, realtime_stats))]
    async fn handle_file_moved(
        from_path: PathBuf,
        to_path: PathBuf,
        sync_manager: &Arc<SyncManager>,
        file_index: &Arc<FileIndex>,
        realtime_stats: &Arc<RealTimeSyncStats>,
    ) -> Result<()> {
        debug!("File moved: {} -> {}", from_path.display(), to_path.display());
        
        // Update file index - remove old path and add new path
        if let Err(e) = file_index.remove_file(&from_path).await {
            warn!("Failed to remove old file from index {}: {}", from_path.display(), e);
        }
        if let Err(e) = file_index.add_file(&to_path, &to_path).await {
            warn!("Failed to update file index for moved file {}: {}", to_path.display(), e);
        }
        
        // Convert to relative paths for server operations
        let from_relative = from_path.to_string_lossy().to_string();
        let to_relative = to_path.to_string_lossy().to_string();
        
        // Handle move operation on server (delete old, upload new)
        // Note: This could be optimized with a dedicated move endpoint
        match sync_manager.delete_single_file(&from_relative).await {
            Ok(()) => {
                debug!("Successfully deleted old file from server: {}", from_relative);
                
                // Upload file at new location
                match sync_manager.upload_single_file(&to_relative).await {
                    Ok(()) => {
                        debug!("Successfully uploaded moved file: {}", to_relative);
                        file_index.mark_synced(&to_path);
                        realtime_stats.increment_move_operations();
                    }
                    Err(e) => {
                        warn!("Failed to upload moved file {}: {}", to_relative, e);
                        file_index.mark_modified(&to_path);
                        realtime_stats.increment_sync_errors();
                    }
                }
            }
            Err(e) => {
                warn!("Failed to delete old file from server {}: {}", from_relative, e);
                realtime_stats.increment_sync_errors();
            }
        }
        
        Ok(())
    }
    
    /// Handle directory creation event
    #[instrument(skip(sync_manager, file_index, realtime_stats))]
    async fn handle_directory_created(
        path: PathBuf,
        sync_manager: &Arc<SyncManager>,
        file_index: &Arc<FileIndex>,
        realtime_stats: &Arc<RealTimeSyncStats>,
    ) -> Result<()> {
        debug!("Directory created: {}", path.display());
        
        // Update file index
        if let Err(e) = file_index.add_file(&path, &path).await {
            warn!("Failed to update file index for created directory {}: {}", path.display(), e);
        }
        
        // Convert to relative path for server operations
        let relative_path = path.to_string_lossy().to_string();
        
        // Create directory on server
        match sync_manager.create_directory_on_server(&relative_path).await {
            Ok(()) => {
                debug!("Successfully created directory on server: {}", relative_path);
                file_index.mark_synced(&path);
                realtime_stats.increment_directories_created();
            }
            Err(e) => {
                warn!("Failed to create directory on server {}: {}", relative_path, e);
                realtime_stats.increment_sync_errors();
            }
        }
        
        Ok(())
    }
    
    /// Handle directory deletion event
    #[instrument(skip(sync_manager, file_index, realtime_stats))]
    async fn handle_directory_deleted(
        path: PathBuf,
        sync_manager: &Arc<SyncManager>,
        file_index: &Arc<FileIndex>,
        realtime_stats: &Arc<RealTimeSyncStats>,
    ) -> Result<()> {
        debug!("Directory deleted: {}", path.display());
        
        // Remove from file index (this should also remove all child files)
        file_index.remove_directory(&path);
        
        // Convert to relative path for server operations
        let relative_path = path.to_string_lossy().to_string();
        
        // Delete directory from server
        match sync_manager.delete_directory_on_server(&relative_path).await {
            Ok(()) => {
                debug!("Successfully deleted directory from server: {}", relative_path);
                realtime_stats.increment_directories_deleted();
            }
            Err(e) => {
                warn!("Failed to delete directory from server {}: {}", relative_path, e);
                realtime_stats.increment_sync_errors();
            }
        }
        
        Ok(())
    }
    
    /// Start periodic status reporting task
    async fn start_status_reporting(&self) -> tokio::task::JoinHandle<()> {
        let realtime_stats = self.realtime_stats.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        
        tokio::spawn(async move {
            let mut status_interval = interval(Duration::from_secs(300)); // Every 5 minutes
            
            while !shutdown_flag.load(Ordering::SeqCst) {
                status_interval.tick().await;
                
                let events = realtime_stats.events_processed.load(Ordering::Relaxed);
                let uploaded = realtime_stats.files_uploaded.load(Ordering::Relaxed);
                let downloaded = realtime_stats.files_downloaded.load(Ordering::Relaxed);
                let deleted = realtime_stats.files_deleted.load(Ordering::Relaxed);
                let dirs_created = realtime_stats.directories_created.load(Ordering::Relaxed);
                let dirs_deleted = realtime_stats.directories_deleted.load(Ordering::Relaxed);
                let moves = realtime_stats.move_operations.load(Ordering::Relaxed);
                let errors = realtime_stats.sync_errors.load(Ordering::Relaxed);
                
                info!("Real-time sync stats: {} events, {} uploaded, {} downloaded, {} deleted, {} dirs created, {} dirs deleted, {} moves, {} errors",
                      events, uploaded, downloaded, deleted, dirs_created, dirs_deleted, moves, errors);
            }
        })
    }
    
    /// Start periodic full sync task (fallback mechanism)
    async fn start_periodic_sync(&self) -> tokio::task::JoinHandle<()> {
        let sync_manager = self.sync_manager.clone();
        let conflict_resolver = self.conflict_resolver.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let sync_interval_secs = self.config.periodic_sync_interval_secs;
        
        tokio::spawn(async move {
            let mut sync_interval = interval(Duration::from_secs(sync_interval_secs));
            
            while !shutdown_flag.load(Ordering::SeqCst) {
                sync_interval.tick().await;
                
                if shutdown_flag.load(Ordering::SeqCst) {
                    break;
                }
                
                debug!("Starting periodic full sync");
                
                match sync_manager.full_sync(conflict_resolver.default_strategy()).await {
                    Ok(result) => {
                        if result.has_changes() {
                            info!("Periodic sync completed: {} uploaded, {} downloaded, {} deleted, {} errors",
                                  result.uploaded.len(), result.downloaded.len(), 
                                  result.deleted_local.len(), result.errors.len());
                        } else {
                            debug!("Periodic sync completed with no changes");
                        }
                    }
                    Err(e) => {
                        warn!("Periodic sync failed: {}", e);
                    }
                }
            }
        })
    }
    
    /// Log current application status
    async fn log_status(&self) {
        let events = self.realtime_stats.events_processed.load(Ordering::Relaxed);
        let uploaded = self.realtime_stats.files_uploaded.load(Ordering::Relaxed);
        let downloaded = self.realtime_stats.files_downloaded.load(Ordering::Relaxed);
        let errors = self.realtime_stats.sync_errors.load(Ordering::Relaxed);
        
        let watcher_running = self.file_watcher.as_ref()
            .map(|w| w.is_running())
            .unwrap_or(false);
        
        debug!("Status: watcher={}, events={}, uploaded={}, downloaded={}, errors={}",
               watcher_running, events, uploaded, downloaded, errors);
    }
    
    /// Perform cleanup operations during shutdown
    async fn cleanup(&self) -> Result<()> {
        debug!("Starting cleanup operations");
        
        // Stop file watcher if still running
        if let Some(file_watcher) = &self.file_watcher {
            if file_watcher.is_running() {
                file_watcher.stop().await;
                debug!("File watcher stopped");
            }
        }
        
        // Save file index state if configured
        if let Err(e) = self.file_index.save_state().await {
            warn!("Failed to save file index state: {}", e);
        }
        
        // Give some time for pending operations to complete
        sleep(Duration::from_millis(500)).await;
        
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