//! File index for tracking local state
//! 
//! This module provides a lock-free file index using DashMap for tracking the entire directory tree state.
//! It supports optional persistence using RocksDB with bitcode serialization and Snappy compression.
//! All operations are async and use atomic operations for change detection.

use crate::error::ConsoleError;
use dashmap::DashMap;
use rocksdb::{DB, Options, ColumnFamilyDescriptor};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tracing::{debug, info};
use volebarn_client::storage_types::StorageFileMetadata;
use volebarn_client::time_utils::SerializableSystemTime;
use xxhash_rust::xxh3::xxh3_64;

/// Sync status for files tracked atomically
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum SyncStatus {
    Synced = 0,
    Modified = 1,
    Deleted = 2,
    New = 3,
}

impl From<u8> for SyncStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => SyncStatus::Synced,
            1 => SyncStatus::Modified,
            2 => SyncStatus::Deleted,
            3 => SyncStatus::New,
            _ => SyncStatus::New, // Default to new for unknown values
        }
    }
}

/// File state tracked in memory with atomic operations
#[derive(Debug)]
pub struct FileState {
    /// File content hash
    pub hash: AtomicU64,
    /// File size in bytes
    pub size: AtomicU64,
    /// Last modified timestamp (as seconds since UNIX_EPOCH)
    pub modified: AtomicU64,
    /// Whether this is a directory
    pub is_directory: bool,
    /// Current sync status
    pub sync_status: AtomicU8,
}

impl FileState {
    /// Create new file state
    pub fn new(hash: u64, size: u64, modified: SystemTime, is_directory: bool) -> Self {
        let modified_secs = modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            hash: AtomicU64::new(hash),
            size: AtomicU64::new(size),
            modified: AtomicU64::new(modified_secs),
            is_directory,
            sync_status: AtomicU8::new(SyncStatus::New as u8),
        }
    }
    
    /// Get current hash
    pub fn get_hash(&self) -> u64 {
        self.hash.load(Ordering::Acquire)
    }
    
    /// Set hash atomically
    pub fn set_hash(&self, hash: u64) {
        self.hash.store(hash, Ordering::Release);
    }
    
    /// Get current size
    pub fn get_size(&self) -> u64 {
        self.size.load(Ordering::Acquire)
    }
    
    /// Set size atomically
    pub fn set_size(&self, size: u64) {
        self.size.store(size, Ordering::Release);
    }
    
    /// Get current modified time
    pub fn get_modified(&self) -> SystemTime {
        let secs = self.modified.load(Ordering::Acquire);
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs)
    }
    
    /// Set modified time atomically
    pub fn set_modified(&self, modified: SystemTime) {
        let secs = modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.modified.store(secs, Ordering::Release);
    }
    
    /// Get current sync status
    pub fn get_sync_status(&self) -> SyncStatus {
        SyncStatus::from(self.sync_status.load(Ordering::Acquire))
    }
    
    /// Set sync status atomically
    pub fn set_sync_status(&self, status: SyncStatus) {
        self.sync_status.store(status as u8, Ordering::Release);
    }
    
    /// Update file state and mark as modified if changed
    pub fn update(&self, hash: u64, size: u64, modified: SystemTime) -> bool {
        let current_hash = self.get_hash();
        let current_size = self.get_size();
        let current_modified = self.get_modified();
        
        let changed = current_hash != hash || 
                     current_size != size || 
                     current_modified != modified;
        
        if changed {
            self.set_hash(hash);
            self.set_size(size);
            self.set_modified(modified);
            self.set_sync_status(SyncStatus::Modified);
        }
        
        changed
    }
}

/// Configuration for file index persistence
#[derive(Debug, Clone)]
pub struct FileIndexConfig {
    /// Optional path to RocksDB database for persistence
    pub persistence_path: Option<PathBuf>,
    /// Whether to enable Snappy compression for stored data
    pub enable_compression: bool,
}

impl Default for FileIndexConfig {
    fn default() -> Self {
        Self {
            persistence_path: None,
            enable_compression: true,
        }
    }
}

/// Lock-free file index for tracking directory tree state
#[derive(Debug)]
pub struct FileIndex {
    /// In-memory index using DashMap for lock-free access
    entries: DashMap<PathBuf, Arc<FileState>>,
    /// Optional RocksDB for persistence
    db: Option<Arc<DB>>,
    /// Configuration
    config: FileIndexConfig,
}

impl FileIndex {
    /// Create a new file index with optional persistence
    pub async fn new(config: FileIndexConfig) -> Result<Self, ConsoleError> {
        let db = if let Some(ref db_path) = config.persistence_path {
            Some(Self::open_database(db_path, config.enable_compression).await?)
        } else {
            None
        };
        
        let index = Self {
            entries: DashMap::new(),
            db,
            config,
        };
        
        // Load existing data from database if available
        if index.db.is_some() {
            index.load_from_database().await?;
        }
        
        Ok(index)
    }
    
    /// Open RocksDB database with proper configuration
    async fn open_database(db_path: &Path, enable_compression: bool) -> Result<Arc<DB>, ConsoleError> {
        // Create database directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        if enable_compression {
            opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        }
        
        // Define column families
        let cf_files = ColumnFamilyDescriptor::new("files", Options::default());
        let cf_directories = ColumnFamilyDescriptor::new("directories", Options::default());
        
        let db = DB::open_cf_descriptors(&opts, db_path, vec![cf_files, cf_directories])
            .map_err(|e| ConsoleError::Index(format!("Failed to open database: {}", e)))?;
        
        info!("Opened file index database at: {}", db_path.display());
        Ok(Arc::new(db))
    }
    
    /// Load existing data from database into memory
    async fn load_from_database(&self) -> Result<(), ConsoleError> {
        let Some(ref db) = self.db else {
            return Ok(());
        };
        
        let cf_files = db.cf_handle("files")
            .ok_or_else(|| ConsoleError::Index("Files column family not found".to_string()))?;
        
        let iter = db.iterator_cf(&cf_files, rocksdb::IteratorMode::Start);
        let mut loaded_count = 0;
        
        for item in iter {
            let (key, value) = item
                .map_err(|e| ConsoleError::Index(format!("Database iteration error: {}", e)))?;
            
            // Decompress and deserialize
            let decompressed = if self.config.enable_compression {
                snap::raw::Decoder::new()
                    .decompress_vec(&value)
                    .map_err(|e| ConsoleError::Index(format!("Decompression error: {}", e)))?
            } else {
                value.to_vec()
            };
            
            let metadata: StorageFileMetadata = bitcode::decode(&decompressed)
                .map_err(|e| ConsoleError::Index(format!("Deserialization error: {}", e)))?;
            
            let path = PathBuf::from(String::from_utf8_lossy(&key).to_string());
            let modified = SystemTime::from(metadata.modified);
            
            let file_state = Arc::new(FileState::new(
                metadata.xxhash3,
                metadata.size,
                modified,
                metadata.is_directory,
            ));
            
            // Mark as synced since it was loaded from persistent storage
            file_state.set_sync_status(SyncStatus::Synced);
            
            self.entries.insert(path, file_state);
            loaded_count += 1;
        }
        
        info!("Loaded {} entries from file index database", loaded_count);
        Ok(())
    }
    
    /// Initialize index from existing directory structure
    pub async fn initialize_from_directory(&self, root_path: &Path) -> Result<(), ConsoleError> {
        info!("Initializing file index from directory: {}", root_path.display());
        
        self.scan_directory_recursive(root_path, root_path).await?;
        
        // Persist the initial scan if database is available
        if self.db.is_some() {
            self.persist_all_entries().await?;
        }
        
        let entry_count = self.entries.len();
        info!("File index initialized with {} entries", entry_count);
        
        Ok(())
    }
    
    /// Recursively scan directory and add entries to index
    fn scan_directory_recursive<'a>(&'a self, current_path: &'a Path, root_path: &'a Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ConsoleError>> + Send + 'a>> {
        Box::pin(async move {
        let mut entries = fs::read_dir(current_path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;
            
            // Calculate relative path from root
            let relative_path = path.strip_prefix(root_path)
                .map_err(|_| ConsoleError::Index("Invalid path prefix".to_string()))?;
            
            if metadata.is_dir() {
                // Add directory entry
                let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                let file_state = Arc::new(FileState::new(0, 0, modified, true));
                file_state.set_sync_status(SyncStatus::New);
                
                self.entries.insert(relative_path.to_path_buf(), file_state);
                
                // Recursively scan subdirectory
                self.scan_directory_recursive(&path, root_path).await?;
            } else {
                // Add file entry with hash calculation
                let size = metadata.len();
                let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                
                // Calculate hash asynchronously
                let hash = self.calculate_file_hash(&path).await?;
                
                let file_state = Arc::new(FileState::new(hash, size, modified, false));
                file_state.set_sync_status(SyncStatus::New);
                
                self.entries.insert(relative_path.to_path_buf(), file_state);
            }
        }
        
        Ok(())
        })
    }
    
    /// Calculate xxHash3 for a file
    async fn calculate_file_hash(&self, file_path: &Path) -> Result<u64, ConsoleError> {
        let content = fs::read(file_path).await?;
        Ok(xxh3_64(&content))
    }
    
    /// Add or update a file in the index
    pub async fn add_file(&self, path: &Path, full_path: &Path) -> Result<bool, ConsoleError> {
        let metadata = fs::metadata(full_path).await?;
        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        
        let (hash, size) = if metadata.is_dir() {
            (0, 0) // Directories don't have content hash
        } else {
            let hash = self.calculate_file_hash(full_path).await?;
            (hash, metadata.len())
        };
        
        let changed = if let Some(existing) = self.entries.get(path) {
            // Update existing entry
            existing.update(hash, size, modified)
        } else {
            // Add new entry
            let file_state = Arc::new(FileState::new(hash, size, modified, metadata.is_dir()));
            file_state.set_sync_status(SyncStatus::New);
            self.entries.insert(path.to_path_buf(), file_state);
            true
        };
        
        if changed {
            debug!("File index updated: {} (hash: {:016x})", path.display(), hash);
            
            // Persist to database if available
            if self.db.is_some() {
                self.persist_entry(path).await?;
            }
        }
        
        Ok(changed)
    }
    
    /// Remove a file from the index
    pub async fn remove_file(&self, path: &Path) -> Result<bool, ConsoleError> {
        let removed = self.entries.remove(path).is_some();
        
        if removed {
            debug!("File removed from index: {}", path.display());
            
            // Remove from database if available
            if let Some(ref db) = self.db {
                let cf_files = db.cf_handle("files")
                    .ok_or_else(|| ConsoleError::Index("Files column family not found".to_string()))?;
                
                let path_string = path.to_string_lossy().to_string();
                let key = path_string.as_bytes();
                db.delete_cf(&cf_files, key)
                    .map_err(|e| ConsoleError::Index(format!("Database delete error: {}", e)))?;
            }
        }
        
        Ok(removed)
    }
    
    /// Mark a file as deleted (for sync purposes)
    pub fn mark_deleted(&self, path: &Path) -> bool {
        if let Some(entry) = self.entries.get(path) {
            entry.set_sync_status(SyncStatus::Deleted);
            debug!("File marked as deleted: {}", path.display());
            true
        } else {
            false
        }
    }
    
    /// Mark a file as synced
    pub fn mark_synced(&self, path: &Path) -> bool {
        if let Some(entry) = self.entries.get(path) {
            entry.set_sync_status(SyncStatus::Synced);
            debug!("File marked as synced: {}", path.display());
            true
        } else {
            false
        }
    }
    
    /// Mark a file as modified
    pub fn mark_modified(&self, path: &Path) -> bool {
        if let Some(entry) = self.entries.get(path) {
            entry.set_sync_status(SyncStatus::Modified);
            debug!("File marked as modified: {}", path.display());
            true
        } else {
            false
        }
    }
    
    /// Remove a directory and all its children from the index
    pub fn remove_directory(&self, dir_path: &Path) {
        let mut to_remove = Vec::new();
        
        // Find all entries that are children of this directory
        for entry in self.entries.iter() {
            let path = entry.key();
            if path.starts_with(dir_path) {
                to_remove.push(path.clone());
            }
        }
        
        // Remove all found entries
        for path in to_remove {
            self.entries.remove(&path);
            debug!("Removed from index (directory deletion): {}", path.display());
        }
        
        debug!("Removed directory and children from index: {}", dir_path.display());
    }
    
    /// Save the current index state (placeholder for persistence)
    pub async fn save_state(&self) -> Result<(), crate::ConsoleError> {
        // For now, this is a no-op since we're using in-memory storage
        // In the future, this could save to a local database or file
        debug!("Index state saved (placeholder)");
        Ok(())
    }
    
    /// Get file state for a path
    pub fn get_file_state(&self, path: &Path) -> Option<Arc<FileState>> {
        self.entries.get(path).map(|entry| entry.clone())
    }
    
    /// Get all files with a specific sync status
    pub fn get_files_by_status(&self, status: SyncStatus) -> Vec<PathBuf> {
        self.entries
            .iter()
            .filter_map(|entry| {
                let (path, file_state) = entry.pair();
                if file_state.get_sync_status() == status {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// Get all modified files (status != Synced)
    pub fn get_modified_files(&self) -> Vec<PathBuf> {
        self.entries
            .iter()
            .filter_map(|entry| {
                let (path, file_state) = entry.pair();
                if file_state.get_sync_status() != SyncStatus::Synced {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// Get total number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    /// Get all file paths
    pub fn get_all_paths(&self) -> Vec<PathBuf> {
        self.entries.iter().map(|entry| entry.key().clone()).collect()
    }
    
    /// Persist a single entry to database
    async fn persist_entry(&self, path: &Path) -> Result<(), ConsoleError> {
        let Some(ref db) = self.db else {
            return Ok(());
        };
        
        let Some(file_state) = self.entries.get(path) else {
            return Ok(());
        };
        
        let cf_files = db.cf_handle("files")
            .ok_or_else(|| ConsoleError::Index("Files column family not found".to_string()))?;
        
        // Convert to storage format
        let metadata = StorageFileMetadata {
            path: path.to_string_lossy().to_string(),
            name: path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string(),
            size: file_state.get_size(),
            modified: SerializableSystemTime::from(file_state.get_modified()),
            is_directory: file_state.is_directory,
            xxhash3: file_state.get_hash(),
            storage_path: None,
        };
        
        // Serialize and compress
        let serialized = bitcode::encode(&metadata);
        let compressed = if self.config.enable_compression {
            snap::raw::Encoder::new()
                .compress_vec(&serialized)
                .map_err(|e| ConsoleError::Index(format!("Compression error: {}", e)))?
        } else {
            serialized
        };
        
        let path_string = path.to_string_lossy().to_string();
        let key = path_string.as_bytes();
        db.put_cf(&cf_files, key, compressed)
            .map_err(|e| ConsoleError::Index(format!("Database write error: {}", e)))?;
        
        Ok(())
    }
    
    /// Persist all entries to database
    async fn persist_all_entries(&self) -> Result<(), ConsoleError> {
        let Some(ref db) = self.db else {
            return Ok(());
        };
        
        let cf_files = db.cf_handle("files")
            .ok_or_else(|| ConsoleError::Index("Files column family not found".to_string()))?;
        
        let mut batch = rocksdb::WriteBatch::default();
        let mut batch_count = 0;
        
        for entry in self.entries.iter() {
            let (path, file_state) = entry.pair();
            
            // Convert to storage format
            let metadata = StorageFileMetadata {
                path: path.to_string_lossy().to_string(),
                name: path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string(),
                size: file_state.get_size(),
                modified: SerializableSystemTime::from(file_state.get_modified()),
                is_directory: file_state.is_directory,
                xxhash3: file_state.get_hash(),
                storage_path: None,
            };
            
            // Serialize and compress
            let serialized = bitcode::encode(&metadata);
            let compressed = if self.config.enable_compression {
                snap::raw::Encoder::new()
                    .compress_vec(&serialized)
                    .map_err(|e| ConsoleError::Index(format!("Compression error: {}", e)))?
            } else {
                serialized
            };
            
            let path_string = path.to_string_lossy().to_string();
            let key = path_string.as_bytes();
            batch.put_cf(&cf_files, key, compressed);
            batch_count += 1;
        }
        
        if batch_count > 0 {
            db.write(batch)
                .map_err(|e| ConsoleError::Index(format!("Batch write error: {}", e)))?;
            
            info!("Persisted {} entries to database", batch_count);
        }
        
        Ok(())
    }
    
    /// Flush all pending changes to database
    pub async fn flush(&self) -> Result<(), ConsoleError> {
        if let Some(ref db) = self.db {
            db.flush()
                .map_err(|e| ConsoleError::Index(format!("Database flush error: {}", e)))?;
        }
        Ok(())
    }
    
    /// Get statistics about the index
    pub fn get_stats(&self) -> IndexStats {
        let mut stats = IndexStats::default();
        
        for entry in self.entries.iter() {
            let file_state = entry.value();
            stats.total_files += 1;
            
            if file_state.is_directory {
                stats.directories += 1;
            } else {
                stats.files += 1;
                stats.total_size += file_state.get_size();
            }
            
            match file_state.get_sync_status() {
                SyncStatus::Synced => stats.synced += 1,
                SyncStatus::Modified => stats.modified += 1,
                SyncStatus::Deleted => stats.deleted += 1,
                SyncStatus::New => stats.new += 1,
            }
        }
        
        stats
    }
}

/// Statistics about the file index
#[derive(Debug, Default, Clone)]
pub struct IndexStats {
    pub total_files: usize,
    pub files: usize,
    pub directories: usize,
    pub total_size: u64,
    pub synced: usize,
    pub modified: usize,
    pub deleted: usize,
    pub new: usize,
}

impl IndexStats {
    /// Get the number of files that need synchronization
    pub fn needs_sync(&self) -> usize {
        self.modified + self.deleted + self.new
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;
    
    async fn create_test_file(path: &Path, content: &str) -> Result<(), std::io::Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, content).await
    }
    
    #[tokio::test]
    async fn test_file_index_creation() {
        let config = FileIndexConfig::default();
        let index = FileIndex::new(config).await.unwrap();
        
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }
    
    #[tokio::test]
    async fn test_file_index_with_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_index.db");
        
        let config = FileIndexConfig {
            persistence_path: Some(db_path.clone()),
            enable_compression: true,
        };
        
        let index = FileIndex::new(config).await.unwrap();
        assert!(index.db.is_some());
    }
    
    #[tokio::test]
    async fn test_initialize_from_directory() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();
        
        // Create test files
        create_test_file(&root_path.join("file1.txt"), "content1").await.unwrap();
        create_test_file(&root_path.join("dir1/file2.txt"), "content2").await.unwrap();
        create_test_file(&root_path.join("dir1/dir2/file3.txt"), "content3").await.unwrap();
        
        let config = FileIndexConfig::default();
        let index = FileIndex::new(config).await.unwrap();
        
        index.initialize_from_directory(root_path).await.unwrap();
        
        // Should have 3 files + 2 directories = 5 entries
        assert_eq!(index.len(), 5);
        
        // Check specific files exist
        assert!(index.get_file_state(&PathBuf::from("file1.txt")).is_some());
        assert!(index.get_file_state(&PathBuf::from("dir1/file2.txt")).is_some());
        assert!(index.get_file_state(&PathBuf::from("dir1/dir2/file3.txt")).is_some());
        
        // Check directories exist
        assert!(index.get_file_state(&PathBuf::from("dir1")).is_some());
        assert!(index.get_file_state(&PathBuf::from("dir1/dir2")).is_some());
    }
    
    #[tokio::test]
    async fn test_add_and_update_file() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();
        let file_path = root_path.join("test.txt");
        
        // Create initial file
        create_test_file(&file_path, "initial content").await.unwrap();
        
        let config = FileIndexConfig::default();
        let index = FileIndex::new(config).await.unwrap();
        
        // Add file to index
        let relative_path = PathBuf::from("test.txt");
        let changed = index.add_file(&relative_path, &file_path).await.unwrap();
        assert!(changed);
        assert_eq!(index.len(), 1);
        
        let file_state = index.get_file_state(&relative_path).unwrap();
        assert_eq!(file_state.get_sync_status(), SyncStatus::New);
        assert!(!file_state.is_directory);
        
        // Update file content
        fs::write(&file_path, "updated content").await.unwrap();
        let changed = index.add_file(&relative_path, &file_path).await.unwrap();
        assert!(changed);
        
        let file_state = index.get_file_state(&relative_path).unwrap();
        assert_eq!(file_state.get_sync_status(), SyncStatus::Modified);
    }
    
    #[tokio::test]
    async fn test_remove_file() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();
        let file_path = root_path.join("test.txt");
        
        create_test_file(&file_path, "content").await.unwrap();
        
        let config = FileIndexConfig::default();
        let index = FileIndex::new(config).await.unwrap();
        
        let relative_path = PathBuf::from("test.txt");
        index.add_file(&relative_path, &file_path).await.unwrap();
        assert_eq!(index.len(), 1);
        
        let removed = index.remove_file(&relative_path).await.unwrap();
        assert!(removed);
        assert_eq!(index.len(), 0);
    }
    
    #[tokio::test]
    async fn test_sync_status_operations() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();
        let file_path = root_path.join("test.txt");
        
        create_test_file(&file_path, "content").await.unwrap();
        
        let config = FileIndexConfig::default();
        let index = FileIndex::new(config).await.unwrap();
        
        let relative_path = PathBuf::from("test.txt");
        index.add_file(&relative_path, &file_path).await.unwrap();
        
        // Test marking as synced
        let marked = index.mark_synced(&relative_path);
        assert!(marked);
        
        let file_state = index.get_file_state(&relative_path).unwrap();
        assert_eq!(file_state.get_sync_status(), SyncStatus::Synced);
        
        // Test marking as deleted
        let marked = index.mark_deleted(&relative_path);
        assert!(marked);
        
        let file_state = index.get_file_state(&relative_path).unwrap();
        assert_eq!(file_state.get_sync_status(), SyncStatus::Deleted);
    }
    
    #[tokio::test]
    async fn test_get_files_by_status() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();
        
        // Create test files
        create_test_file(&root_path.join("new.txt"), "new").await.unwrap();
        create_test_file(&root_path.join("modified.txt"), "modified").await.unwrap();
        create_test_file(&root_path.join("synced.txt"), "synced").await.unwrap();
        
        let config = FileIndexConfig::default();
        let index = FileIndex::new(config).await.unwrap();
        
        // Add files
        index.add_file(&PathBuf::from("new.txt"), &root_path.join("new.txt")).await.unwrap();
        index.add_file(&PathBuf::from("modified.txt"), &root_path.join("modified.txt")).await.unwrap();
        index.add_file(&PathBuf::from("synced.txt"), &root_path.join("synced.txt")).await.unwrap();
        
        // Set different statuses
        index.mark_synced(&PathBuf::from("synced.txt"));
        
        // Update modified file to trigger modified status
        fs::write(&root_path.join("modified.txt"), "updated").await.unwrap();
        index.add_file(&PathBuf::from("modified.txt"), &root_path.join("modified.txt")).await.unwrap();
        
        // Test filtering by status
        let new_files = index.get_files_by_status(SyncStatus::New);
        assert_eq!(new_files.len(), 1);
        assert!(new_files.contains(&PathBuf::from("new.txt")));
        
        let modified_files = index.get_files_by_status(SyncStatus::Modified);
        assert_eq!(modified_files.len(), 1);
        assert!(modified_files.contains(&PathBuf::from("modified.txt")));
        
        let synced_files = index.get_files_by_status(SyncStatus::Synced);
        assert_eq!(synced_files.len(), 1);
        assert!(synced_files.contains(&PathBuf::from("synced.txt")));
        
        let modified_all = index.get_modified_files();
        assert_eq!(modified_all.len(), 2); // new + modified
    }
    
    #[tokio::test]
    async fn test_persistence_and_reload() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_index.db");
        let root_path = temp_dir.path().join("files");
        
        // Create test file
        create_test_file(&root_path.join("test.txt"), "content").await.unwrap();
        
        // Create index with persistence
        let config = FileIndexConfig {
            persistence_path: Some(db_path.clone()),
            enable_compression: true,
        };
        
        {
            let index = FileIndex::new(config.clone()).await.unwrap();
            index.add_file(&PathBuf::from("test.txt"), &root_path.join("test.txt")).await.unwrap();
            index.mark_synced(&PathBuf::from("test.txt"));
            index.flush().await.unwrap();
            assert_eq!(index.len(), 1);
        }
        
        // Create new index instance and verify data is loaded
        {
            let index = FileIndex::new(config).await.unwrap();
            assert_eq!(index.len(), 1);
            
            let file_state = index.get_file_state(&PathBuf::from("test.txt")).unwrap();
            assert_eq!(file_state.get_sync_status(), SyncStatus::Synced);
            assert!(!file_state.is_directory);
        }
    }
    
    #[tokio::test]
    async fn test_concurrent_operations() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();
        
        let config = FileIndexConfig::default();
        let index = Arc::new(FileIndex::new(config).await.unwrap());
        
        // Create multiple files concurrently
        let mut handles = Vec::new();
        
        for i in 0..10 {
            let file_path = root_path.join(format!("file{}.txt", i));
            create_test_file(&file_path, &format!("content{}", i)).await.unwrap();
            
            let index_clone = Arc::clone(&index);
            let relative_path = PathBuf::from(format!("file{}.txt", i));
            
            let handle = tokio::spawn(async move {
                index_clone.add_file(&relative_path, &file_path).await.unwrap();
                index_clone.mark_synced(&relative_path);
            });
            
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        assert_eq!(index.len(), 10);
        
        let synced_files = index.get_files_by_status(SyncStatus::Synced);
        assert_eq!(synced_files.len(), 10);
    }
    
    #[tokio::test]
    async fn test_index_stats() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();
        
        // Create test files and directories
        create_test_file(&root_path.join("file1.txt"), "content1").await.unwrap();
        create_test_file(&root_path.join("dir1/file2.txt"), "content2").await.unwrap();
        
        let config = FileIndexConfig::default();
        let index = FileIndex::new(config).await.unwrap();
        
        index.initialize_from_directory(root_path).await.unwrap();
        
        let stats = index.get_stats();
        assert_eq!(stats.total_files, 3); // 2 files + 1 directory
        assert_eq!(stats.files, 2);
        assert_eq!(stats.directories, 1);
        assert_eq!(stats.new, 3); // All files are new initially
        assert_eq!(stats.needs_sync(), 3);
        
        // Mark one file as synced
        index.mark_synced(&PathBuf::from("file1.txt"));
        
        let stats = index.get_stats();
        assert_eq!(stats.synced, 1);
        assert_eq!(stats.new, 2);
        assert_eq!(stats.needs_sync(), 2);
    }
}