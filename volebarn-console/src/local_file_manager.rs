//! Local file operations with zero-copy and lock-free patterns
//! 
//! This module provides async local file management with:
//! - Zero-copy file operations using Bytes
//! - Lock-free state tracking with atomic counters
//! - xxHash3 integrity verification
//! - Atomic directory structure management

use bytes::Bytes;
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;
use volebarn_client::hash::HashManager;
use volebarn_client::types::FileMetadata;

use crate::error::ConsoleError;

/// Statistics for local file operations
#[derive(Debug, Default)]
pub struct LocalFileStats {
    /// Number of files read
    pub files_read: AtomicU64,
    /// Number of files written
    pub files_written: AtomicU64,
    /// Number of files deleted
    pub files_deleted: AtomicU64,
    /// Number of directories created
    pub directories_created: AtomicU64,
    /// Number of directories deleted
    pub directories_deleted: AtomicU64,
    /// Total bytes read
    pub bytes_read: AtomicU64,
    /// Total bytes written
    pub bytes_written: AtomicU64,
    /// Number of hash calculations performed
    pub hash_calculations: AtomicU64,
}

/// Progress tracking for ongoing operations
#[derive(Debug)]
pub struct OperationProgress {
    /// Total bytes to process
    pub total_bytes: AtomicU64,
    /// Bytes processed so far
    pub processed_bytes: AtomicU64,
    /// Number of files to process
    pub total_files: AtomicUsize,
    /// Number of files processed
    pub processed_files: AtomicUsize,
}

impl OperationProgress {
    pub fn new(total_bytes: u64, total_files: usize) -> Self {
        Self {
            total_bytes: AtomicU64::new(total_bytes),
            processed_bytes: AtomicU64::new(0),
            total_files: AtomicUsize::new(total_files),
            processed_files: AtomicUsize::new(0),
        }
    }
    
    pub fn get_progress(&self) -> (u64, u64, usize, usize) {
        (
            self.processed_bytes.load(Ordering::Relaxed),
            self.total_bytes.load(Ordering::Relaxed),
            self.processed_files.load(Ordering::Relaxed),
            self.total_files.load(Ordering::Relaxed),
        )
    }
}

/// Local file manager with zero-copy operations and lock-free state tracking
#[derive(Debug)]
pub struct LocalFileManager {
    /// Root directory for local file operations
    root_path: PathBuf,
    /// Hash manager for integrity verification
    hash_manager: HashManager,
    /// Operation statistics
    stats: LocalFileStats,
    /// Active operation progress tracking
    active_operations: DashMap<String, OperationProgress>,
    /// Temporary directory for atomic operations
    temp_dir: PathBuf,
}

impl LocalFileManager {
    /// Create a new local file manager
    pub async fn new<P: AsRef<Path>>(root_path: P) -> Result<Self, ConsoleError> {
        let root_path = root_path.as_ref().to_path_buf();
        let temp_dir = root_path.join(".volebarn_temp");
        
        // Ensure root directory exists
        fs::create_dir_all(&root_path).await?;
        
        // Ensure temp directory exists
        fs::create_dir_all(&temp_dir).await?;
        
        Ok(Self {
            root_path,
            hash_manager: HashManager::new(),
            stats: LocalFileStats::default(),
            active_operations: DashMap::new(),
            temp_dir,
        })
    }
    
    /// Get the full path for a relative path
    fn get_full_path(&self, relative_path: &str) -> PathBuf {
        // Normalize path separators and remove leading slash
        let normalized = relative_path.trim_start_matches('/').replace('\\', "/");
        self.root_path.join(normalized)
    }
    
    /// Read a file and return its content as Bytes (zero-copy)
    pub async fn read_file(&self, path: &str) -> Result<Bytes, ConsoleError> {
        let full_path = self.get_full_path(path);
        
        // Read file content
        let content = fs::read(&full_path).await?;
        let bytes = Bytes::from(content);
        
        // Update statistics
        self.stats.files_read.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_read.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        
        Ok(bytes)
    }
    
    /// Write file content atomically using temp file + rename
    pub async fn write_file(&self, path: &str, content: Bytes) -> Result<(), ConsoleError> {
        let full_path = self.get_full_path(path);
        
        // Ensure parent directory exists
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        // Write to temporary file first for atomic operation
        let temp_file = self.temp_dir.join(format!("{}.tmp", Uuid::new_v4()));
        let mut file = fs::File::create(&temp_file).await?;
        file.write_all(&content).await?;
        file.sync_all().await?;
        drop(file);
        
        // Atomic rename to final location
        fs::rename(&temp_file, &full_path).await?;
        
        // Update statistics
        self.stats.files_written.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(content.len() as u64, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// Delete a file
    pub async fn delete_file(&self, path: &str) -> Result<(), ConsoleError> {
        let full_path = self.get_full_path(path);
        
        // Check if file exists before attempting deletion
        if !full_path.exists() {
            return Err(ConsoleError::FileSystem(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("File not found: {}", path),
            )));
        }
        
        fs::remove_file(&full_path).await?;
        
        // Update statistics
        self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// Calculate hash for a file
    pub async fn calculate_file_hash(&self, path: &str) -> Result<u64, ConsoleError> {
        let content = self.read_file(path).await?;
        let hash = self.hash_manager.calculate_hash_bytes(&content);
        
        // Update statistics
        self.stats.hash_calculations.fetch_add(1, Ordering::Relaxed);
        
        Ok(hash)
    }
    
    /// Get file metadata including hash
    pub async fn get_file_metadata(&self, path: &str) -> Result<FileMetadata, ConsoleError> {
        let full_path = self.get_full_path(path);
        let metadata = fs::metadata(&full_path).await?;
        
        let (size, modified, is_directory, xxhash3) = if metadata.is_dir() {
            (0, metadata.modified()?, true, 0)
        } else {
            let size = metadata.len();
            let modified = metadata.modified()?;
            let hash = self.calculate_file_hash(path).await?;
            (size, modified, false, hash)
        };
        
        let name = Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(path)
            .to_string();
        
        Ok(FileMetadata {
            path: path.to_string(),
            name,
            size,
            modified,
            is_directory,
            xxhash3,
            storage_path: Some(full_path.to_string_lossy().to_string()),
        })
    }
    
    /// Create a directory and all parent directories
    pub async fn create_directory(&self, path: &str) -> Result<(), ConsoleError> {
        let full_path = self.get_full_path(path);
        fs::create_dir_all(&full_path).await?;
        
        // Update statistics
        self.stats.directories_created.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// Delete a directory recursively
    pub async fn delete_directory(&self, path: &str) -> Result<(), ConsoleError> {
        let full_path = self.get_full_path(path);
        
        // Check if directory exists
        if !full_path.exists() {
            return Err(ConsoleError::FileSystem(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Directory not found: {}", path),
            )));
        }
        
        fs::remove_dir_all(&full_path).await?;
        
        // Update statistics
        self.stats.directories_deleted.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// List directory contents
    pub async fn list_directory(&self, path: &str) -> Result<Vec<FileMetadata>, ConsoleError> {
        let full_path = self.get_full_path(path);
        let mut entries = fs::read_dir(&full_path).await?;
        let mut results = Vec::new();
        
        while let Some(entry) = entries.next_entry().await? {
            let entry_path = entry.path();
            let relative_path = entry_path
                .strip_prefix(&self.root_path)
                .map_err(|_| ConsoleError::FileSystem(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid path prefix",
                )))?;
            
            let path_str = relative_path.to_string_lossy().to_string();
            
            // Get metadata for this entry
            match self.get_file_metadata(&path_str).await {
                Ok(metadata) => results.push(metadata),
                Err(e) => {
                    tracing::warn!("Failed to get metadata for {}: {}", path_str, e);
                    continue;
                }
            }
        }
        
        Ok(results)
    }
    
    /// Check if a file or directory exists
    pub async fn exists(&self, path: &str) -> bool {
        let full_path = self.get_full_path(path);
        full_path.exists()
    }
    
    /// Move a file or directory
    pub async fn move_path(&self, from_path: &str, to_path: &str) -> Result<(), ConsoleError> {
        let from_full = self.get_full_path(from_path);
        let to_full = self.get_full_path(to_path);
        
        // Ensure destination parent directory exists
        if let Some(parent) = to_full.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        fs::rename(&from_full, &to_full).await?;
        
        Ok(())
    }
    
    /// Copy a file
    pub async fn copy_file(&self, from_path: &str, to_path: &str) -> Result<(), ConsoleError> {
        let content = self.read_file(from_path).await?;
        self.write_file(to_path, content).await?;
        
        Ok(())
    }
    
    /// Verify file integrity against expected hash
    pub async fn verify_file_integrity(&self, path: &str, expected_hash: u64) -> Result<bool, ConsoleError> {
        let content = self.read_file(path).await?;
        Ok(self.hash_manager.verify_hash_bytes(&content, expected_hash))
    }
    
    /// Start tracking progress for a bulk operation
    pub fn start_operation_tracking(&self, operation_id: String, total_bytes: u64, total_files: usize) {
        let progress = OperationProgress::new(total_bytes, total_files);
        self.active_operations.insert(operation_id, progress);
    }
    
    /// Update progress for an operation
    pub fn update_operation_progress(&self, operation_id: &str, bytes_processed: u64, files_processed: usize) {
        if let Some(progress) = self.active_operations.get(operation_id) {
            progress.processed_bytes.store(bytes_processed, Ordering::Relaxed);
            progress.processed_files.store(files_processed, Ordering::Relaxed);
        }
    }
    
    /// Get progress for an operation
    pub fn get_operation_progress(&self, operation_id: &str) -> Option<(u64, u64, usize, usize)> {
        self.active_operations.get(operation_id).map(|progress| progress.get_progress())
    }
    
    /// Finish tracking an operation
    pub fn finish_operation_tracking(&self, operation_id: &str) {
        self.active_operations.remove(operation_id);
    }
    
    /// Get operation statistics
    pub fn get_stats(&self) -> LocalFileStats {
        LocalFileStats {
            files_read: AtomicU64::new(self.stats.files_read.load(Ordering::Relaxed)),
            files_written: AtomicU64::new(self.stats.files_written.load(Ordering::Relaxed)),
            files_deleted: AtomicU64::new(self.stats.files_deleted.load(Ordering::Relaxed)),
            directories_created: AtomicU64::new(self.stats.directories_created.load(Ordering::Relaxed)),
            directories_deleted: AtomicU64::new(self.stats.directories_deleted.load(Ordering::Relaxed)),
            bytes_read: AtomicU64::new(self.stats.bytes_read.load(Ordering::Relaxed)),
            bytes_written: AtomicU64::new(self.stats.bytes_written.load(Ordering::Relaxed)),
            hash_calculations: AtomicU64::new(self.stats.hash_calculations.load(Ordering::Relaxed)),
        }
    }
    
    /// Reset statistics
    pub fn reset_stats(&self) {
        self.stats.files_read.store(0, Ordering::Relaxed);
        self.stats.files_written.store(0, Ordering::Relaxed);
        self.stats.files_deleted.store(0, Ordering::Relaxed);
        self.stats.directories_created.store(0, Ordering::Relaxed);
        self.stats.directories_deleted.store(0, Ordering::Relaxed);
        self.stats.bytes_read.store(0, Ordering::Relaxed);
        self.stats.bytes_written.store(0, Ordering::Relaxed);
        self.stats.hash_calculations.store(0, Ordering::Relaxed);
    }
    
    /// Get the root path
    pub fn root_path(&self) -> &Path {
        &self.root_path
    }
    
    /// Clean up temporary files
    pub async fn cleanup_temp_files(&self) -> Result<(), ConsoleError> {
        let mut entries = fs::read_dir(&self.temp_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Err(e) = fs::remove_file(&path).await {
                    tracing::warn!("Failed to remove temp file {:?}: {}", path, e);
                }
            }
        }
        
        Ok(())
    }
}

impl Clone for LocalFileManager {
    fn clone(&self) -> Self {
        Self {
            root_path: self.root_path.clone(),
            hash_manager: self.hash_manager.clone(),
            stats: LocalFileStats::default(), // New instance gets fresh stats
            active_operations: DashMap::new(), // New instance gets fresh operations
            temp_dir: self.temp_dir.clone(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::task::JoinSet;
    use std::sync::Arc;

    async fn create_test_manager() -> (LocalFileManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let manager = LocalFileManager::new(temp_dir.path()).await.unwrap();
        (manager, temp_dir)
    }

    #[tokio::test]
    async fn test_file_operations() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let test_content = Bytes::from_static(b"Hello, World!");
        let test_path = "test_file.txt";
        
        // Test write
        manager.write_file(test_path, test_content.clone()).await.unwrap();
        
        // Test read
        let read_content = manager.read_file(test_path).await.unwrap();
        assert_eq!(read_content, test_content);
        
        // Test exists
        assert!(manager.exists(test_path).await);
        
        // Test delete
        manager.delete_file(test_path).await.unwrap();
        assert!(!manager.exists(test_path).await);
    }

    #[tokio::test]
    async fn test_directory_operations() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let test_dir = "test_directory";
        let nested_dir = "test_directory/nested";
        
        // Test create directory
        manager.create_directory(test_dir).await.unwrap();
        assert!(manager.exists(test_dir).await);
        
        // Test nested directory creation
        manager.create_directory(nested_dir).await.unwrap();
        assert!(manager.exists(nested_dir).await);
        
        // Test delete directory
        manager.delete_directory(test_dir).await.unwrap();
        assert!(!manager.exists(test_dir).await);
    }

    #[tokio::test]
    async fn test_file_metadata() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let test_content = Bytes::from_static(b"Test content for metadata");
        let test_path = "metadata_test.txt";
        
        // Write file
        manager.write_file(test_path, test_content.clone()).await.unwrap();
        
        // Get metadata
        let metadata = manager.get_file_metadata(test_path).await.unwrap();
        
        assert_eq!(metadata.path, test_path);
        assert_eq!(metadata.name, "metadata_test.txt");
        assert_eq!(metadata.size, test_content.len() as u64);
        assert!(!metadata.is_directory);
        assert_ne!(metadata.xxhash3, 0); // Should have calculated hash
        
        // Verify hash calculation
        let expected_hash = manager.hash_manager.calculate_hash_bytes(&test_content);
        assert_eq!(metadata.xxhash3, expected_hash);
    }

    #[tokio::test]
    async fn test_hash_calculation() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let test_content = Bytes::from_static(b"Hash test content");
        let test_path = "hash_test.txt";
        
        // Write file
        manager.write_file(test_path, test_content.clone()).await.unwrap();
        
        // Calculate hash
        let hash = manager.calculate_file_hash(test_path).await.unwrap();
        
        // Verify hash matches expected
        let expected_hash = manager.hash_manager.calculate_hash_bytes(&test_content);
        assert_eq!(hash, expected_hash);
        
        // Test integrity verification
        assert!(manager.verify_file_integrity(test_path, hash).await.unwrap());
        assert!(!manager.verify_file_integrity(test_path, hash + 1).await.unwrap());
    }

    #[tokio::test]
    async fn test_list_directory() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        // Create test files and directories
        manager.write_file("file1.txt", Bytes::from_static(b"content1")).await.unwrap();
        manager.write_file("file2.txt", Bytes::from_static(b"content2")).await.unwrap();
        manager.create_directory("subdir").await.unwrap();
        manager.write_file("subdir/file3.txt", Bytes::from_static(b"content3")).await.unwrap();
        
        // List root directory
        let entries = manager.list_directory("").await.unwrap();
        
        // Filter out the temp directory that gets created automatically
        let filtered_entries: Vec<_> = entries.iter()
            .filter(|e| e.name != ".volebarn_temp")
            .collect();
        
        // Should have 3 entries: 2 files + 1 directory
        assert_eq!(filtered_entries.len(), 3);
        
        let file_names: Vec<&str> = filtered_entries.iter().map(|e| e.name.as_str()).collect();
        assert!(file_names.contains(&"file1.txt"));
        assert!(file_names.contains(&"file2.txt"));
        assert!(file_names.contains(&"subdir"));
        
        // Check directory entry
        let subdir_entry = filtered_entries.iter().find(|e| e.name == "subdir").unwrap();
        assert!(subdir_entry.is_directory);
        
        // List subdirectory
        let subdir_entries = manager.list_directory("subdir").await.unwrap();
        assert_eq!(subdir_entries.len(), 1);
        assert_eq!(subdir_entries[0].name, "file3.txt");
    }

    #[tokio::test]
    async fn test_move_and_copy() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let original_content = Bytes::from_static(b"Original content");
        let original_path = "original.txt";
        let moved_path = "moved.txt";
        let copied_path = "copied.txt";
        
        // Write original file
        manager.write_file(original_path, original_content.clone()).await.unwrap();
        
        // Test copy
        manager.copy_file(original_path, copied_path).await.unwrap();
        assert!(manager.exists(original_path).await);
        assert!(manager.exists(copied_path).await);
        
        let copied_content = manager.read_file(copied_path).await.unwrap();
        assert_eq!(copied_content, original_content);
        
        // Test move
        manager.move_path(original_path, moved_path).await.unwrap();
        assert!(!manager.exists(original_path).await);
        assert!(manager.exists(moved_path).await);
        
        let moved_content = manager.read_file(moved_path).await.unwrap();
        assert_eq!(moved_content, original_content);
    }

    #[tokio::test]
    async fn test_atomic_write() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let test_path = "atomic_test.txt";
        let content1 = Bytes::from_static(b"First content");
        let content2 = Bytes::from_static(b"Second content");
        
        // Write first content
        manager.write_file(test_path, content1.clone()).await.unwrap();
        let read1 = manager.read_file(test_path).await.unwrap();
        assert_eq!(read1, content1);
        
        // Overwrite with second content
        manager.write_file(test_path, content2.clone()).await.unwrap();
        let read2 = manager.read_file(test_path).await.unwrap();
        assert_eq!(read2, content2);
    }

    #[tokio::test]
    async fn test_nested_path_operations() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let nested_path = "level1/level2/level3/nested_file.txt";
        let content = Bytes::from_static(b"Nested content");
        
        // Write to nested path (should create directories automatically)
        manager.write_file(nested_path, content.clone()).await.unwrap();
        
        // Verify file exists and content is correct
        assert!(manager.exists(nested_path).await);
        let read_content = manager.read_file(nested_path).await.unwrap();
        assert_eq!(read_content, content);
        
        // Verify intermediate directories exist
        assert!(manager.exists("level1").await);
        assert!(manager.exists("level1/level2").await);
        assert!(manager.exists("level1/level2/level3").await);
    }

    #[tokio::test]
    async fn test_statistics_tracking() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let initial_stats = manager.get_stats();
        assert_eq!(initial_stats.files_read.load(Ordering::Relaxed), 0);
        assert_eq!(initial_stats.files_written.load(Ordering::Relaxed), 0);
        
        // Perform some operations
        let content = Bytes::from_static(b"Stats test content");
        manager.write_file("stats_test.txt", content.clone()).await.unwrap();
        manager.read_file("stats_test.txt").await.unwrap();
        manager.calculate_file_hash("stats_test.txt").await.unwrap();
        
        let stats = manager.get_stats();
        assert_eq!(stats.files_written.load(Ordering::Relaxed), 1);
        assert_eq!(stats.files_read.load(Ordering::Relaxed), 2); // read + hash calculation
        assert_eq!(stats.bytes_written.load(Ordering::Relaxed), content.len() as u64);
        assert!(stats.hash_calculations.load(Ordering::Relaxed) >= 1);
        
        // Test reset
        manager.reset_stats();
        let reset_stats = manager.get_stats();
        assert_eq!(reset_stats.files_read.load(Ordering::Relaxed), 0);
        assert_eq!(reset_stats.files_written.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_operation_progress_tracking() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        let operation_id = "test_operation";
        let total_bytes = 1000u64;
        let total_files = 5usize;
        
        // Start tracking
        manager.start_operation_tracking(operation_id.to_string(), total_bytes, total_files);
        
        // Check initial progress
        let (processed_bytes, total_b, processed_files, total_f) = 
            manager.get_operation_progress(operation_id).unwrap();
        assert_eq!(processed_bytes, 0);
        assert_eq!(total_b, total_bytes);
        assert_eq!(processed_files, 0);
        assert_eq!(total_f, total_files);
        
        // Update progress
        manager.update_operation_progress(operation_id, 500, 2);
        let (processed_bytes, _, processed_files, _) = 
            manager.get_operation_progress(operation_id).unwrap();
        assert_eq!(processed_bytes, 500);
        assert_eq!(processed_files, 2);
        
        // Finish tracking
        manager.finish_operation_tracking(operation_id);
        assert!(manager.get_operation_progress(operation_id).is_none());
    }

    #[tokio::test]
    async fn test_concurrent_file_operations() {
        let (manager, _temp_dir) = create_test_manager().await;
        let manager = Arc::new(manager);
        
        let mut join_set = JoinSet::new();
        
        // Spawn multiple concurrent write operations
        for i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            join_set.spawn(async move {
                let content = Bytes::from(format!("Content for file {}", i));
                let path = format!("concurrent_file_{}.txt", i);
                manager_clone.write_file(&path, content).await.unwrap();
                path
            });
        }
        
        // Wait for all writes to complete
        let mut written_files = Vec::new();
        while let Some(result) = join_set.join_next().await {
            written_files.push(result.unwrap());
        }
        
        // Verify all files were written correctly
        assert_eq!(written_files.len(), 10);
        
        // Spawn concurrent read operations
        let mut join_set = JoinSet::new();
        for path in written_files {
            let manager_clone = Arc::clone(&manager);
            join_set.spawn(async move {
                let content = manager_clone.read_file(&path).await.unwrap();
                (path, content)
            });
        }
        
        // Wait for all reads to complete
        let mut read_results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            read_results.push(result.unwrap());
        }
        
        // Verify all reads were successful
        assert_eq!(read_results.len(), 10);
        
        for (path, content) in read_results {
            let expected_content = format!("Content for file {}", 
                path.strip_prefix("concurrent_file_").unwrap().strip_suffix(".txt").unwrap());
            assert_eq!(content, Bytes::from(expected_content));
        }
    }

    #[tokio::test]
    async fn test_concurrent_directory_operations() {
        let (manager, _temp_dir) = create_test_manager().await;
        let manager = Arc::new(manager);
        
        let mut join_set = JoinSet::new();
        
        // Spawn concurrent directory creation operations
        for i in 0..5 {
            let manager_clone = Arc::clone(&manager);
            join_set.spawn(async move {
                let dir_path = format!("concurrent_dir_{}", i);
                manager_clone.create_directory(&dir_path).await.unwrap();
                
                // Create a file in each directory
                let file_path = format!("{}/file.txt", dir_path);
                let content = Bytes::from(format!("Content in directory {}", i));
                manager_clone.write_file(&file_path, content).await.unwrap();
                
                (dir_path, file_path)
            });
        }
        
        // Wait for all operations to complete
        let mut created_items = Vec::new();
        while let Some(result) = join_set.join_next().await {
            created_items.push(result.unwrap());
        }
        
        // Verify all directories and files exist
        for (dir_path, file_path) in created_items {
            assert!(manager.exists(&dir_path).await);
            assert!(manager.exists(&file_path).await);
            
            let content = manager.read_file(&file_path).await.unwrap();
            let expected_content = format!("Content in directory {}", 
                dir_path.strip_prefix("concurrent_dir_").unwrap());
            assert_eq!(content, Bytes::from(expected_content));
        }
    }

    #[tokio::test]
    async fn test_concurrent_hash_calculations() {
        let (manager, _temp_dir) = create_test_manager().await;
        let manager = Arc::new(manager);
        
        // Create test files
        for i in 0..5 {
            let content = Bytes::from(format!("Hash test content {}", i));
            let path = format!("hash_test_{}.txt", i);
            manager.write_file(&path, content).await.unwrap();
        }
        
        let mut join_set = JoinSet::new();
        
        // Spawn concurrent hash calculations
        for i in 0..5 {
            let manager_clone = Arc::clone(&manager);
            join_set.spawn(async move {
                let path = format!("hash_test_{}.txt", i);
                let hash = manager_clone.calculate_file_hash(&path).await.unwrap();
                (path, hash)
            });
        }
        
        // Wait for all hash calculations to complete
        let mut hash_results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            hash_results.push(result.unwrap());
        }
        
        // Verify all hashes were calculated
        assert_eq!(hash_results.len(), 5);
        
        // Verify hashes are consistent
        for (path, hash) in hash_results {
            let content = manager.read_file(&path).await.unwrap();
            let expected_hash = manager.hash_manager.calculate_hash_bytes(&content);
            assert_eq!(hash, expected_hash);
        }
    }

    #[tokio::test]
    async fn test_error_handling() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        // Test reading non-existent file
        let result = manager.read_file("non_existent.txt").await;
        assert!(result.is_err());
        
        // Test deleting non-existent file
        let result = manager.delete_file("non_existent.txt").await;
        assert!(result.is_err());
        
        // Test deleting non-existent directory
        let result = manager.delete_directory("non_existent_dir").await;
        assert!(result.is_err());
        
        // Test getting metadata for non-existent file
        let result = manager.get_file_metadata("non_existent.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cleanup_temp_files() {
        let (manager, _temp_dir) = create_test_manager().await;
        
        // Create some temporary files manually
        let temp_file1 = manager.temp_dir.join("temp1.tmp");
        let temp_file2 = manager.temp_dir.join("temp2.tmp");
        
        tokio::fs::write(&temp_file1, b"temp content 1").await.unwrap();
        tokio::fs::write(&temp_file2, b"temp content 2").await.unwrap();
        
        // Verify temp files exist
        assert!(temp_file1.exists());
        assert!(temp_file2.exists());
        
        // Clean up temp files
        manager.cleanup_temp_files().await.unwrap();
        
        // Verify temp files are gone
        assert!(!temp_file1.exists());
        assert!(!temp_file2.exists());
    }
}