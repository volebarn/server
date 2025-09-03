//! Storage layer for Volebarn server
//! 
//! This module provides persistent metadata storage using RocksDB with lock-free operations.
//! Features content-addressable file storage, atomic operations, and hierarchical directory support.
//! Uses bitcode serialization with Snappy compression for optimal performance.

use crate::error::{ServerError, ServerResult};
use crate::hash::{hash_bytes, HashManager};
use crate::types::FileMetadata;
use bytes::Bytes;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Wrapper for SystemTime that supports bitcode serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
struct BitcodeSystemTime(u64);

impl From<SystemTime> for BitcodeSystemTime {
    fn from(time: SystemTime) -> Self {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
        Self(duration.as_secs())
    }
}

impl From<BitcodeSystemTime> for SystemTime {
    fn from(time: BitcodeSystemTime) -> Self {
        UNIX_EPOCH + std::time::Duration::from_secs(time.0)
    }
}

/// Column family names for RocksDB
const CF_FILES: &str = "files";
const CF_DIRECTORIES: &str = "directories";
const CF_HASH_INDEX: &str = "hash_index";
const CF_MODIFIED_INDEX: &str = "modified_index";

/// Storage-specific FileMetadata with bitcode-compatible types
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
struct StorageFileMetadata {
    path: String,
    name: String,
    size: u64,
    modified: BitcodeSystemTime,
    is_directory: bool,
    xxhash3: u64,
    storage_path: Option<String>,
}

impl From<FileMetadata> for StorageFileMetadata {
    fn from(metadata: FileMetadata) -> Self {
        Self {
            path: metadata.path,
            name: metadata.name,
            size: metadata.size,
            modified: metadata.modified.into(),
            is_directory: metadata.is_directory,
            xxhash3: metadata.xxhash3,
            storage_path: metadata.storage_path,
        }
    }
}

impl From<StorageFileMetadata> for FileMetadata {
    fn from(storage: StorageFileMetadata) -> Self {
        Self {
            path: storage.path,
            name: storage.name,
            size: storage.size,
            modified: storage.modified.into(),
            is_directory: storage.is_directory,
            xxhash3: storage.xxhash3,
            storage_path: storage.storage_path,
        }
    }
}

/// Serialize data using bitcode with Snappy compression
fn serialize_compressed<T: bitcode::Encode>(data: &T) -> ServerResult<Vec<u8>> {
    let encoded = bitcode::encode(data);
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&encoded)
        .map_err(|e| ServerError::Serialization {
            operation: "snappy_compress".to_string(),
            error: e.to_string(),
        })?;
    Ok(compressed)
}

/// Deserialize data using bitcode with Snappy decompression
fn deserialize_compressed<T: for<'a> bitcode::Decode<'a>>(data: &[u8]) -> ServerResult<T> {
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(data)
        .map_err(|e| ServerError::Deserialization {
            operation: "snappy_decompress".to_string(),
            error: e.to_string(),
        })?;
    
    bitcode::decode(&decompressed).map_err(|e| ServerError::Deserialization {
        operation: "bitcode_decode".to_string(),
        error: e.to_string(),
    })
}

/// Directory metadata for hierarchical support
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
struct DirectoryMetadata {
    path: String,
    name: String,
    created: BitcodeSystemTime,
    modified: BitcodeSystemTime,
    child_count: u64,
}

/// Async RocksDB metadata storage system with lock-free operations
#[derive(Debug)]
pub struct MetadataStore {
    db: Arc<DB>,
    _db_path: PathBuf,
}

impl MetadataStore {
    /// Create a new MetadataStore with RocksDB backend
    pub async fn new<P: AsRef<Path>>(db_path: P) -> ServerResult<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        
        // Ensure the database directory exists
        if let Some(parent) = db_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ServerError::Storage {
                    path: parent.to_string_lossy().to_string(),
                    error: format!("Failed to create database directory: {}", e),
                }
            })?;
        }

        // Configure RocksDB options for optimal performance
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_background_jobs(4);
        db_opts.set_bytes_per_sync(1048576); // 1MB
        db_opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        db_opts.set_max_write_buffer_number(3);
        db_opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB

        // Define column families
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_FILES, Options::default()),
            ColumnFamilyDescriptor::new(CF_DIRECTORIES, Options::default()),
            ColumnFamilyDescriptor::new(CF_HASH_INDEX, Options::default()),
            ColumnFamilyDescriptor::new(CF_MODIFIED_INDEX, Options::default()),
        ];

        // Open database with column families
        let db = DB::open_cf_descriptors(&db_opts, &db_path, cf_descriptors).map_err(|e| {
            ServerError::Database {
                operation: "open".to_string(),
                error: e.to_string(),
            }
        })?;

        Ok(Self {
            db: Arc::new(db),
            _db_path: db_path,
        })
    }

    /// Get column family handle
    fn cf_handle(&self, cf_name: &str) -> ServerResult<&ColumnFamily> {
        self.db.cf_handle(cf_name).ok_or_else(|| {
            ServerError::Database {
                operation: "get_cf_handle".to_string(),
                error: format!("Column family '{}' not found", cf_name),
            }
        })
    }

    /// Normalize path for consistent storage (remove leading/trailing slashes, handle empty paths)
    fn normalize_path(path: &str) -> String {
        if path.is_empty() || path == "/" {
            return "/".to_string();
        }
        
        let normalized = path.trim_start_matches('/').trim_end_matches('/');
        if normalized.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", normalized)
        }
    }

    /// Create a modified time index key for time-based queries
    fn create_modified_key(modified: SystemTime, path: &str) -> Vec<u8> {
        let timestamp = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        format!("{:020}{}", timestamp, path).into_bytes()
    }

    /// Get file metadata by path
    pub async fn get_file_metadata(&self, path: &str) -> ServerResult<Option<FileMetadata>> {
        let normalized_path = Self::normalize_path(path);
        let cf = self.cf_handle(CF_FILES)?;
        
        let result = self
            .db
            .get_cf(cf, normalized_path.as_bytes())
            .map_err(|e| ServerError::Database {
                operation: "get_file_metadata".to_string(),
                error: e.to_string(),
            })?;

        match result {
            Some(data) => {
                let storage_metadata: StorageFileMetadata = deserialize_compressed(&data)?;
                let metadata: FileMetadata = storage_metadata.into();
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Store file metadata with atomic operations
    pub async fn put_file_metadata(&self, metadata: &FileMetadata) -> ServerResult<()> {
        let normalized_path = Self::normalize_path(&metadata.path);
        let cf_files = self.cf_handle(CF_FILES)?;
        let cf_hash = self.cf_handle(CF_HASH_INDEX)?;
        let cf_modified = self.cf_handle(CF_MODIFIED_INDEX)?;

        // Create normalized metadata with corrected path
        let mut normalized_metadata = metadata.clone();
        normalized_metadata.path = normalized_path.clone();

        // Convert to storage format and serialize using bitcode with Snappy compression
        let storage_metadata: StorageFileMetadata = normalized_metadata.clone().into();
        let data = serialize_compressed(&storage_metadata)?;

        // Create batch for atomic operations
        let mut batch = rocksdb::WriteBatch::default();

        // Store file metadata
        batch.put_cf(cf_files, normalized_path.as_bytes(), &data);

        // Update hash index for deduplication (only for files, not directories)
        // Only add to hash index if no entry exists yet
        if !normalized_metadata.is_directory && normalized_metadata.xxhash3 != 0 {
            let hash_key = normalized_metadata.xxhash3.to_be_bytes();
            if self.db.get_cf(cf_hash, hash_key).map_err(|e| ServerError::Database {
                operation: "check_hash_index".to_string(),
                error: e.to_string(),
            })?.is_none() {
                // No existing entry for this hash, add it
                batch.put_cf(cf_hash, hash_key, normalized_path.as_bytes());
            }
        }

        // Update modified time index for time-based queries
        let modified_key = Self::create_modified_key(normalized_metadata.modified, &normalized_path);
        batch.put_cf(cf_modified, modified_key, b"");

        // Execute atomic batch
        self.db.write(batch).map_err(|e| ServerError::Database {
            operation: "put_file_metadata".to_string(),
            error: e.to_string(),
        })?;

        // Update parent directory metadata if this is a new file
        if let Some(parent_path) = self.get_parent_path(&normalized_path) {
            self.update_directory_child_count(&parent_path, 1).await?;
        }

        Ok(())
    }

    /// Delete file metadata with atomic cleanup
    pub async fn delete_file_metadata(&self, path: &str) -> ServerResult<bool> {
        let normalized_path = Self::normalize_path(path);
        
        // First check if file exists and get its metadata
        let metadata = match self.get_file_metadata(&normalized_path).await? {
            Some(meta) => meta,
            None => return Ok(false), // File doesn't exist
        };

        let cf_files = self.cf_handle(CF_FILES)?;
        let cf_hash = self.cf_handle(CF_HASH_INDEX)?;
        let cf_modified = self.cf_handle(CF_MODIFIED_INDEX)?;

        // Create batch for atomic operations
        let mut batch = rocksdb::WriteBatch::default();

        // Delete file metadata
        batch.delete_cf(cf_files, normalized_path.as_bytes());

        // Handle hash index cleanup for deduplication
        if !metadata.is_directory && metadata.xxhash3 != 0 {
            let hash_key = metadata.xxhash3.to_be_bytes();
            if let Ok(Some(indexed_path)) = self.db.get_cf(cf_hash, hash_key) {
                let indexed_path_str = String::from_utf8_lossy(&indexed_path);
                if indexed_path_str == normalized_path {
                    // This file is the one in the hash index, we need to either remove it
                    // or replace it with another file that has the same hash
                    
                    // First, remove the current entry
                    batch.delete_cf(cf_hash, hash_key);
                    
                    // Try to find another file with the same hash to replace it
                    // We'll do this after the batch is executed
                }
            }
        }

        // Remove from modified time index
        let modified_key = Self::create_modified_key(metadata.modified, &normalized_path);
        batch.delete_cf(cf_modified, modified_key);

        // Execute atomic batch
        self.db.write(batch).map_err(|e| ServerError::Database {
            operation: "delete_file_metadata".to_string(),
            error: e.to_string(),
        })?;

        // After deletion, if we removed a hash index entry, try to find another file
        // with the same hash to maintain the hash index
        if !metadata.is_directory && metadata.xxhash3 != 0 {
            let hash_key = metadata.xxhash3.to_be_bytes();
            if self.db.get_cf(cf_hash, hash_key).map_err(|e| ServerError::Database {
                operation: "check_hash_after_delete".to_string(),
                error: e.to_string(),
            })?.is_none() {
                // Hash index entry was removed, try to find another file with same hash
                if let Some(replacement_path) = self.find_file_with_hash(metadata.xxhash3).await? {
                    // Update hash index with the replacement file
                    self.db.put_cf(cf_hash, hash_key, replacement_path.as_bytes()).map_err(|e| {
                        ServerError::Database {
                            operation: "update_hash_index_after_delete".to_string(),
                            error: e.to_string(),
                        }
                    })?;
                }
            }
        }

        // Update parent directory metadata
        if let Some(parent_path) = self.get_parent_path(&normalized_path) {
            self.update_directory_child_count(&parent_path, -1).await?;
        }

        Ok(true)
    }

    /// List directory contents with hierarchical support
    pub async fn list_directory(&self, path: &str) -> ServerResult<Vec<FileMetadata>> {
        let normalized_path = Self::normalize_path(path);
        let cf = self.cf_handle(CF_FILES)?;
        
        let mut results = Vec::new();
        let prefix = if normalized_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", normalized_path)
        };

        // Iterate over all files with the directory prefix
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, value) = item.map_err(|e| ServerError::Database {
                operation: "list_directory".to_string(),
                error: e.to_string(),
            })?;

            let file_path = String::from_utf8_lossy(&key);
            
            // Check if this file is a direct child of the requested directory
            if self.is_direct_child(&normalized_path, &file_path) {
                let storage_metadata: StorageFileMetadata = deserialize_compressed(&value)?;
                let metadata: FileMetadata = storage_metadata.into();
                results.push(metadata);
            } else if !file_path.starts_with(&prefix) {
                // We've moved beyond files in this directory
                break;
            }
        }

        // Sort results by name for consistent ordering
        results.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(results)
    }

    /// Find file path by hash for deduplication
    pub async fn find_by_hash(&self, hash: u64) -> ServerResult<Option<String>> {
        let cf = self.cf_handle(CF_HASH_INDEX)?;
        let hash_key = hash.to_be_bytes();

        let result = self.db.get_cf(cf, hash_key).map_err(|e| {
            ServerError::Database {
                operation: "find_by_hash".to_string(),
                error: e.to_string(),
            }
        })?;

        match result {
            Some(path_bytes) => {
                let path = String::from_utf8_lossy(&path_bytes).to_string();
                Ok(Some(path))
            }
            None => Ok(None),
        }
    }

    /// Get files modified since a specific timestamp
    pub async fn get_files_modified_since(
        &self,
        timestamp: SystemTime,
    ) -> ServerResult<Vec<String>> {
        let cf = self.cf_handle(CF_MODIFIED_INDEX)?;
        let start_key = Self::create_modified_key(timestamp, "");
        
        let mut results = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| ServerError::Database {
                operation: "get_files_modified_since".to_string(),
                error: e.to_string(),
            })?;

            let key_str = String::from_utf8_lossy(&key);
            // Extract path from the modified index key (skip timestamp prefix)
            if key_str.len() > 20 {
                let path = key_str[20..].to_string();
                results.push(path);
            }
        }

        Ok(results)
    }

    /// Create directory metadata
    pub async fn create_directory(&self, path: &str) -> ServerResult<()> {
        let normalized_path = Self::normalize_path(path);
        
        // Check if directory already exists
        if self.get_file_metadata(&normalized_path).await?.is_some() {
            return Err(ServerError::DirectoryAlreadyExists {
                path: normalized_path,
            });
        }

        let now = SystemTime::now();
        let name = std::path::Path::new(&normalized_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&normalized_path)
            .to_string();

        let metadata = FileMetadata {
            path: normalized_path.clone(),
            name,
            size: 0,
            modified: now,
            is_directory: true,
            xxhash3: 0, // Directories don't have content hash
            storage_path: None,
        };

        self.put_file_metadata(&metadata).await?;

        // Create directory metadata entry
        let dir_metadata = DirectoryMetadata {
            path: normalized_path.clone(),
            name: metadata.name.clone(),
            created: now.into(),
            modified: now.into(),
            child_count: 0,
        };

        let cf_dirs = self.cf_handle(CF_DIRECTORIES)?;
        let data = serialize_compressed(&dir_metadata)?;

        self.db
            .put_cf(cf_dirs, normalized_path.as_bytes(), data)
            .map_err(|e| ServerError::Database {
                operation: "create_directory".to_string(),
                error: e.to_string(),
            })?;

        Ok(())
    }

    /// Delete directory recursively
    pub async fn delete_directory_recursive(&self, path: &str) -> ServerResult<Vec<String>> {
        let normalized_path = Self::normalize_path(path);
        let mut deleted_paths = Vec::new();

        // Get all files in the directory and subdirectories
        let files_to_delete = self.get_all_files_in_directory(&normalized_path).await?;

        // Delete all files first
        for file_path in files_to_delete {
            if self.delete_file_metadata(&file_path).await? {
                deleted_paths.push(file_path);
            }
        }

        // Delete the directory itself
        if self.delete_file_metadata(&normalized_path).await? {
            deleted_paths.push(normalized_path.clone());
        }

        // Remove directory metadata
        let cf_dirs = self.cf_handle(CF_DIRECTORIES)?;
        self.db
            .delete_cf(cf_dirs, normalized_path.as_bytes())
            .map_err(|e| ServerError::Database {
                operation: "delete_directory_recursive".to_string(),
                error: e.to_string(),
            })?;

        Ok(deleted_paths)
    }

    /// Get all files in a directory recursively (public method)
    pub async fn get_all_files_in_directory(&self, path: &str) -> ServerResult<Vec<String>> {
        let cf = self.cf_handle(CF_FILES)?;
        let mut results = Vec::new();
        
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path)
        };

        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, _) = item.map_err(|e| ServerError::Database {
                operation: "get_all_files_in_directory".to_string(),
                error: e.to_string(),
            })?;

            let file_path = String::from_utf8_lossy(&key);
            
            if file_path.starts_with(&prefix) && file_path != path {
                results.push(file_path.to_string());
            } else if !file_path.starts_with(&prefix) {
                break;
            }
        }

        Ok(results)
    }

    /// Check if a file path is a direct child of a directory
    fn is_direct_child(&self, dir_path: &str, file_path: &str) -> bool {
        if dir_path == "/" {
            // Root directory: check if path has no additional slashes after the first one
            let path_without_root = &file_path[1..]; // Remove leading slash
            !path_without_root.contains('/')
        } else {
            let expected_prefix = format!("{}/", dir_path);
            if !file_path.starts_with(&expected_prefix) {
                return false;
            }
            
            let remaining = &file_path[expected_prefix.len()..];
            !remaining.contains('/') // Direct child has no additional slashes
        }
    }

    /// Get parent directory path
    fn get_parent_path(&self, path: &str) -> Option<String> {
        if path == "/" {
            return None;
        }
        
        let path_obj = std::path::Path::new(path);
        path_obj.parent().map(|p| {
            let parent_str = p.to_string_lossy();
            if parent_str.is_empty() || parent_str == "/" {
                "/".to_string()
            } else {
                parent_str.to_string()
            }
        })
    }

    /// Update directory child count (atomic operation)
    async fn update_directory_child_count(&self, dir_path: &str, delta: i64) -> ServerResult<()> {
        let cf_dirs = self.cf_handle(CF_DIRECTORIES)?;
        
        // Get current directory metadata
        let current_data = self.db.get_cf(cf_dirs, dir_path.as_bytes()).map_err(|e| {
            ServerError::Database {
                operation: "update_directory_child_count".to_string(),
                error: e.to_string(),
            }
        })?;

        if let Some(data) = current_data {
            let mut dir_metadata: DirectoryMetadata = deserialize_compressed(&data)?;

            // Update child count (ensure it doesn't go below 0)
            if delta < 0 && dir_metadata.child_count < (-delta) as u64 {
                dir_metadata.child_count = 0;
            } else {
                dir_metadata.child_count = (dir_metadata.child_count as i64 + delta).max(0) as u64;
            }
            
            dir_metadata.modified = SystemTime::now().into();

            // Save updated metadata
            let updated_data = serialize_compressed(&dir_metadata)?;

            self.db
                .put_cf(cf_dirs, dir_path.as_bytes(), updated_data)
                .map_err(|e| ServerError::Database {
                    operation: "update_directory_child_count".to_string(),
                    error: e.to_string(),
                })?;
        }

        Ok(())
    }

    /// Find any file with the given hash (used for hash index maintenance)
    async fn find_file_with_hash(&self, hash: u64) -> ServerResult<Option<String>> {
        let cf = self.cf_handle(CF_FILES)?;
        
        // Iterate through all files to find one with matching hash
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item.map_err(|e| ServerError::Database {
                operation: "find_file_with_hash".to_string(),
                error: e.to_string(),
            })?;
            
            let storage_metadata: StorageFileMetadata = deserialize_compressed(&value)?;
            let metadata: FileMetadata = storage_metadata.into();
            
            if !metadata.is_directory && metadata.xxhash3 == hash {
                return Ok(Some(String::from_utf8_lossy(&key).to_string()));
            }
        }
        
        Ok(None)
    }

    /// Get database statistics for monitoring
    pub async fn get_stats(&self) -> ServerResult<HashMap<String, String>> {
        let mut stats = HashMap::new();
        
        // Get RocksDB statistics
        if let Ok(Some(db_stats)) = self.db.property_value("rocksdb.stats") {
            stats.insert("rocksdb_stats".to_string(), db_stats);
        }
        
        // Count entries in each column family
        for cf_name in [CF_FILES, CF_DIRECTORIES, CF_HASH_INDEX, CF_MODIFIED_INDEX] {
            let cf = self.cf_handle(cf_name)?;
            let mut count = 0u64;
            
            let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            for _ in iter {
                count += 1;
            }
            
            stats.insert(format!("{}_count", cf_name), count.to_string());
        }
        
        Ok(stats)
    }

    /// Compact database for optimal performance
    pub async fn compact(&self) -> ServerResult<()> {
        // Compact all column families
        for cf_name in [CF_FILES, CF_DIRECTORIES, CF_HASH_INDEX, CF_MODIFIED_INDEX] {
            let cf = self.cf_handle(cf_name)?;
            self.db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
        }
        
        Ok(())
    }
}

/// Async file system storage layer with content-addressable storage and deduplication
#[derive(Debug)]
pub struct FileStorage {
    /// Root directory for file storage
    storage_root: PathBuf,
    /// Temporary directory for atomic operations
    temp_dir: PathBuf,
    /// Metadata store for file information and hash index
    metadata_store: Arc<MetadataStore>,
    /// Hash manager for integrity verification
    hash_manager: HashManager,
}

impl FileStorage {
    /// Create a new FileStorage with content-addressable storage
    pub async fn new<P: AsRef<Path>>(
        storage_root: P,
        metadata_store: Arc<MetadataStore>,
    ) -> ServerResult<Self> {
        let storage_root = storage_root.as_ref().to_path_buf();
        let temp_dir = storage_root.join("temp");

        // Create storage directories
        tokio::fs::create_dir_all(&storage_root).await.map_err(|e| {
            ServerError::Storage {
                path: storage_root.to_string_lossy().to_string(),
                error: format!("Failed to create storage root: {}", e),
            }
        })?;

        tokio::fs::create_dir_all(&temp_dir).await.map_err(|e| {
            ServerError::Storage {
                path: temp_dir.to_string_lossy().to_string(),
                error: format!("Failed to create temp directory: {}", e),
            }
        })?;

        Ok(Self {
            storage_root,
            temp_dir,
            metadata_store,
            hash_manager: HashManager::new(),
        })
    }

    /// Store file content using content-addressable storage with deduplication
    pub async fn store_file(&self, path: &str, content: Bytes) -> ServerResult<FileMetadata> {
        // Calculate hash for content-addressable storage and integrity verification
        let hash = hash_bytes(&content);
        let size = content.len() as u64;
        let modified = SystemTime::now();

        // Check if file with same hash already exists (deduplication)
        if let Some(existing_path) = self.metadata_store.find_by_hash(hash).await? {
            // File content already exists, create new metadata entry pointing to same storage
            let existing_metadata = self.metadata_store
                .get_file_metadata(&existing_path)
                .await?
                .ok_or_else(|| ServerError::Storage {
                    path: existing_path.clone(),
                    error: "Existing file metadata not found".to_string(),
                })?;

            let new_metadata = FileMetadata {
                path: path.to_string(),
                name: std::path::Path::new(path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(path)
                    .to_string(),
                size,
                modified,
                is_directory: false,
                xxhash3: hash,
                storage_path: existing_metadata.storage_path.clone(),
            };

            // Store metadata for new path
            self.metadata_store.put_file_metadata(&new_metadata).await?;
            return Ok(new_metadata);
        }

        // File content doesn't exist, store it using hash-based path
        let storage_path = self.hash_to_storage_path(hash);
        
        // Ensure parent directories exist
        if let Some(parent) = storage_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ServerError::Storage {
                    path: parent.to_string_lossy().to_string(),
                    error: format!("Failed to create storage directory: {}", e),
                }
            })?;
        }

        // Write file atomically using temp file + rename
        let temp_file_path = self.temp_dir.join(format!("{}.tmp", Uuid::new_v4()));
        
        // Write content to temporary file
        let mut temp_file = tokio::fs::File::create(&temp_file_path).await.map_err(|e| {
            ServerError::Storage {
                path: temp_file_path.to_string_lossy().to_string(),
                error: format!("Failed to create temp file: {}", e),
            }
        })?;

        temp_file.write_all(&content).await.map_err(|e| {
            ServerError::Storage {
                path: temp_file_path.to_string_lossy().to_string(),
                error: format!("Failed to write temp file: {}", e),
            }
        })?;

        temp_file.sync_all().await.map_err(|e| {
            ServerError::Storage {
                path: temp_file_path.to_string_lossy().to_string(),
                error: format!("Failed to sync temp file: {}", e),
            }
        })?;

        drop(temp_file);

        // Atomically move temp file to final location
        tokio::fs::rename(&temp_file_path, &storage_path).await.map_err(|e| {
            ServerError::Storage {
                path: storage_path.to_string_lossy().to_string(),
                error: format!("Failed to move temp file to storage: {}", e),
            }
        })?;

        // Create metadata with storage path
        let metadata = FileMetadata {
            path: path.to_string(),
            name: std::path::Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path)
                .to_string(),
            size,
            modified,
            is_directory: false,
            xxhash3: hash,
            storage_path: Some(storage_path.to_string_lossy().to_string()),
        };

        // Store metadata in RocksDB
        self.metadata_store.put_file_metadata(&metadata).await?;

        Ok(metadata)
    }

    /// Retrieve file content using zero-copy operations
    pub async fn retrieve_file(&self, path: &str) -> ServerResult<Bytes> {
        // Get file metadata
        let metadata = self.metadata_store
            .get_file_metadata(path)
            .await?
            .ok_or_else(|| ServerError::FileNotFound {
                path: path.to_string(),
            })?;

        if metadata.is_directory {
            return Err(ServerError::InvalidPath {
                path: path.to_string(),
                reason: "Cannot retrieve content of directory".to_string(),
            });
        }

        let storage_path = metadata.storage_path.ok_or_else(|| ServerError::Storage {
            path: path.to_string(),
            error: "No storage path in metadata".to_string(),
        })?;

        // Read file content
        let content = tokio::fs::read(&storage_path).await.map_err(|e| {
            ServerError::Storage {
                path: storage_path.clone(),
                error: format!("Failed to read file: {}", e),
            }
        })?;

        // Verify integrity using hash before converting to Bytes
        if !self.hash_manager.verify(&content, metadata.xxhash3) {
            let actual_hash = hash_bytes(&Bytes::from(content.clone()));
            return Err(ServerError::HashVerificationFailed {
                path: path.to_string(),
                expected: self.hash_manager.to_hex(metadata.xxhash3),
                actual: self.hash_manager.to_hex(actual_hash),
            });
        }

        let bytes = Bytes::from(content);



        Ok(bytes)
    }

    /// Update file content atomically
    pub async fn update_file(&self, path: &str, content: Bytes) -> ServerResult<FileMetadata> {
        // Check if file exists
        let existing_metadata = self.metadata_store
            .get_file_metadata(path)
            .await?
            .ok_or_else(|| ServerError::FileNotFound {
                path: path.to_string(),
            })?;

        if existing_metadata.is_directory {
            return Err(ServerError::InvalidPath {
                path: path.to_string(),
                reason: "Cannot update directory as file".to_string(),
            });
        }

        // Store new content (this handles deduplication automatically)
        let new_metadata = self.store_file(path, content).await?;

        // Clean up old storage if it's no longer referenced
        if let Some(old_storage_path) = &existing_metadata.storage_path {
            if let Some(new_storage_path) = &new_metadata.storage_path {
                if old_storage_path != new_storage_path {
                    // Check if old storage is still referenced by other files
                    if self.metadata_store.find_by_hash(existing_metadata.xxhash3).await?.is_none() {
                        // No other files reference this storage, safe to delete
                        let _ = tokio::fs::remove_file(old_storage_path).await;
                    }
                }
            }
        }

        Ok(new_metadata)
    }

    /// Delete file and clean up storage
    pub async fn delete_file(&self, path: &str) -> ServerResult<bool> {
        // Get file metadata before deletion
        let metadata = match self.metadata_store.get_file_metadata(path).await? {
            Some(meta) => meta,
            None => return Ok(false), // File doesn't exist
        };

        if metadata.is_directory {
            return Err(ServerError::InvalidPath {
                path: path.to_string(),
                reason: "Use delete_directory for directories".to_string(),
            });
        }

        // Delete metadata from RocksDB (this also removes from hash index)
        let deleted = self.metadata_store.delete_file_metadata(path).await?;

        if deleted {
            // Check if storage file is still referenced by other files
            // We need to check after the metadata is deleted to see if hash index is empty
            if let Some(storage_path) = &metadata.storage_path {
                if self.metadata_store.find_by_hash(metadata.xxhash3).await?.is_none() {
                    // No other files reference this storage, safe to delete
                    let _ = tokio::fs::remove_file(storage_path).await;
                }
            }
        }

        Ok(deleted)
    }

    /// Move file to new path (metadata operation only, storage stays the same)
    pub async fn move_file(&self, from_path: &str, to_path: &str) -> ServerResult<FileMetadata> {
        // Get source file metadata
        let mut metadata = self.metadata_store
            .get_file_metadata(from_path)
            .await?
            .ok_or_else(|| ServerError::FileNotFound {
                path: from_path.to_string(),
            })?;

        // Check if destination already exists
        if self.metadata_store.get_file_metadata(to_path).await?.is_some() {
            return Err(ServerError::FileAlreadyExists {
                path: to_path.to_string(),
            });
        }

        // Update metadata with new path
        metadata.path = to_path.to_string();
        metadata.name = std::path::Path::new(to_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(to_path)
            .to_string();
        metadata.modified = SystemTime::now();

        // Store new metadata
        self.metadata_store.put_file_metadata(&metadata).await?;

        // Delete old metadata
        self.metadata_store.delete_file_metadata(from_path).await?;

        Ok(metadata)
    }

    /// Copy file to new path (creates new metadata entry, same storage)
    pub async fn copy_file(&self, from_path: &str, to_path: &str) -> ServerResult<FileMetadata> {
        // Get source file metadata
        let source_metadata = self.metadata_store
            .get_file_metadata(from_path)
            .await?
            .ok_or_else(|| ServerError::FileNotFound {
                path: from_path.to_string(),
            })?;

        // Check if destination already exists
        if self.metadata_store.get_file_metadata(to_path).await?.is_some() {
            return Err(ServerError::FileAlreadyExists {
                path: to_path.to_string(),
            });
        }

        // Create new metadata for copy
        let copy_metadata = FileMetadata {
            path: to_path.to_string(),
            name: std::path::Path::new(to_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(to_path)
                .to_string(),
            size: source_metadata.size,
            modified: SystemTime::now(),
            is_directory: source_metadata.is_directory,
            xxhash3: source_metadata.xxhash3,
            storage_path: source_metadata.storage_path.clone(),
        };

        // Store copy metadata
        self.metadata_store.put_file_metadata(&copy_metadata).await?;

        Ok(copy_metadata)
    }

    /// Get file metadata without reading content
    pub async fn get_file_metadata(&self, path: &str) -> ServerResult<Option<FileMetadata>> {
        self.metadata_store.get_file_metadata(path).await
    }

    /// List directory contents
    pub async fn list_directory(&self, path: &str) -> ServerResult<Vec<FileMetadata>> {
        self.metadata_store.list_directory(path).await
    }

    /// Create directory
    pub async fn create_directory(&self, path: &str) -> ServerResult<()> {
        self.metadata_store.create_directory(path).await
    }

    /// Delete directory recursively
    pub async fn delete_directory_recursive(&self, path: &str) -> ServerResult<Vec<String>> {
        // Get all files that will be deleted
        let deleted_paths = self.metadata_store.delete_directory_recursive(path).await?;

        // Clean up storage files that are no longer referenced
        for deleted_path in &deleted_paths {
            if let Ok(Some(metadata)) = self.metadata_store.get_file_metadata(deleted_path).await {
                if !metadata.is_directory {
                    if let Some(storage_path) = &metadata.storage_path {
                        // Check if storage is still referenced
                        if self.metadata_store.find_by_hash(metadata.xxhash3).await?.is_none() {
                            let _ = tokio::fs::remove_file(storage_path).await;
                        }
                    }
                }
            }
        }

        Ok(deleted_paths)
    }

    /// Calculate hash for file content using zero-copy operations
    pub async fn calculate_hash(&self, content: &Bytes) -> u64 {
        hash_bytes(content)
    }

    /// Verify file integrity
    pub async fn verify_file_integrity(&self, path: &str) -> ServerResult<bool> {
        let metadata = self.metadata_store
            .get_file_metadata(path)
            .await?
            .ok_or_else(|| ServerError::FileNotFound {
                path: path.to_string(),
            })?;

        if metadata.is_directory {
            return Ok(true); // Directories don't have content to verify
        }

        let storage_path = metadata.storage_path.ok_or_else(|| ServerError::Storage {
            path: path.to_string(),
            error: "No storage path in metadata".to_string(),
        })?;

        // Read and hash file content
        let content = tokio::fs::read(&storage_path).await.map_err(|e| {
            ServerError::Storage {
                path: storage_path,
                error: format!("Failed to read file for verification: {}", e),
            }
        })?;

        Ok(self.hash_manager.verify(&content, metadata.xxhash3))
    }

    /// Get storage statistics
    pub async fn get_storage_stats(&self) -> ServerResult<HashMap<String, String>> {
        let mut stats = HashMap::new();

        // Get metadata store stats
        let metadata_stats = self.metadata_store.get_stats().await?;
        for (key, value) in metadata_stats {
            stats.insert(format!("metadata_{}", key), value);
        }

        // Calculate storage directory size
        let storage_size = self.calculate_directory_size(&self.storage_root).await?;
        stats.insert("storage_size_bytes".to_string(), storage_size.to_string());

        // Count files in storage
        let file_count = self.count_storage_files(&self.storage_root).await?;
        stats.insert("storage_file_count".to_string(), file_count.to_string());

        Ok(stats)
    }

    /// Convert hash to storage path using directory structure for performance
    fn hash_to_storage_path(&self, hash: u64) -> PathBuf {
        let hash_str = format!("{:016x}", hash);
        // Create directory structure: ab/cd/ef/abcdef1234567890
        // This spreads files across directories for better filesystem performance
        self.storage_root
            .join(&hash_str[0..2])
            .join(&hash_str[2..4])
            .join(&hash_str[4..6])
            .join(&hash_str)
    }

    /// Calculate total size of directory recursively
    fn calculate_directory_size<'a>(&'a self, dir: &'a Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = ServerResult<u64>> + Send + 'a>> {
        Box::pin(async move {
            let mut total_size = 0u64;
            let mut entries = tokio::fs::read_dir(dir).await.map_err(|e| {
                ServerError::Storage {
                    path: dir.to_string_lossy().to_string(),
                    error: format!("Failed to read directory: {}", e),
                }
            })?;

            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                ServerError::Storage {
                    path: dir.to_string_lossy().to_string(),
                    error: format!("Failed to read directory entry: {}", e),
                }
            })? {
                let metadata = entry.metadata().await.map_err(|e| {
                    ServerError::Storage {
                        path: entry.path().to_string_lossy().to_string(),
                        error: format!("Failed to get file metadata: {}", e),
                    }
                })?;

                if metadata.is_file() {
                    total_size += metadata.len();
                } else if metadata.is_dir() {
                    total_size += self.calculate_directory_size(&entry.path()).await?;
                }
            }

            Ok(total_size)
        })
    }

    /// Count files in storage directory recursively
    fn count_storage_files<'a>(&'a self, dir: &'a Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = ServerResult<u64>> + Send + 'a>> {
        Box::pin(async move {
            let mut file_count = 0u64;
            let mut entries = tokio::fs::read_dir(dir).await.map_err(|e| {
                ServerError::Storage {
                    path: dir.to_string_lossy().to_string(),
                    error: format!("Failed to read directory: {}", e),
                }
            })?;

            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                ServerError::Storage {
                    path: dir.to_string_lossy().to_string(),
                    error: format!("Failed to read directory entry: {}", e),
                }
            })? {
                let metadata = entry.metadata().await.map_err(|e| {
                    ServerError::Storage {
                        path: entry.path().to_string_lossy().to_string(),
                        error: format!("Failed to get file metadata: {}", e),
                    }
                })?;

                if metadata.is_file() {
                    file_count += 1;
                } else if metadata.is_dir() {
                    file_count += self.count_storage_files(&entry.path()).await?;
                }
            }

            Ok(file_count)
        })
    }
}

// Implement Clone for FileStorage to allow sharing across async tasks
impl Clone for FileStorage {
    fn clone(&self) -> Self {
        Self {
            storage_root: self.storage_root.clone(),
            temp_dir: self.temp_dir.clone(),
            metadata_store: Arc::clone(&self.metadata_store),
            hash_manager: self.hash_manager.clone(),
        }
    }
}

// Implement Clone for MetadataStore to allow sharing across async tasks
impl Clone for MetadataStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            _db_path: self._db_path.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;


    async fn create_test_store() -> (MetadataStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = MetadataStore::new(temp_dir.path().join("test.db"))
            .await
            .unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_file_metadata_crud() {
        let (store, _temp_dir) = create_test_store().await;

        let metadata = FileMetadata {
            path: "/test/file.txt".to_string(),
            name: "file.txt".to_string(),
            size: 1024,
            modified: SystemTime::now(),
            is_directory: false,
            xxhash3: 12345,
            storage_path: Some("/storage/abc123".to_string()),
        };

        // Test create
        store.put_file_metadata(&metadata).await.unwrap();

        // Test read
        let retrieved = store
            .get_file_metadata("/test/file.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.path, metadata.path);
        assert_eq!(retrieved.size, metadata.size);
        assert_eq!(retrieved.xxhash3, metadata.xxhash3);

        // Test update
        let mut updated_metadata = metadata.clone();
        updated_metadata.size = 2048;
        store.put_file_metadata(&updated_metadata).await.unwrap();

        let retrieved = store
            .get_file_metadata("/test/file.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.size, 2048);

        // Test delete
        let deleted = store.delete_file_metadata("/test/file.txt").await.unwrap();
        assert!(deleted);

        let retrieved = store.get_file_metadata("/test/file.txt").await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_directory_operations() {
        let (store, _temp_dir) = create_test_store().await;

        // Create directory
        store.create_directory("/test").await.unwrap();

        // Verify directory exists
        let dir_metadata = store.get_file_metadata("/test").await.unwrap().unwrap();
        assert!(dir_metadata.is_directory);
        assert_eq!(dir_metadata.path, "/test");

        // Create files in directory
        let file1 = FileMetadata::new(
            "/test/file1.txt".to_string(),
            100,
            SystemTime::now(),
            111,
        );
        let file2 = FileMetadata::new(
            "/test/file2.txt".to_string(),
            200,
            SystemTime::now(),
            222,
        );

        store.put_file_metadata(&file1).await.unwrap();
        store.put_file_metadata(&file2).await.unwrap();

        // List directory contents
        let contents = store.list_directory("/test").await.unwrap();
        assert_eq!(contents.len(), 2);
        assert!(contents.iter().any(|f| f.name == "file1.txt"));
        assert!(contents.iter().any(|f| f.name == "file2.txt"));

        // Delete directory recursively
        let deleted_paths = store.delete_directory_recursive("/test").await.unwrap();
        assert_eq!(deleted_paths.len(), 3); // 2 files + 1 directory

        // Verify directory is gone
        let dir_metadata = store.get_file_metadata("/test").await.unwrap();
        assert!(dir_metadata.is_none());
    }

    #[tokio::test]
    async fn test_hash_index() {
        let (store, _temp_dir) = create_test_store().await;

        let metadata = FileMetadata::new(
            "/test/file.txt".to_string(),
            1024,
            SystemTime::now(),
            12345,
        );

        store.put_file_metadata(&metadata).await.unwrap();

        // Test hash lookup
        let found_path = store.find_by_hash(12345).await.unwrap().unwrap();
        assert_eq!(found_path, "/test/file.txt");

        // Test non-existent hash
        let not_found = store.find_by_hash(99999).await.unwrap();
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_path_normalization() {
        let (store, _temp_dir) = create_test_store().await;

        let metadata = FileMetadata::new(
            "test/file.txt".to_string(), // No leading slash
            1024,
            SystemTime::now(),
            12345,
        );

        store.put_file_metadata(&metadata).await.unwrap();

        // Should be able to retrieve with normalized path
        let retrieved = store
            .get_file_metadata("/test/file.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.path, "/test/file.txt");

        // Should also work with the original path
        let retrieved2 = store
            .get_file_metadata("test/file.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved2.path, "/test/file.txt");
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let (store, _temp_dir) = create_test_store().await;

        // Create multiple files concurrently
        let mut handles = Vec::new();
        for i in 0..10 {
            let store_clone = store.clone();
            let handle = tokio::spawn(async move {
                let metadata = FileMetadata::new(
                    format!("/test/file{}.txt", i),
                    i * 100,
                    SystemTime::now(),
                    i,
                );
                store_clone.put_file_metadata(&metadata).await.unwrap();
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all files were created
        for i in 0..10 {
            let metadata = store
                .get_file_metadata(&format!("/test/file{}.txt", i))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(metadata.size, i * 100);
        }
    }

    #[tokio::test]
    async fn test_modified_time_index() {
        let (store, _temp_dir) = create_test_store().await;

        let now = SystemTime::now();
        let past = now - std::time::Duration::from_secs(3600); // 1 hour ago

        // Create files with different modification times
        let old_file = FileMetadata {
            path: "/old_file.txt".to_string(),
            name: "old_file.txt".to_string(),
            size: 100,
            modified: past,
            is_directory: false,
            xxhash3: 111,
            storage_path: None,
        };

        let new_file = FileMetadata {
            path: "/new_file.txt".to_string(),
            name: "new_file.txt".to_string(),
            size: 200,
            modified: now,
            is_directory: false,
            xxhash3: 222,
            storage_path: None,
        };

        store.put_file_metadata(&old_file).await.unwrap();
        store.put_file_metadata(&new_file).await.unwrap();

        // Query files modified since 30 minutes ago
        let recent_threshold = now - std::time::Duration::from_secs(1800);
        let recent_files = store
            .get_files_modified_since(recent_threshold)
            .await
            .unwrap();

        assert_eq!(recent_files.len(), 1);
        assert!(recent_files.contains(&"/new_file.txt".to_string()));
    }

    // FileStorage tests
    async fn create_test_file_storage() -> (FileStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let metadata_store = Arc::new(
            MetadataStore::new(temp_dir.path().join("metadata.db"))
                .await
                .unwrap(),
        );
        let storage = FileStorage::new(temp_dir.path().join("storage"), metadata_store)
            .await
            .unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_file_storage_store_and_retrieve() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let content = Bytes::from_static(b"Hello, World!");
        let path = "/test/file.txt";

        // Store file
        let metadata = storage.store_file(path, content.clone()).await.unwrap();
        assert_eq!(metadata.path, path);
        assert_eq!(metadata.size, content.len() as u64);
        assert!(!metadata.is_directory);
        assert!(metadata.storage_path.is_some());

        // Retrieve file
        let retrieved_content = storage.retrieve_file(path).await.unwrap();
        assert_eq!(retrieved_content, content);

        // Verify metadata
        let retrieved_metadata = storage.get_file_metadata(path).await.unwrap().unwrap();
        assert_eq!(retrieved_metadata.path, metadata.path);
        assert_eq!(retrieved_metadata.xxhash3, metadata.xxhash3);
    }

    #[tokio::test]
    async fn test_file_storage_deduplication() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let content = Bytes::from_static(b"Duplicate content");
        let path1 = "/file1.txt";
        let path2 = "/file2.txt";

        // Store same content in two different paths
        let metadata1 = storage.store_file(path1, content.clone()).await.unwrap();
        let metadata2 = storage.store_file(path2, content.clone()).await.unwrap();

        // Should have same hash and storage path (deduplication)
        assert_eq!(metadata1.xxhash3, metadata2.xxhash3);
        assert_eq!(metadata1.storage_path, metadata2.storage_path);

        // Both files should be retrievable
        let content1 = storage.retrieve_file(path1).await.unwrap();
        let content2 = storage.retrieve_file(path2).await.unwrap();
        assert_eq!(content1, content);
        assert_eq!(content2, content);
    }

    #[tokio::test]
    async fn test_file_storage_update() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let original_content = Bytes::from_static(b"Original content");
        let updated_content = Bytes::from_static(b"Updated content");
        let path = "/test/file.txt";

        // Store original file
        let original_metadata = storage.store_file(path, original_content.clone()).await.unwrap();

        // Update file
        let updated_metadata = storage.update_file(path, updated_content.clone()).await.unwrap();

        // Should have different hash and potentially different storage path
        assert_ne!(original_metadata.xxhash3, updated_metadata.xxhash3);
        assert_eq!(updated_metadata.path, path);

        // Retrieve updated content
        let retrieved_content = storage.retrieve_file(path).await.unwrap();
        assert_eq!(retrieved_content, updated_content);
    }

    #[tokio::test]
    async fn test_file_storage_delete() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let content = Bytes::from_static(b"Content to delete");
        let path = "/test/file.txt";

        // Store file
        storage.store_file(path, content).await.unwrap();

        // Verify file exists
        assert!(storage.get_file_metadata(path).await.unwrap().is_some());

        // Delete file
        let deleted = storage.delete_file(path).await.unwrap();
        assert!(deleted);

        // Verify file is gone
        assert!(storage.get_file_metadata(path).await.unwrap().is_none());

        // Verify retrieval fails
        assert!(storage.retrieve_file(path).await.is_err());
    }

    #[tokio::test]
    async fn test_file_storage_move_and_copy() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let content = Bytes::from_static(b"Content to move and copy");
        let original_path = "/original.txt";
        let moved_path = "/moved.txt";
        let copied_path = "/copied.txt";

        // Store original file
        let original_metadata = storage.store_file(original_path, content.clone()).await.unwrap();

        // Move file
        let moved_metadata = storage.move_file(original_path, moved_path).await.unwrap();
        assert_eq!(moved_metadata.xxhash3, original_metadata.xxhash3);
        assert_eq!(moved_metadata.storage_path, original_metadata.storage_path);

        // Original should be gone, moved should exist
        assert!(storage.get_file_metadata(original_path).await.unwrap().is_none());
        assert!(storage.get_file_metadata(moved_path).await.unwrap().is_some());

        // Copy file
        let copied_metadata = storage.copy_file(moved_path, copied_path).await.unwrap();
        assert_eq!(copied_metadata.xxhash3, moved_metadata.xxhash3);
        assert_eq!(copied_metadata.storage_path, moved_metadata.storage_path);

        // Both moved and copied should exist
        assert!(storage.get_file_metadata(moved_path).await.unwrap().is_some());
        assert!(storage.get_file_metadata(copied_path).await.unwrap().is_some());

        // Both should have same content
        let moved_content = storage.retrieve_file(moved_path).await.unwrap();
        let copied_content = storage.retrieve_file(copied_path).await.unwrap();
        assert_eq!(moved_content, content);
        assert_eq!(copied_content, content);
    }

    #[tokio::test]
    async fn test_file_storage_directory_operations() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let dir_path = "/test_dir";
        let file_path = "/test_dir/file.txt";
        let content = Bytes::from_static(b"File in directory");

        // Create directory
        storage.create_directory(dir_path).await.unwrap();

        // Store file in directory
        storage.store_file(file_path, content.clone()).await.unwrap();

        // List directory
        let entries = storage.list_directory(dir_path).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "file.txt");

        // Delete directory recursively
        let deleted_paths = storage.delete_directory_recursive(dir_path).await.unwrap();
        assert_eq!(deleted_paths.len(), 2); // file + directory

        // Verify directory is gone
        assert!(storage.get_file_metadata(dir_path).await.unwrap().is_none());
        assert!(storage.get_file_metadata(file_path).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_storage_integrity_verification() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let content = Bytes::from_static(b"Content for integrity test");
        let path = "/test/integrity.txt";

        // Store file
        storage.store_file(path, content).await.unwrap();

        // Verify integrity
        let is_valid = storage.verify_file_integrity(path).await.unwrap();
        assert!(is_valid);
    }

    #[tokio::test]
    async fn test_file_storage_concurrent_operations() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        // Store multiple files concurrently
        let mut handles = Vec::new();
        for i in 0..10 {
            let storage_clone = storage.clone();
            let handle = tokio::spawn(async move {
                let content = Bytes::from(format!("Content for file {}", i));
                let path = format!("/concurrent/file{}.txt", i);
                storage_clone.store_file(&path, content).await.unwrap();
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all files were created
        for i in 0..10 {
            let path = format!("/concurrent/file{}.txt", i);
            let metadata = storage.get_file_metadata(&path).await.unwrap().unwrap();
            assert_eq!(metadata.path, path);
        }
    }

    #[tokio::test]
    async fn test_file_storage_deduplication_cleanup() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let content = Bytes::from_static(b"Shared content for cleanup test");
        let path1 = "/shared1.txt";
        let path2 = "/shared2.txt";

        // Store same content in two files
        storage.store_file(path1, content.clone()).await.unwrap();
        storage.store_file(path2, content.clone()).await.unwrap();

        // Delete one file - storage should remain (still referenced)
        storage.delete_file(path1).await.unwrap();
        
        // Second file should still be retrievable
        let retrieved = storage.retrieve_file(path2).await.unwrap();
        assert_eq!(retrieved, content);

        // Delete second file - now storage should be cleaned up
        storage.delete_file(path2).await.unwrap();
        
        // Both files should be gone
        assert!(storage.get_file_metadata(path1).await.unwrap().is_none());
        assert!(storage.get_file_metadata(path2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_storage_hash_calculation() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        let content = Bytes::from_static(b"Content for hash test");
        
        // Calculate hash
        let hash = storage.calculate_hash(&content).await;
        
        // Store file and verify same hash
        let metadata = storage.store_file("/hash_test.txt", content).await.unwrap();
        assert_eq!(metadata.xxhash3, hash);
    }

    #[tokio::test]
    async fn test_file_storage_error_handling() {
        let (storage, _temp_dir) = create_test_file_storage().await;

        // Test retrieving non-existent file
        let result = storage.retrieve_file("/nonexistent.txt").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ServerError::FileNotFound { .. }));

        // Test updating non-existent file
        let content = Bytes::from_static(b"test");
        let result = storage.update_file("/nonexistent.txt", content).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ServerError::FileNotFound { .. }));

        // Test deleting non-existent file
        let result = storage.delete_file("/nonexistent.txt").await.unwrap();
        assert!(!result); // Should return false, not error

        // Test moving non-existent file
        let result = storage.move_file("/nonexistent.txt", "/moved.txt").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ServerError::FileNotFound { .. }));
    }
}