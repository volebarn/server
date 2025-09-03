// Core types and utilities for benchmarking
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// File metadata structure for benchmarking serialization
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileMetadata {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub modified: SystemTime,
    pub is_directory: bool,
    pub xxhash3: u64,
    pub storage_path: Option<String>,
    pub permissions: u32,
    pub created: SystemTime,
    pub accessed: SystemTime,
}

/// Bitcode-compatible file metadata (uses u64 timestamps)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct FileMetadataBitcode {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub modified_secs: u64,
    pub is_directory: bool,
    pub xxhash3: u64,
    pub storage_path: Option<String>,
    pub permissions: u32,
    pub created_secs: u64,
    pub accessed_secs: u64,
}

impl From<&FileMetadata> for FileMetadataBitcode {
    fn from(meta: &FileMetadata) -> Self {
        Self {
            path: meta.path.clone(),
            name: meta.name.clone(),
            size: meta.size,
            modified_secs: meta.modified.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
            is_directory: meta.is_directory,
            xxhash3: meta.xxhash3,
            storage_path: meta.storage_path.clone(),
            permissions: meta.permissions,
            created_secs: meta.created.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
            accessed_secs: meta.accessed.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
        }
    }
}

/// Directory listing structure
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectoryListing {
    pub path: String,
    pub entries: Vec<FileMetadata>,
    pub total_size: u64,
    pub file_count: usize,
    pub directory_count: usize,
}

/// Bitcode-compatible directory listing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct DirectoryListingBitcode {
    pub path: String,
    pub entries: Vec<FileMetadataBitcode>,
    pub total_size: u64,
    pub file_count: usize,
    pub directory_count: usize,
}

impl From<&DirectoryListing> for DirectoryListingBitcode {
    fn from(dir: &DirectoryListing) -> Self {
        Self {
            path: dir.path.clone(),
            entries: dir.entries.iter().map(FileMetadataBitcode::from).collect(),
            total_size: dir.total_size,
            file_count: dir.file_count,
            directory_count: dir.directory_count,
        }
    }
}

/// File manifest for sync operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileManifest {
    pub files: HashMap<String, FileMetadata>,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub checksum: u64,
}

/// Sync plan structure
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncPlan {
    pub client_upload: Vec<String>,
    pub client_download: Vec<String>,
    pub client_delete: Vec<String>,
    pub client_create_dirs: Vec<String>,
    pub conflicts: Vec<SyncConflict>,
    pub total_operations: usize,
    pub estimated_bytes: u64,
}

/// Sync conflict information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncConflict {
    pub path: String,
    pub local_modified: SystemTime,
    pub remote_modified: SystemTime,
    pub local_size: u64,
    pub remote_size: u64,
    pub local_hash: u64,
    pub remote_hash: u64,
}

/// Bulk operation response
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BulkOperationResponse {
    pub success: Vec<String>,
    pub failed: Vec<OperationError>,
    pub total_processed: usize,
    pub total_bytes: u64,
    pub duration_ms: u64,
}

/// Operation error details
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperationError {
    pub path: String,
    pub error: String,
    pub error_code: ErrorCode,
    pub retry_count: u32,
}

/// Error codes for operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub enum ErrorCode {
    FileNotFound,
    PermissionDenied,
    HashMismatch,
    NetworkError,
    ServerError,
    StorageError,
    TimeoutError,
}

pub mod test_data;
pub mod generators;
pub mod simd_utils;