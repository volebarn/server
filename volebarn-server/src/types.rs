//! Shared data types for Volebarn file synchronization system
//! 
//! This module contains all the core data structures used throughout the system.
//! Types support dual serialization: serde_json for API layer and bincode for storage.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;


/// File metadata with dual serialization support
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode)]
pub struct FileMetadata {
    /// Full path including subdirectories
    pub path: String,
    /// Just the filename
    pub name: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub modified: SystemTime,
    /// Whether this is a directory
    pub is_directory: bool,
    /// xxHash3 for integrity verification
    pub xxhash3: u64,
    /// Path to actual file on disk (None for directories, used internally)
    pub storage_path: Option<String>,
}

/// API response version of FileMetadata with JSON-friendly types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadataResponse {
    pub path: String,
    pub name: String,
    pub size: u64,
    /// ISO 8601 timestamp string for JSON compatibility
    pub modified: String,
    pub is_directory: bool,
    /// Hex-encoded hash for JSON compatibility
    pub hash: String,
}

/// Directory listing containing metadata for all entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryListing {
    /// Directory path
    pub path: String,
    /// List of files and subdirectories
    pub entries: Vec<FileMetadata>,
}

/// File upload data for bulk operations
#[derive(Debug, Clone)]
pub struct FileUpload {
    /// Target path for the file
    pub path: String,
    /// File content using zero-copy bytes
    pub content: Bytes,
}

/// Complete file manifest with checksums (internal format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileManifest {
    /// Map of file path to metadata
    pub files: HashMap<String, FileMetadata>,
}

/// Complete file manifest for API responses (JSON-friendly)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileManifestResponse {
    /// Map of file path to metadata
    pub files: HashMap<String, FileMetadataResponse>,
}

/// Sync plan describing operations needed to synchronize client with server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPlan {
    /// Files client should upload to server
    pub client_upload: Vec<String>,
    /// Files client should download from server
    pub client_download: Vec<String>,
    /// Files client should delete locally (server is source of truth)
    pub client_delete: Vec<String>,
    /// Directories client needs to create locally
    pub client_create_dirs: Vec<String>,
    /// Conflicts that need resolution
    pub conflicts: Vec<SyncConflict>,
}

/// Represents a sync conflict between local and remote files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConflict {
    /// Path of the conflicting file
    pub path: String,
    /// Local file modification time
    pub local_modified: SystemTime,
    /// Remote file modification time
    pub remote_modified: SystemTime,
    /// Suggested resolution strategy
    pub resolution: ConflictResolution,
}

/// Conflict resolution strategies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConflictResolution {
    /// Upload local version to server
    UseLocal,
    /// Download server version to client
    UseRemote,
    /// Use the version with newer timestamp
    UseNewer,
    /// Requires manual user intervention
    Manual,
}

/// Result of applying a sync plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// Files successfully uploaded to server
    pub uploaded: Vec<String>,
    /// Files successfully downloaded from server
    pub downloaded: Vec<String>,
    /// Files successfully deleted locally
    pub deleted_local: Vec<String>,
    /// Directories successfully created locally
    pub created_dirs: Vec<String>,
    /// Conflicts that were resolved automatically
    pub conflicts_resolved: Vec<String>,
    /// Operations that failed with error messages
    pub errors: Vec<(String, String)>,
}

/// Response for bulk upload operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkUploadResponse {
    /// Successfully uploaded file paths
    pub success: Vec<String>,
    /// Failed uploads with error messages
    pub failed: Vec<OperationError>,
}

/// Response for bulk delete operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkDeleteResponse {
    /// Successfully deleted file paths
    pub success: Vec<String>,
    /// Failed deletions with error messages
    pub failed: Vec<OperationError>,
}

/// Individual file download result for bulk operations
#[derive(Debug, Clone)]
pub struct FileDownload {
    /// File path
    pub path: String,
    /// File content using zero-copy bytes
    pub content: Bytes,
    /// File integrity hash
    pub xxhash3: u64,
}

/// Operation error with detailed context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationError {
    /// Path that failed
    pub path: String,
    /// Error message
    pub error: String,
    /// Structured error code
    pub error_code: ErrorCode,
}

/// Structured error codes for better error handling
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ErrorCode {
    FileNotFound,
    PermissionDenied,
    HashMismatch,
    NetworkError,
    ServerError,
    StorageError,
    InvalidPath,
    ResourceLimit,
    ConcurrentModification,
    TlsError,
    Timeout,
    RateLimited,
    ConfigError,
}

/// Request types for various operations (internal format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    /// Client's current file manifest
    pub client_manifest: FileManifest,
    /// How to resolve conflicts
    pub conflict_resolution: ConflictResolutionStrategy,
}

/// Sync request for API (JSON-friendly format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequestApi {
    /// Client's current file manifest
    pub client_manifest: FileManifestResponse,
    /// How to resolve conflicts
    pub conflict_resolution: ConflictResolutionStrategy,
}

/// Conflict resolution strategy for sync requests
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConflictResolutionStrategy {
    /// Local changes win (upload to server)
    PreferLocal,
    /// Remote changes win (download from server)
    PreferRemote,
    /// Newer timestamp wins
    PreferNewer,
    /// Return conflicts for manual resolution
    Manual,
}

/// Request to move a file or directory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveRequest {
    pub from_path: String,
    pub to_path: String,
}

/// Request to copy a file or directory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyRequest {
    pub from_path: String,
    pub to_path: String,
}

/// Request to search for files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    /// Search pattern (glob-style)
    pub pattern: String,
    /// Directory to search within (None for root)
    pub path: Option<String>,
    /// Whether to search recursively
    pub recursive: bool,
}

/// Request to delete multiple files/directories
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkDeleteRequest {
    pub paths: Vec<String>,
}

/// Request to download multiple files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkDownloadRequest {
    pub paths: Vec<String>,
}

/// Response for bulk download operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkDownloadResponse {
    /// Individual files (not archived)
    pub files: Vec<FileDownloadResponse>,
}

/// File download response for JSON serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDownloadResponse {
    pub path: String,
    /// Base64-encoded content for JSON transport
    pub content: String,
    /// Hex-encoded hash
    pub hash: String,
}

/// Bulk operation types for tracking and progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BulkOperation {
    Upload {
        files: Vec<String>,
        total_size: u64,
    },
    Download {
        files: Vec<String>,
        total_size: u64,
    },
    Delete {
        paths: Vec<String>,
    },
    Sync {
        plan: SyncPlan,
    },
}

/// Conversion utilities between internal and external formats
impl From<FileMetadata> for FileMetadataResponse {
    fn from(metadata: FileMetadata) -> Self {
        use std::time::UNIX_EPOCH;
        
        let modified = metadata
            .modified
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string();
        
        Self {
            path: metadata.path,
            name: metadata.name,
            size: metadata.size,
            modified,
            is_directory: metadata.is_directory,
            hash: format!("{:016x}", metadata.xxhash3),
        }
    }
}

impl TryFrom<FileMetadataResponse> for FileMetadata {
    type Error = crate::error::ServerError;
    
    fn try_from(response: FileMetadataResponse) -> Result<Self, Self::Error> {
        use std::time::{Duration, UNIX_EPOCH};
        
        let timestamp = response.modified.parse::<u64>()
            .map_err(|_| crate::error::ServerError::InvalidTimestamp { 
                timestamp: response.modified.clone() 
            })?;
        let modified = UNIX_EPOCH + Duration::from_secs(timestamp);
        let xxhash3 = u64::from_str_radix(&response.hash, 16)
            .map_err(|_| crate::error::ServerError::InvalidHash { 
                hash: response.hash.clone() 
            })?;
            
        Ok(Self {
            path: response.path,
            name: response.name,
            size: response.size,
            modified,
            is_directory: response.is_directory,
            xxhash3,
            storage_path: None, // Set separately by storage layer
        })
    }
}

/// Helper functions for working with file paths
impl FileMetadata {
    /// Create a new file metadata instance
    pub fn new(path: String, size: u64, modified: SystemTime, xxhash3: u64) -> Self {
        let name = std::path::Path::new(&path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&path)
            .to_string();
            
        Self {
            path,
            name,
            size,
            modified,
            is_directory: false,
            xxhash3,
            storage_path: None,
        }
    }
    
    /// Create a new directory metadata instance
    pub fn new_directory(path: String, modified: SystemTime) -> Self {
        let name = std::path::Path::new(&path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&path)
            .to_string();
            
        Self {
            path,
            name,
            size: 0,
            modified,
            is_directory: true,
            xxhash3: 0, // Directories don't have content hash
            storage_path: None,
        }
    }
    
    /// Get the parent directory path
    pub fn parent_path(&self) -> Option<String> {
        std::path::Path::new(&self.path)
            .parent()
            .and_then(|p| p.to_str())
            .map(|s| s.to_string())
    }
    
    /// Check if this file is in the given directory
    pub fn is_in_directory(&self, dir_path: &str) -> bool {
        self.path.starts_with(&format!("{}/", dir_path)) || 
        (dir_path == "/" && !self.path[1..].contains('/'))
    }
}

/// Helper functions for sync operations
impl SyncPlan {
    /// Create an empty sync plan
    pub fn new() -> Self {
        Self {
            client_upload: Vec::new(),
            client_download: Vec::new(),
            client_delete: Vec::new(),
            client_create_dirs: Vec::new(),
            conflicts: Vec::new(),
        }
    }
    
    /// Check if the sync plan has any operations
    pub fn is_empty(&self) -> bool {
        self.client_upload.is_empty() &&
        self.client_download.is_empty() &&
        self.client_delete.is_empty() &&
        self.client_create_dirs.is_empty() &&
        self.conflicts.is_empty()
    }
    
    /// Get total number of operations
    pub fn total_operations(&self) -> usize {
        self.client_upload.len() +
        self.client_download.len() +
        self.client_delete.len() +
        self.client_create_dirs.len() +
        self.conflicts.len()
    }
}

impl Default for SyncPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper functions for sync results
impl SyncResult {
    /// Create an empty sync result
    pub fn new() -> Self {
        Self {
            uploaded: Vec::new(),
            downloaded: Vec::new(),
            deleted_local: Vec::new(),
            created_dirs: Vec::new(),
            conflicts_resolved: Vec::new(),
            errors: Vec::new(),
        }
    }
    
    /// Check if the sync was completely successful
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }
    
    /// Get total number of successful operations
    pub fn success_count(&self) -> usize {
        self.uploaded.len() +
        self.downloaded.len() +
        self.deleted_local.len() +
        self.created_dirs.len() +
        self.conflicts_resolved.len()
    }
    
    /// Get total number of failed operations
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }
}

impl Default for SyncResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert API sync request to internal format
impl TryFrom<SyncRequestApi> for SyncRequest {
    type Error = crate::error::ServerError;
    
    fn try_from(api_request: SyncRequestApi) -> Result<Self, Self::Error> {
        let mut files = HashMap::new();
        
        for (path, response_metadata) in api_request.client_manifest.files {
            let metadata: FileMetadata = response_metadata.try_into()?;
            files.insert(path, metadata);
        }
        
        let client_manifest = FileManifest { files };
        
        Ok(Self {
            client_manifest,
            conflict_resolution: api_request.conflict_resolution,
        })
    }
}