//! Storage-specific types that use bitcode serialization
//! 
//! These types are optimized for local storage using bitcode + Snappy compression.
//! They use SerializableSystemTime instead of SystemTime for bitcode compatibility.

use crate::time_utils::SerializableSystemTime;
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use base64::{Engine as _, engine::general_purpose};

/// File metadata optimized for storage with bitcode serialization
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, bitcode::Encode, bitcode::Decode)]
pub struct StorageFileMetadata {
    /// Full path including subdirectories
    pub path: String,
    /// Just the filename
    pub name: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified timestamp (serializable)
    pub modified: SerializableSystemTime,
    /// Whether this is a directory
    pub is_directory: bool,
    /// xxHash3 for integrity verification
    pub xxhash3: u64,
    /// Path to actual file on disk (None for directories, used internally)
    pub storage_path: Option<String>,
}

/// Directory listing optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageDirectoryListing {
    /// Directory path
    pub path: String,
    /// List of files and subdirectories
    pub entries: Vec<StorageFileMetadata>,
}

/// File manifest optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageFileManifest {
    /// Map of file path to metadata
    pub files: HashMap<String, StorageFileMetadata>,
}

/// Sync plan optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageSyncPlan {
    /// Files client should upload to server
    pub client_upload: Vec<String>,
    /// Files client should download from server
    pub client_download: Vec<String>,
    /// Files client should delete locally (server is source of truth)
    pub client_delete: Vec<String>,
    /// Directories client needs to create locally
    pub client_create_dirs: Vec<String>,
    /// Conflicts that need resolution
    pub conflicts: Vec<StorageSyncConflict>,
}

/// Sync conflict optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageSyncConflict {
    /// Path of the conflicting file
    pub path: String,
    /// Local file modification time
    pub local_modified: SerializableSystemTime,
    /// Remote file modification time
    pub remote_modified: SerializableSystemTime,
    /// Suggested resolution strategy
    pub resolution: ConflictResolution,
}

/// Sync result optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageSyncResult {
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

/// Sync request optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageSyncRequest {
    /// Client's current file manifest
    pub client_manifest: StorageFileManifest,
    /// How to resolve conflicts
    pub conflict_resolution: ConflictResolutionStrategy,
}

/// Bulk operation optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub enum StorageBulkOperation {
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
        plan: StorageSyncPlan,
    },
}

/// File download for bulk operations optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageFileDownload {
    /// File path
    pub path: String,
    /// File content as Vec<u8> for bitcode compatibility
    pub content: Vec<u8>,
    /// File integrity hash
    pub xxhash3: u64,
}

/// Bulk download response optimized for storage
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct StorageBulkDownloadResponse {
    /// Individual files (not archived)
    pub files: Vec<StorageFileDownload>,
}

// Conversion implementations between API types and storage types
impl From<FileMetadata> for StorageFileMetadata {
    fn from(metadata: FileMetadata) -> Self {
        Self {
            path: metadata.path,
            name: metadata.name,
            size: metadata.size,
            modified: SerializableSystemTime::from(metadata.modified),
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
            modified: SystemTime::from(storage.modified),
            is_directory: storage.is_directory,
            xxhash3: storage.xxhash3,
            storage_path: storage.storage_path,
        }
    }
}

impl From<DirectoryListing> for StorageDirectoryListing {
    fn from(listing: DirectoryListing) -> Self {
        Self {
            path: listing.path,
            entries: listing.entries.into_iter().map(StorageFileMetadata::from).collect(),
        }
    }
}

impl From<StorageDirectoryListing> for DirectoryListing {
    fn from(storage: StorageDirectoryListing) -> Self {
        Self {
            path: storage.path,
            entries: storage.entries.into_iter().map(FileMetadata::from).collect(),
        }
    }
}

impl From<FileManifest> for StorageFileManifest {
    fn from(manifest: FileManifest) -> Self {
        Self {
            files: manifest.files.into_iter()
                .map(|(k, v)| (k, StorageFileMetadata::from(v)))
                .collect(),
        }
    }
}

impl From<StorageFileManifest> for FileManifest {
    fn from(storage: StorageFileManifest) -> Self {
        Self {
            files: storage.files.into_iter()
                .map(|(k, v)| (k, FileMetadata::from(v)))
                .collect(),
        }
    }
}

impl From<SyncConflict> for StorageSyncConflict {
    fn from(conflict: SyncConflict) -> Self {
        Self {
            path: conflict.path,
            local_modified: SerializableSystemTime::from(conflict.local_modified),
            remote_modified: SerializableSystemTime::from(conflict.remote_modified),
            resolution: conflict.resolution,
        }
    }
}

impl From<StorageSyncConflict> for SyncConflict {
    fn from(storage: StorageSyncConflict) -> Self {
        Self {
            path: storage.path,
            local_modified: SystemTime::from(storage.local_modified),
            remote_modified: SystemTime::from(storage.remote_modified),
            resolution: storage.resolution,
        }
    }
}

impl From<SyncPlan> for StorageSyncPlan {
    fn from(plan: SyncPlan) -> Self {
        Self {
            client_upload: plan.client_upload,
            client_download: plan.client_download,
            client_delete: plan.client_delete,
            client_create_dirs: plan.client_create_dirs,
            conflicts: plan.conflicts.into_iter().map(StorageSyncConflict::from).collect(),
        }
    }
}

impl From<StorageSyncPlan> for SyncPlan {
    fn from(storage: StorageSyncPlan) -> Self {
        Self {
            client_upload: storage.client_upload,
            client_download: storage.client_download,
            client_delete: storage.client_delete,
            client_create_dirs: storage.client_create_dirs,
            conflicts: storage.conflicts.into_iter().map(SyncConflict::from).collect(),
        }
    }
}

impl From<SyncResult> for StorageSyncResult {
    fn from(result: SyncResult) -> Self {
        Self {
            uploaded: result.uploaded,
            downloaded: result.downloaded,
            deleted_local: result.deleted_local,
            created_dirs: result.created_dirs,
            conflicts_resolved: result.conflicts_resolved,
            errors: result.errors,
        }
    }
}

impl From<StorageSyncResult> for SyncResult {
    fn from(storage: StorageSyncResult) -> Self {
        Self {
            uploaded: storage.uploaded,
            downloaded: storage.downloaded,
            deleted_local: storage.deleted_local,
            created_dirs: storage.created_dirs,
            conflicts_resolved: storage.conflicts_resolved,
            errors: storage.errors,
        }
    }
}

impl From<SyncRequest> for StorageSyncRequest {
    fn from(request: SyncRequest) -> Self {
        Self {
            client_manifest: StorageFileManifest::from(request.client_manifest),
            conflict_resolution: request.conflict_resolution,
        }
    }
}

impl From<StorageSyncRequest> for SyncRequest {
    fn from(storage: StorageSyncRequest) -> Self {
        Self {
            client_manifest: FileManifest::from(storage.client_manifest),
            conflict_resolution: storage.conflict_resolution,
        }
    }
}

impl From<BulkOperation> for StorageBulkOperation {
    fn from(operation: BulkOperation) -> Self {
        match operation {
            BulkOperation::Upload { files, total_size } => {
                StorageBulkOperation::Upload { files, total_size }
            }
            BulkOperation::Download { files, total_size } => {
                StorageBulkOperation::Download { files, total_size }
            }
            BulkOperation::Delete { paths } => {
                StorageBulkOperation::Delete { paths }
            }
            BulkOperation::Sync { plan } => {
                StorageBulkOperation::Sync { plan: StorageSyncPlan::from(plan) }
            }
        }
    }
}

impl From<StorageBulkOperation> for BulkOperation {
    fn from(storage: StorageBulkOperation) -> Self {
        match storage {
            StorageBulkOperation::Upload { files, total_size } => {
                BulkOperation::Upload { files, total_size }
            }
            StorageBulkOperation::Download { files, total_size } => {
                BulkOperation::Download { files, total_size }
            }
            StorageBulkOperation::Delete { paths } => {
                BulkOperation::Delete { paths }
            }
            StorageBulkOperation::Sync { plan } => {
                BulkOperation::Sync { plan: SyncPlan::from(plan) }
            }
        }
    }
}

// Implement dual serialization for storage types
use crate::serialization::{DualSerialize, DualDeserialize};

impl DualSerialize for StorageFileMetadata {}
impl DualDeserialize for StorageFileMetadata {}

impl DualSerialize for StorageFileManifest {}
impl DualDeserialize for StorageFileManifest {}

impl DualSerialize for StorageSyncPlan {}
impl DualDeserialize for StorageSyncPlan {}

impl DualSerialize for StorageSyncResult {}
impl DualDeserialize for StorageSyncResult {}

impl DualSerialize for StorageSyncRequest {}
impl DualDeserialize for StorageSyncRequest {}

impl DualSerialize for StorageBulkOperation {}
impl DualDeserialize for StorageBulkOperation {}

impl DualSerialize for StorageFileDownload {}
impl DualDeserialize for StorageFileDownload {}

impl DualSerialize for StorageBulkDownloadResponse {}
impl DualDeserialize for StorageBulkDownloadResponse {}

// Conversion implementations for new storage types
impl From<crate::types::FileDownload> for StorageFileDownload {
    fn from(download: crate::types::FileDownload) -> Self {
        Self {
            path: download.path,
            content: download.content.to_vec(),
            xxhash3: download.xxhash3,
        }
    }
}

impl From<StorageFileDownload> for crate::types::FileDownload {
    fn from(storage: StorageFileDownload) -> Self {
        Self {
            path: storage.path,
            content: bytes::Bytes::from(storage.content),
            xxhash3: storage.xxhash3,
        }
    }
}

impl From<crate::types::BulkDownloadResponse> for StorageBulkDownloadResponse {
    fn from(response: crate::types::BulkDownloadResponse) -> Self {
        Self {
            files: response.files.into_iter()
                .map(|f| StorageFileDownload {
                    path: f.path,
                    content: base64::engine::general_purpose::STANDARD.decode(&f.content).unwrap_or_default(),
                    xxhash3: u64::from_str_radix(&f.hash, 16).unwrap_or(0),
                })
                .collect(),
        }
    }
}

impl From<StorageBulkDownloadResponse> for crate::types::BulkDownloadResponse {
    fn from(storage: StorageBulkDownloadResponse) -> Self {
        Self {
            files: storage.files.into_iter()
                .map(|f| crate::types::FileDownloadResponse {
                    path: f.path,
                    content: base64::engine::general_purpose::STANDARD.encode(&f.content),
                    hash: format!("{:016x}", f.xxhash3),
                })
                .collect(),
        }
    }
}