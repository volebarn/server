//! HTTP request handlers for single file operations
//! 
//! This module implements async handlers for all single file operations:
//! - POST /files/*path - Upload files with RocksDB metadata and file system storage
//! - GET /files/*path - Download files with zero-copy Bytes streaming
//! - PUT /files/*path - Update files atomically using temp files
//! - DELETE /files/*path - Delete files from both RocksDB and file system
//! - HEAD /files/*path - Get file metadata from RocksDB

use crate::error::{ServerError, ServerResult};
use crate::storage::{FileStorage, MetadataStore};
use crate::types::FileMetadataResponse;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Shared application state containing storage components
#[derive(Debug, Clone)]
pub struct AppState {
    pub metadata_store: Arc<MetadataStore>,
    pub file_storage: Arc<FileStorage>,
}

impl AppState {
    /// Create new application state with storage components
    pub async fn new(storage_root: &std::path::Path) -> ServerResult<Self> {
        let metadata_path = storage_root.join("metadata");
        let files_path = storage_root.join("files");
        
        let metadata_store = Arc::new(MetadataStore::new(&metadata_path).await?);
        let file_storage = Arc::new(FileStorage::new(&files_path, metadata_store.clone()).await?);
        
        Ok(Self {
            metadata_store,
            file_storage,
        })
    }
}

/// Upload a file to the server
/// POST /files/*path
pub async fn upload_file(
    Path(file_path): Path<String>,
    State(state): State<AppState>,
    body: Bytes,
) -> ServerResult<Response> {
    debug!("Uploading file: {}", file_path);
    
    // Validate path
    if file_path.is_empty() {
        return Err(ServerError::InvalidPath {
            path: file_path,
            reason: "Path cannot be empty".to_string(),
        });
    }
    
    // Check if file already exists
    if let Some(_existing) = state.metadata_store.get_file_metadata(&file_path).await? {
        return Err(ServerError::FileAlreadyExists { path: file_path });
    }
    
    // Store file content and metadata
    let metadata = state.file_storage.store_file(&file_path, body).await?;
    
    info!("File uploaded successfully: {} ({} bytes)", file_path, metadata.size);
    
    // Return file metadata as JSON
    let response_metadata: FileMetadataResponse = metadata.into();
    let response = Json(response_metadata);
    
    Ok((StatusCode::CREATED, response).into_response())
}

/// Download a file from the server
/// GET /files/*path
pub async fn download_file(
    Path(file_path): Path<String>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    debug!("Downloading file: {}", file_path);
    
    // Get file metadata first
    let metadata = state
        .metadata_store
        .get_file_metadata(&file_path)
        .await?
        .ok_or_else(|| ServerError::FileNotFound { path: file_path.clone() })?;
    
    // Check if it's a directory
    if metadata.is_directory {
        return Err(ServerError::InvalidPath {
            path: file_path,
            reason: "Cannot download a directory".to_string(),
        });
    }
    
    // Retrieve file content
    let content = state.file_storage.retrieve_file(&file_path).await?;
    
    info!("File downloaded successfully: {} ({} bytes)", file_path, content.len());
    
    // Create response with file content and hash header
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-File-Hash",
        HeaderValue::from_str(&format!("{:016x}", metadata.xxhash3))
            .map_err(|e| ServerError::Internal {
                context: format!("Failed to create hash header: {}", e),
            })?,
    );
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/octet-stream"),
    );
    
    Ok((StatusCode::OK, headers, Body::from(content)).into_response())
}

/// Update an existing file
/// PUT /files/*path
pub async fn update_file(
    Path(file_path): Path<String>,
    State(state): State<AppState>,
    body: Bytes,
) -> ServerResult<Response> {
    debug!("Updating file: {}", file_path);
    
    // Check if file exists
    let existing_metadata = state
        .metadata_store
        .get_file_metadata(&file_path)
        .await?
        .ok_or_else(|| ServerError::FileNotFound { path: file_path.clone() })?;
    
    // Check if it's a directory
    if existing_metadata.is_directory {
        return Err(ServerError::InvalidPath {
            path: file_path,
            reason: "Cannot update a directory".to_string(),
        });
    }
    
    // Delete old file content if it exists
    if let Err(e) = state.file_storage.delete_file(&file_path).await {
        warn!("Failed to delete old file content during update: {}", e);
        // Continue with update - the new content will overwrite
    }
    
    // Store new file content and metadata
    let metadata = state.file_storage.store_file(&file_path, body).await?;
    
    info!("File updated successfully: {} ({} bytes)", file_path, metadata.size);
    
    // Return file metadata as JSON
    let response_metadata: FileMetadataResponse = metadata.into();
    let response = Json(response_metadata);
    
    Ok((StatusCode::OK, response).into_response())
}

/// Delete a file from the server
/// DELETE /files/*path
pub async fn delete_file(
    Path(file_path): Path<String>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    debug!("Deleting file: {}", file_path);
    
    // Check if file exists and get metadata
    let metadata = state
        .metadata_store
        .get_file_metadata(&file_path)
        .await?
        .ok_or_else(|| ServerError::FileNotFound { path: file_path.clone() })?;
    
    // Check if it's a directory
    if metadata.is_directory {
        return Err(ServerError::InvalidPath {
            path: file_path,
            reason: "Cannot delete a directory using file endpoint".to_string(),
        });
    }
    
    // Delete file content and metadata
    state.file_storage.delete_file(&file_path).await?;
    
    info!("File deleted successfully: {}", file_path);
    
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Get file metadata without downloading content
/// HEAD /files/*path
pub async fn get_file_metadata(
    Path(file_path): Path<String>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    debug!("Getting metadata for file: {}", file_path);
    
    // Get file metadata
    let metadata = state
        .metadata_store
        .get_file_metadata(&file_path)
        .await?
        .ok_or_else(|| ServerError::FileNotFound { path: file_path.clone() })?;
    
    // Create response headers with metadata
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-File-Size",
        HeaderValue::from_str(&metadata.size.to_string())
            .map_err(|e| ServerError::Internal {
                context: format!("Failed to create size header: {}", e),
            })?,
    );
    headers.insert(
        "X-File-Hash",
        HeaderValue::from_str(&format!("{:016x}", metadata.xxhash3))
            .map_err(|e| ServerError::Internal {
                context: format!("Failed to create hash header: {}", e),
            })?,
    );
    
    // Convert SystemTime to RFC3339 string for HTTP header
    let modified_timestamp = metadata
        .modified
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    headers.insert(
        "X-Modified-Time",
        HeaderValue::from_str(&modified_timestamp.to_string())
            .map_err(|e| ServerError::Internal {
                context: format!("Failed to create modified time header: {}", e),
            })?,
    );
    
    info!("File metadata retrieved: {} ({} bytes)", file_path, metadata.size);
    
    Ok((StatusCode::OK, headers).into_response())
}