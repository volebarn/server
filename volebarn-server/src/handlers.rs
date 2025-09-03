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
use crate::types::{FileMetadataResponse, DirectoryListing};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use std::collections::HashMap;
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

/// List directory contents or root directory
/// GET /files or GET /files/*path
pub async fn list_directory(
    path: Option<Path<String>>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    let dir_path = path.map(|p| p.0).unwrap_or_else(|| "/".to_string());
    debug!("Listing directory: {}", dir_path);
    
    // Validate path
    if dir_path.is_empty() {
        return Err(ServerError::InvalidPath {
            path: dir_path,
            reason: "Path cannot be empty".to_string(),
        });
    }
    
    // Check if the path exists and is a directory (if not root)
    if dir_path != "/" {
        if let Some(metadata) = state.metadata_store.get_file_metadata(&dir_path).await? {
            if !metadata.is_directory {
                return Err(ServerError::InvalidPath {
                    path: dir_path,
                    reason: "Path is not a directory".to_string(),
                });
            }
        } else {
            return Err(ServerError::FileNotFound { path: dir_path });
        }
    }
    
    // Get directory contents
    let entries = state.metadata_store.list_directory(&dir_path).await?;
    
    info!("Directory listed successfully: {} ({} entries)", dir_path, entries.len());
    
    // Convert to response format
    let response_entries: Vec<FileMetadataResponse> = entries
        .into_iter()
        .map(|metadata| metadata.into())
        .collect();
    
    // Use normalized path for consistency (storage layer normalizes paths)
    let normalized_path = if dir_path == "/" {
        "/".to_string()
    } else {
        let trimmed = dir_path.trim_start_matches('/').trim_end_matches('/');
        if trimmed.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", trimmed)
        }
    };
    
    let listing = DirectoryListing {
        path: normalized_path,
        entries: response_entries.into_iter().map(|r| r.try_into()).collect::<Result<Vec<_>, _>>()
            .map_err(|e| ServerError::Internal {
                context: format!("Failed to convert directory listing: {}", e),
            })?,
    };
    
    Ok((StatusCode::OK, Json(listing)).into_response())
}

/// Create a directory
/// POST /directories/*path
pub async fn create_directory(
    Path(dir_path): Path<String>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    debug!("Creating directory: {}", dir_path);
    
    // Validate path
    if dir_path.is_empty() {
        return Err(ServerError::InvalidPath {
            path: dir_path,
            reason: "Path cannot be empty".to_string(),
        });
    }
    
    // Create directory in metadata store
    state.metadata_store.create_directory(&dir_path).await?;
    
    info!("Directory created successfully: {}", dir_path);
    
    Ok(StatusCode::CREATED.into_response())
}

/// Delete a directory recursively
/// DELETE /directories/*path
pub async fn delete_directory(
    Path(dir_path): Path<String>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    debug!("Deleting directory recursively: {}", dir_path);
    
    // Validate path
    if dir_path.is_empty() {
        return Err(ServerError::InvalidPath {
            path: dir_path,
            reason: "Path cannot be empty".to_string(),
        });
    }
    
    // Check if directory exists
    let metadata = state
        .metadata_store
        .get_file_metadata(&dir_path)
        .await?
        .ok_or_else(|| ServerError::FileNotFound { path: dir_path.clone() })?;
    
    if !metadata.is_directory {
        return Err(ServerError::InvalidPath {
            path: dir_path,
            reason: "Path is not a directory".to_string(),
        });
    }
    
    // Delete directory recursively (this will also delete file content)
    let deleted_paths = state.metadata_store.delete_directory_recursive(&dir_path).await?;
    
    // Delete file content for all deleted files
    for path in &deleted_paths {
        if let Ok(Some(file_metadata)) = state.metadata_store.get_file_metadata(path).await {
            if !file_metadata.is_directory {
                // Try to delete file content, but don't fail if it's already gone
                if let Err(e) = state.file_storage.delete_file(path).await {
                    warn!("Failed to delete file content during directory deletion: {}", e);
                }
            }
        }
    }
    
    info!("Directory deleted successfully: {} ({} items)", dir_path, deleted_paths.len());
    
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Search for files matching a pattern
/// GET /search?pattern=<pattern>&path=<path>&recursive=<bool>
pub async fn search_files(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    let pattern = params.get("pattern")
        .ok_or_else(|| ServerError::InvalidPath {
            path: "query".to_string(),
            reason: "Missing 'pattern' query parameter".to_string(),
        })?;
    
    let search_path = params.get("path").cloned().unwrap_or_else(|| "/".to_string());
    let recursive = params.get("recursive")
        .map(|s| s.parse::<bool>().unwrap_or(true))
        .unwrap_or(true);
    
    debug!("Searching for files: pattern='{}', path='{}', recursive={}", pattern, search_path, recursive);
    
    // Get all files in the search path
    let all_files = if recursive {
        // Get all files recursively
        state.metadata_store.get_all_files_in_directory(&search_path).await
            .unwrap_or_else(|_| Vec::new())
    } else {
        // Get only direct children
        state.metadata_store.list_directory(&search_path).await
            .map(|entries| entries.into_iter().map(|e| e.path).collect())
            .unwrap_or_else(|_| Vec::new())
    };
    
    // Filter files by pattern using glob-style matching
    let mut matching_files = Vec::new();
    for file_path in all_files {
        if let Ok(Some(metadata)) = state.metadata_store.get_file_metadata(&file_path).await {
            if matches_pattern(&metadata.name, pattern) || matches_pattern(&metadata.path, pattern) {
                matching_files.push(metadata);
            }
        }
    }
    
    // Sort results by path for consistent ordering
    matching_files.sort_by(|a, b| a.path.cmp(&b.path));
    
    info!("Search completed: pattern='{}', found {} matches", pattern, matching_files.len());
    
    // Convert to response format
    let response_files: Vec<FileMetadataResponse> = matching_files
        .into_iter()
        .map(|metadata| metadata.into())
        .collect();
    
    Ok((StatusCode::OK, Json(response_files)).into_response())
}

/// Handle GET requests for files or directories
/// This function determines whether to download a file or list a directory
pub async fn get_file_or_directory(
    Path(path): Path<String>,
    State(state): State<AppState>,
) -> ServerResult<Response> {
    debug!("GET request for path: {}", path);
    
    // Check if path exists and determine if it's a file or directory
    if let Some(metadata) = state.metadata_store.get_file_metadata(&path).await? {
        if metadata.is_directory {
            // It's a directory, list its contents
            list_directory(Some(Path(path)), State(state)).await
        } else {
            // It's a file, download it
            download_file(Path(path), State(state)).await
        }
    } else {
        // Path doesn't exist, return 404
        Err(ServerError::FileNotFound { path })
    }
}

/// Simple glob-style pattern matching
fn matches_pattern(text: &str, pattern: &str) -> bool {
    // Simple implementation supporting * and ? wildcards
    if pattern == "*" {
        return true;
    }
    
    // Convert glob pattern to regex-like matching
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();
    
    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                // Match zero or more characters
                if pattern_chars.peek().is_none() {
                    // Pattern ends with *, match rest of text
                    return true;
                }
                
                // Try to match the rest of the pattern at each position
                let remaining_pattern: String = pattern_chars.collect();
                while text_chars.peek().is_some() {
                    let remaining_text: String = text_chars.clone().collect();
                    if matches_pattern(&remaining_text, &remaining_pattern) {
                        return true;
                    }
                    text_chars.next();
                }
                return matches_pattern("", &remaining_pattern);
            }
            '?' => {
                // Match exactly one character
                if text_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                // Match exact character
                if text_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }
    
    // Pattern consumed, text should also be consumed
    text_chars.peek().is_none()
}