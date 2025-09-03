//! HTTP request handlers for single file operations and bulk operations
//! 
//! This module implements async handlers for all file operations:
//! - POST /files/*path - Upload files with RocksDB metadata and file system storage
//! - GET /files/*path - Download files with zero-copy Bytes streaming
//! - PUT /files/*path - Update files atomically using temp files
//! - DELETE /files/*path - Delete files from both RocksDB and file system
//! - HEAD /files/*path - Get file metadata from RocksDB
//! - POST /bulk/upload - Bulk upload multiple files with multipart/form-data

use crate::error::{ServerError, ServerResult};
use crate::storage::{FileStorage, MetadataStore};
use crate::types::{FileMetadata, FileMetadataResponse, DirectoryListing, BulkUploadResponse, OperationError, ErrorCode, FileUpload, FileManifestResponse, SyncRequest, SyncRequestApi, SyncPlan, SyncConflict, ConflictResolution, ConflictResolutionStrategy};
use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::time::SystemTime;
use tracing::{debug, info, warn, error};

/// Progress tracking for bulk upload operations
#[derive(Debug)]
pub struct BulkUploadProgress {
    pub total_files: AtomicUsize,
    pub processed_files: AtomicUsize,
    pub successful_files: AtomicUsize,
    pub failed_files: AtomicUsize,
    pub total_bytes: AtomicU64,
    pub processed_bytes: AtomicU64,
}

impl BulkUploadProgress {
    pub fn new() -> Self {
        Self {
            total_files: AtomicUsize::new(0),
            processed_files: AtomicUsize::new(0),
            successful_files: AtomicUsize::new(0),
            failed_files: AtomicUsize::new(0),
            total_bytes: AtomicU64::new(0),
            processed_bytes: AtomicU64::new(0),
        }
    }
}

/// Shared application state containing storage components
#[derive(Debug, Clone)]
pub struct AppState {
    pub metadata_store: Arc<MetadataStore>,
    pub file_storage: Arc<FileStorage>,
    /// Request-scoped progress tracking for bulk operations
    pub bulk_progress: Arc<DashMap<String, Arc<BulkUploadProgress>>>,
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
            bulk_progress: Arc::new(DashMap::new()),
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

/// Bulk upload multiple files using multipart/form-data
/// POST /bulk/upload
pub async fn bulk_upload(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ServerResult<Response> {
    use uuid::Uuid;
    
    let request_id = Uuid::new_v4().to_string();
    debug!("Starting bulk upload with request ID: {}", request_id);
    
    // Create progress tracker for this request
    let progress = Arc::new(BulkUploadProgress::new());
    state.bulk_progress.insert(request_id.clone(), progress.clone());
    
    // Collect all files from multipart data
    let mut files_to_upload = Vec::new();
    let mut total_size = 0u64;
    
    while let Some(field) = multipart.next_field().await.map_err(|e| {
        ServerError::MultipartParsing {
            error: format!("Failed to get next field: {}", e),
        }
    })? {
        let field_name = field.name().unwrap_or("unknown").to_string();
        let file_name = field.file_name().map(|s| s.to_string());
        
        debug!("Processing multipart field: {} (filename: {:?})", field_name, file_name);
        
        // Get file content as bytes
        let content = field.bytes().await.map_err(|e| {
            ServerError::MultipartParsing {
                error: format!("Failed to read field content: {}", e),
            }
        })?;
        
        // Determine file path - use field name as the full path, removing "files/" prefix
        let file_path = if field_name.starts_with("files/") {
            field_name[6..].to_string() // Remove "files/" prefix
        } else if field_name == "files" {
            // If field name is just "files", use the filename
            file_name.unwrap_or("unknown".to_string())
        } else {
            // Use field name as-is
            field_name
        };
        
        // Skip empty files or invalid paths
        if content.is_empty() || file_path.is_empty() {
            warn!("Skipping empty file or invalid path: {}", file_path);
            continue;
        }
        
        total_size += content.len() as u64;
        files_to_upload.push(FileUpload {
            path: file_path,
            content,
        });
    }
    
    if files_to_upload.is_empty() {
        // Clean up progress tracker
        state.bulk_progress.remove(&request_id);
        return Err(ServerError::InvalidPath {
            path: "multipart".to_string(),
            reason: "No valid files found in multipart data".to_string(),
        });
    }
    
    // Update progress with totals
    progress.total_files.store(files_to_upload.len(), Ordering::Relaxed);
    progress.total_bytes.store(total_size, Ordering::Relaxed);
    
    info!("Bulk upload: {} files, {} bytes total", files_to_upload.len(), total_size);
    
    // Process files concurrently but with controlled parallelism
    let mut success_paths = Vec::new();
    let mut failed_operations = Vec::new();
    
    // Use futures for concurrent processing with limited concurrency
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::pin::Pin;
    use std::future::Future;
    
    type UploadFuture = Pin<Box<dyn Future<Output = Result<String, OperationError>> + Send>>;
    let mut upload_futures: FuturesUnordered<UploadFuture> = FuturesUnordered::new();
    const MAX_CONCURRENT_UPLOADS: usize = 10; // Limit concurrent uploads
    
    let mut file_iter = files_to_upload.into_iter();
    
    // Start initial batch of uploads
    for _ in 0..MAX_CONCURRENT_UPLOADS {
        if let Some(file_upload) = file_iter.next() {
            let state_clone = state.clone();
            let progress_clone = progress.clone();
            
            let future: UploadFuture = Box::pin(async move {
                process_single_upload(state_clone, file_upload, progress_clone).await
            });
            upload_futures.push(future);
        }
    }
    
    // Process uploads as they complete and start new ones
    while let Some(result) = upload_futures.next().await {
        match result {
            Ok(path) => {
                success_paths.push(path);
                progress.successful_files.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                failed_operations.push(error);
                progress.failed_files.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        progress.processed_files.fetch_add(1, Ordering::Relaxed);
        
        // Start next upload if available
        if let Some(file_upload) = file_iter.next() {
            let state_clone = state.clone();
            let progress_clone = progress.clone();
            
            let future: UploadFuture = Box::pin(async move {
                process_single_upload(state_clone, file_upload, progress_clone).await
            });
            upload_futures.push(future);
        }
    }
    
    // Clean up progress tracker
    state.bulk_progress.remove(&request_id);
    
    let response = BulkUploadResponse {
        success: success_paths,
        failed: failed_operations,
    };
    
    info!(
        "Bulk upload completed: {} successful, {} failed",
        response.success.len(),
        response.failed.len()
    );
    
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Process a single file upload within a bulk operation
async fn process_single_upload(
    state: AppState,
    file_upload: FileUpload,
    progress: Arc<BulkUploadProgress>,
) -> Result<String, OperationError> {
    let path = file_upload.path.clone();
    let content_size = file_upload.content.len() as u64;
    
    debug!("Processing upload for: {} ({} bytes)", path, content_size);
    
    // Check if file already exists
    if let Ok(Some(_existing)) = state.metadata_store.get_file_metadata(&path).await {
        return Err(OperationError {
            path: path.clone(),
            error: "File already exists".to_string(),
            error_code: ErrorCode::ServerError, // Using ServerError for conflict
        });
    }
    
    // Ensure parent directories exist
    if let Some(parent_path) = get_parent_directory(&path) {
        if let Err(e) = ensure_parent_directories_exist(&state, &parent_path).await {
            return Err(OperationError {
                path: path.clone(),
                error: format!("Failed to create parent directories: {}", e),
                error_code: ErrorCode::StorageError,
            });
        }
    }
    
    // Store file using FileStorage
    match state.file_storage.store_file(&path, file_upload.content).await {
        Ok(_metadata) => {
            progress.processed_bytes.fetch_add(content_size, Ordering::Relaxed);
            debug!("Successfully uploaded: {}", path);
            Ok(path)
        }
        Err(e) => {
            error!("Failed to upload {}: {}", path, e);
            Err(OperationError {
                path: path.clone(),
                error: e.to_string(),
                error_code: e.error_code(),
            })
        }
    }
}

/// Extract parent directory path from a file path
fn get_parent_directory(path: &str) -> Option<String> {
    let path_obj = std::path::Path::new(path);
    path_obj.parent().and_then(|p| {
        let parent_str = p.to_string_lossy();
        if parent_str.is_empty() || parent_str == "." {
            None
        } else {
            Some(parent_str.to_string())
        }
    })
}

/// Ensure all parent directories exist, creating them if necessary
async fn ensure_parent_directories_exist(
    state: &AppState,
    parent_path: &str,
) -> ServerResult<()> {
    let normalized_path = if parent_path.starts_with('/') {
        parent_path.to_string()
    } else {
        format!("/{}", parent_path)
    };
    
    // Check if directory already exists
    if let Ok(Some(metadata)) = state.metadata_store.get_file_metadata(&normalized_path).await {
        if metadata.is_directory {
            return Ok(()); // Directory already exists
        } else {
            return Err(ServerError::InvalidPath {
                path: normalized_path,
                reason: "Path exists but is not a directory".to_string(),
            });
        }
    }
    
    // Create parent directories recursively
    let path_parts: Vec<&str> = normalized_path.trim_start_matches('/').split('/').collect();
    let mut current_path = String::new();
    
    for part in path_parts {
        if part.is_empty() {
            continue;
        }
        
        current_path.push('/');
        current_path.push_str(part);
        
        // Check if this directory level exists
        if let Ok(Some(metadata)) = state.metadata_store.get_file_metadata(&current_path).await {
            if !metadata.is_directory {
                return Err(ServerError::InvalidPath {
                    path: current_path,
                    reason: "Path exists but is not a directory".to_string(),
                });
            }
        } else {
            // Create this directory level
            state.metadata_store.create_directory(&current_path).await?;
            debug!("Created directory: {}", current_path);
        }
    }
    
    Ok(())
}

/// Get complete file manifest from server
/// GET /bulk/manifest
pub async fn get_manifest(
    State(state): State<AppState>,
) -> ServerResult<Response> {
    debug!("Getting complete file manifest");
    
    // Get all files from metadata store using RocksDB iteration
    let all_files = get_all_files_from_metadata(&state).await?;
    
    // Convert to API-friendly manifest format
    let mut files = std::collections::HashMap::new();
    for metadata in all_files {
        let response_metadata: FileMetadataResponse = metadata.clone().into();
        files.insert(metadata.path.clone(), response_metadata);
    }
    
    let manifest = FileManifestResponse { files };
    
    info!("File manifest generated: {} files", manifest.files.len());
    
    Ok((StatusCode::OK, Json(manifest)).into_response())
}

/// Compare client manifest with server and return sync operations
/// POST /bulk/sync
pub async fn sync_with_manifest(
    State(state): State<AppState>,
    Json(api_sync_request): Json<SyncRequestApi>,
) -> ServerResult<Response> {
    debug!("Processing sync request with {} client files", api_sync_request.client_manifest.files.len());
    
    // Convert API request to internal format
    let sync_request: SyncRequest = api_sync_request.try_into().map_err(|e| {
        crate::error::ServerError::InvalidRequest {
            field: "sync_request".to_string(),
            error: format!("Failed to convert sync request: {}", e),
        }
    })?;
    
    // Get server manifest
    let server_files = get_all_files_from_metadata(&state).await?;
    let mut server_manifest = std::collections::HashMap::new();
    for metadata in server_files {
        server_manifest.insert(metadata.path.clone(), metadata);
    }
    
    // Generate sync plan where server is authoritative
    let sync_plan = generate_sync_plan(
        &sync_request.client_manifest.files,
        &server_manifest,
        &sync_request.conflict_resolution,
    ).await?;
    
    info!(
        "Sync plan generated: {} uploads, {} downloads, {} deletes, {} conflicts",
        sync_plan.client_upload.len(),
        sync_plan.client_download.len(),
        sync_plan.client_delete.len(),
        sync_plan.conflicts.len()
    );
    
    Ok((StatusCode::OK, Json(sync_plan)).into_response())
}

/// Get all files from metadata store using efficient RocksDB iteration
async fn get_all_files_from_metadata(state: &AppState) -> ServerResult<Vec<FileMetadata>> {
    
    // Use the metadata store's efficient iteration method
    let all_file_paths = state.metadata_store.get_all_files_in_directory("/").await?;
    let mut all_files = Vec::new();
    
    // Get metadata for each file
    for path in all_file_paths {
        if let Some(metadata) = state.metadata_store.get_file_metadata(&path).await? {
            all_files.push(metadata);
        }
    }
    
    // Also include root directory files that might not be captured by get_all_files_in_directory
    let root_files = state.metadata_store.list_directory("/").await?;
    for metadata in root_files {
        // Only add if not already in the list (avoid duplicates)
        if !all_files.iter().any(|f| f.path == metadata.path) {
            all_files.push(metadata);
        }
    }
    
    Ok(all_files)
}

/// Generate sync plan where server state is authoritative
async fn generate_sync_plan(
    client_files: &std::collections::HashMap<String, FileMetadata>,
    server_files: &std::collections::HashMap<String, FileMetadata>,
    conflict_resolution: &ConflictResolutionStrategy,
) -> ServerResult<SyncPlan> {
    let mut sync_plan = SyncPlan::new();
    
    // Find files that exist on server but not on client (client should download)
    for (server_path, server_metadata) in server_files {
        if let Some(client_metadata) = client_files.get(server_path) {
            // File exists on both sides, check for conflicts
            if server_metadata.xxhash3 != client_metadata.xxhash3 {
                // Hash mismatch indicates different content
                let conflict = SyncConflict {
                    path: server_path.clone(),
                    local_modified: client_metadata.modified,
                    remote_modified: server_metadata.modified,
                    resolution: resolve_conflict(
                        client_metadata,
                        server_metadata,
                        conflict_resolution,
                    ),
                };
                
                // Based on conflict resolution, add to appropriate operation list
                match conflict.resolution {
                    ConflictResolution::UseLocal => {
                        sync_plan.client_upload.push(server_path.clone());
                    }
                    ConflictResolution::UseRemote => {
                        sync_plan.client_download.push(server_path.clone());
                    }
                    ConflictResolution::UseNewer => {
                        if client_metadata.modified > server_metadata.modified {
                            sync_plan.client_upload.push(server_path.clone());
                        } else {
                            sync_plan.client_download.push(server_path.clone());
                        }
                    }
                    ConflictResolution::Manual => {
                        sync_plan.conflicts.push(conflict);
                    }
                }
            }
            // If hashes match, files are identical - no action needed
        } else {
            // File exists on server but not on client
            if server_metadata.is_directory {
                sync_plan.client_create_dirs.push(server_path.clone());
            } else {
                sync_plan.client_download.push(server_path.clone());
            }
        }
    }
    
    // Find files that exist on client but not on server
    for (client_path, client_metadata) in client_files {
        if !server_files.contains_key(client_path) {
            // File exists on client but not on server
            // Since server is authoritative, client should delete local files that don't exist on server
            if !client_metadata.is_directory {
                sync_plan.client_delete.push(client_path.clone());
            }
        }
    }
    
    Ok(sync_plan)
}

/// Resolve conflict between local and remote files based on strategy
fn resolve_conflict(
    _client_metadata: &FileMetadata,
    _server_metadata: &FileMetadata,
    strategy: &ConflictResolutionStrategy,
) -> ConflictResolution {
    match strategy {
        ConflictResolutionStrategy::PreferLocal => ConflictResolution::UseLocal,
        ConflictResolutionStrategy::PreferRemote => ConflictResolution::UseRemote,
        ConflictResolutionStrategy::PreferNewer => ConflictResolution::UseNewer,
        ConflictResolutionStrategy::Manual => ConflictResolution::Manual,
    }
}

/// Simple glob-style pattern matching for file search
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

/// Bulk download multiple files using concurrent retrieval and zero-copy Bytes
/// POST /bulk/download
pub async fn bulk_download(
    State(state): State<AppState>,
    Json(request): Json<crate::types::BulkDownloadRequest>,
) -> ServerResult<Response> {
    use uuid::Uuid;
    
    let request_id = Uuid::new_v4().to_string();
    debug!("Starting bulk download with request ID: {} for {} files", request_id, request.paths.len());
    
    if request.paths.is_empty() {
        return Err(ServerError::InvalidPath {
            path: "bulk_download".to_string(),
            reason: "No paths provided for download".to_string(),
        });
    }
    
    // Create progress tracker for this request
    let progress = Arc::new(BulkUploadProgress::new()); // Reuse progress structure
    state.bulk_progress.insert(request_id.clone(), progress.clone());
    progress.total_files.store(request.paths.len(), Ordering::Relaxed);
    
    // Process downloads concurrently with controlled parallelism
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::pin::Pin;
    use std::future::Future;
    
    type DownloadFuture = Pin<Box<dyn Future<Output = Result<crate::types::FileDownloadResponse, OperationError>> + Send>>;
    let mut download_futures: FuturesUnordered<DownloadFuture> = FuturesUnordered::new();
    const MAX_CONCURRENT_DOWNLOADS: usize = 10;
    
    let mut path_iter = request.paths.into_iter();
    let mut successful_downloads = Vec::new();
    let mut failed_operations = Vec::new();
    
    // Start initial batch of downloads
    for _ in 0..MAX_CONCURRENT_DOWNLOADS {
        if let Some(path) = path_iter.next() {
            let state_clone = state.clone();
            let progress_clone = progress.clone();
            
            let future: DownloadFuture = Box::pin(async move {
                process_single_download(state_clone, path, progress_clone).await
            });
            download_futures.push(future);
        }
    }
    
    // Process downloads as they complete and start new ones
    while let Some(result) = download_futures.next().await {
        match result {
            Ok(file_download) => {
                successful_downloads.push(file_download);
                progress.successful_files.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                failed_operations.push(error);
                progress.failed_files.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        progress.processed_files.fetch_add(1, Ordering::Relaxed);
        
        // Start next download if available
        if let Some(path) = path_iter.next() {
            let state_clone = state.clone();
            let progress_clone = progress.clone();
            
            let future: DownloadFuture = Box::pin(async move {
                process_single_download(state_clone, path, progress_clone).await
            });
            download_futures.push(future);
        }
    }
    
    // Clean up progress tracker
    state.bulk_progress.remove(&request_id);
    
    let response = crate::types::BulkDownloadResponse {
        files: successful_downloads,
    };
    
    info!(
        "Bulk download completed: {} successful, {} failed",
        response.files.len(),
        failed_operations.len()
    );
    
    // If there were failures, include them in the response
    if !failed_operations.is_empty() {
        // For now, we'll log the failures but still return successful downloads
        for error in &failed_operations {
            warn!("Download failed for {}: {}", error.path, error.error);
        }
    }
    
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Process a single file download within a bulk operation
async fn process_single_download(
    state: AppState,
    path: String,
    progress: Arc<BulkUploadProgress>,
) -> Result<crate::types::FileDownloadResponse, OperationError> {
    debug!("Processing download for: {}", path);
    
    // Get file metadata first
    let metadata = match state.metadata_store.get_file_metadata(&path).await {
        Ok(Some(meta)) => meta,
        Ok(None) => {
            return Err(OperationError {
                path: path.clone(),
                error: "File not found".to_string(),
                error_code: ErrorCode::FileNotFound,
            });
        }
        Err(e) => {
            return Err(OperationError {
                path: path.clone(),
                error: e.to_string(),
                error_code: e.error_code(),
            });
        }
    };
    
    // Check if it's a directory
    if metadata.is_directory {
        return Err(OperationError {
            path: path.clone(),
            error: "Cannot download a directory".to_string(),
            error_code: ErrorCode::InvalidPath,
        });
    }
    
    // Retrieve file content
    match state.file_storage.retrieve_file(&path).await {
        Ok(content) => {
            let content_size = content.len() as u64;
            progress.processed_bytes.fetch_add(content_size, Ordering::Relaxed);
            
            // Convert to base64 for JSON transport
            use base64::{Engine as _, engine::general_purpose};
            let content_base64 = general_purpose::STANDARD.encode(&content);
            
            debug!("Successfully downloaded: {} ({} bytes)", path, content_size);
            Ok(crate::types::FileDownloadResponse {
                path,
                content: content_base64,
                hash: format!("{:016x}", metadata.xxhash3),
            })
        }
        Err(e) => {
            error!("Failed to download {}: {}", path, e);
            Err(OperationError {
                path: path.clone(),
                error: e.to_string(),
                error_code: e.error_code(),
            })
        }
    }
}

/// Bulk delete multiple files and directories using RocksDB transactions and FileStorage cleanup
/// DELETE /bulk/delete
pub async fn bulk_delete(
    State(state): State<AppState>,
    Json(request): Json<crate::types::BulkDeleteRequest>,
) -> ServerResult<Response> {
    use uuid::Uuid;
    
    let request_id = Uuid::new_v4().to_string();
    debug!("Starting bulk delete with request ID: {} for {} paths", request_id, request.paths.len());
    
    if request.paths.is_empty() {
        return Err(ServerError::InvalidPath {
            path: "bulk_delete".to_string(),
            reason: "No paths provided for deletion".to_string(),
        });
    }
    
    // Create progress tracker for this request
    let progress = Arc::new(BulkUploadProgress::new());
    state.bulk_progress.insert(request_id.clone(), progress.clone());
    progress.total_files.store(request.paths.len(), Ordering::Relaxed);
    
    // Process deletions concurrently with controlled parallelism
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::pin::Pin;
    use std::future::Future;
    
    type DeleteFuture = Pin<Box<dyn Future<Output = Result<String, OperationError>> + Send>>;
    let mut delete_futures: FuturesUnordered<DeleteFuture> = FuturesUnordered::new();
    const MAX_CONCURRENT_DELETES: usize = 10;
    
    let mut path_iter = request.paths.into_iter();
    let mut successful_deletes = Vec::new();
    let mut failed_operations = Vec::new();
    
    // Start initial batch of deletions
    for _ in 0..MAX_CONCURRENT_DELETES {
        if let Some(path) = path_iter.next() {
            let state_clone = state.clone();
            let progress_clone = progress.clone();
            
            let future: DeleteFuture = Box::pin(async move {
                process_single_delete(state_clone, path, progress_clone).await
            });
            delete_futures.push(future);
        }
    }
    
    // Process deletions as they complete and start new ones
    while let Some(result) = delete_futures.next().await {
        match result {
            Ok(path) => {
                successful_deletes.push(path);
                progress.successful_files.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                failed_operations.push(error);
                progress.failed_files.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        progress.processed_files.fetch_add(1, Ordering::Relaxed);
        
        // Start next deletion if available
        if let Some(path) = path_iter.next() {
            let state_clone = state.clone();
            let progress_clone = progress.clone();
            
            let future: DeleteFuture = Box::pin(async move {
                process_single_delete(state_clone, path, progress_clone).await
            });
            delete_futures.push(future);
        }
    }
    
    // Clean up progress tracker
    state.bulk_progress.remove(&request_id);
    
    let response = crate::types::BulkDeleteResponse {
        success: successful_deletes,
        failed: failed_operations,
    };
    
    info!(
        "Bulk delete completed: {} successful, {} failed",
        response.success.len(),
        response.failed.len()
    );
    
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Process a single file/directory deletion within a bulk operation
async fn process_single_delete(
    state: AppState,
    path: String,
    _progress: Arc<BulkUploadProgress>,
) -> Result<String, OperationError> {
    debug!("Processing delete for: {}", path);
    
    // Get metadata to determine if it's a file or directory
    let metadata = match state.metadata_store.get_file_metadata(&path).await {
        Ok(Some(meta)) => meta,
        Ok(None) => {
            return Err(OperationError {
                path: path.clone(),
                error: "File or directory not found".to_string(),
                error_code: ErrorCode::FileNotFound,
            });
        }
        Err(e) => {
            return Err(OperationError {
                path: path.clone(),
                error: e.to_string(),
                error_code: e.error_code(),
            });
        }
    };
    
    if metadata.is_directory {
        // Delete directory recursively
        match state.metadata_store.delete_directory_recursive(&path).await {
            Ok(deleted_paths) => {
                // Delete file content for all deleted files
                for deleted_path in &deleted_paths {
                    if let Ok(Some(file_metadata)) = state.metadata_store.get_file_metadata(deleted_path).await {
                        if !file_metadata.is_directory {
                            // Try to delete file content, but don't fail if it's already gone
                            if let Err(e) = state.file_storage.delete_file(deleted_path).await {
                                warn!("Failed to delete file content during bulk directory deletion: {}", e);
                            }
                        }
                    }
                }
                debug!("Successfully deleted directory: {} ({} items)", path, deleted_paths.len());
                Ok(path)
            }
            Err(e) => {
                error!("Failed to delete directory {}: {}", path, e);
                Err(OperationError {
                    path: path.clone(),
                    error: e.to_string(),
                    error_code: e.error_code(),
                })
            }
        }
    } else {
        // Delete single file
        match state.file_storage.delete_file(&path).await {
            Ok(true) => {
                debug!("Successfully deleted file: {}", path);
                Ok(path)
            }
            Ok(false) => {
                // File didn't exist, but that's not necessarily an error for bulk operations
                debug!("File not found during delete: {}", path);
                Ok(path)
            }
            Err(e) => {
                error!("Failed to delete file {}: {}", path, e);
                Err(OperationError {
                    path: path.clone(),
                    error: e.to_string(),
                    error_code: e.error_code(),
                })
            }
        }
    }
}

/// Move a file or directory to a new location
/// POST /files/move
pub async fn move_file_or_directory(
    State(state): State<AppState>,
    Json(request): Json<crate::types::MoveRequest>,
) -> ServerResult<Response> {
    debug!("Moving {} to {}", request.from_path, request.to_path);
    
    // Validate paths
    if request.from_path.is_empty() || request.to_path.is_empty() {
        return Err(ServerError::InvalidPath {
            path: "move_request".to_string(),
            reason: "Source and destination paths cannot be empty".to_string(),
        });
    }
    
    if request.from_path == request.to_path {
        return Err(ServerError::InvalidPath {
            path: request.from_path,
            reason: "Source and destination paths cannot be the same".to_string(),
        });
    }
    
    // Check if source exists
    let source_metadata = state
        .metadata_store
        .get_file_metadata(&request.from_path)
        .await?
        .ok_or_else(|| ServerError::FileNotFound { path: request.from_path.clone() })?;
    
    // Check if destination already exists
    if state.metadata_store.get_file_metadata(&request.to_path).await?.is_some() {
        return Err(ServerError::FileAlreadyExists { path: request.to_path });
    }
    
    // Ensure parent directories exist for destination
    if let Some(parent_path) = get_parent_directory(&request.to_path) {
        ensure_parent_directories_exist(&state, &parent_path).await?;
    }
    
    if source_metadata.is_directory {
        // Move directory: get all files in the directory and update their paths
        let files_in_dir = state.metadata_store.get_all_files_in_directory(&request.from_path).await?;
        
        // Create new directory at destination
        state.metadata_store.create_directory(&request.to_path).await?;
        
        // Move all files in the directory
        for file_path in files_in_dir {
            let relative_path = file_path.strip_prefix(&format!("{}/", request.from_path))
                .unwrap_or(&file_path);
            let new_file_path = format!("{}/{}", request.to_path, relative_path);
            
            if let Ok(Some(file_metadata)) = state.metadata_store.get_file_metadata(&file_path).await {
                // Create new metadata with updated path
                let mut new_metadata = file_metadata.clone();
                new_metadata.path = new_file_path.clone();
                new_metadata.name = std::path::Path::new(&new_file_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&new_file_path)
                    .to_string();
                
                // Store new metadata
                state.metadata_store.put_file_metadata(&new_metadata).await?;
                
                // Delete old metadata
                state.metadata_store.delete_file_metadata(&file_path).await?;
            }
        }
        
        // Delete source directory
        state.metadata_store.delete_directory_recursive(&request.from_path).await?;
        
    } else {
        // Move single file: update metadata with new path, file content stays the same
        let mut new_metadata = source_metadata.clone();
        new_metadata.path = request.to_path.clone();
        new_metadata.name = std::path::Path::new(&request.to_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&request.to_path)
            .to_string();
        new_metadata.modified = SystemTime::now();
        
        // Store new metadata
        state.metadata_store.put_file_metadata(&new_metadata).await?;
        
        // Delete old metadata
        state.metadata_store.delete_file_metadata(&request.from_path).await?;
    }
    
    info!("Successfully moved {} to {}", request.from_path, request.to_path);
    
    Ok(StatusCode::OK.into_response())
}

/// Copy a file or directory to a new location
/// POST /files/copy
pub async fn copy_file_or_directory(
    State(state): State<AppState>,
    Json(request): Json<crate::types::CopyRequest>,
) -> ServerResult<Response> {
    debug!("Copying {} to {}", request.from_path, request.to_path);
    
    // Validate paths
    if request.from_path.is_empty() || request.to_path.is_empty() {
        return Err(ServerError::InvalidPath {
            path: "copy_request".to_string(),
            reason: "Source and destination paths cannot be empty".to_string(),
        });
    }
    
    if request.from_path == request.to_path {
        return Err(ServerError::InvalidPath {
            path: request.from_path,
            reason: "Source and destination paths cannot be the same".to_string(),
        });
    }
    
    // Check if source exists
    let source_metadata = state
        .metadata_store
        .get_file_metadata(&request.from_path)
        .await?
        .ok_or_else(|| ServerError::FileNotFound { path: request.from_path.clone() })?;
    
    // Check if destination already exists
    if state.metadata_store.get_file_metadata(&request.to_path).await?.is_some() {
        return Err(ServerError::FileAlreadyExists { path: request.to_path });
    }
    
    // Ensure parent directories exist for destination
    if let Some(parent_path) = get_parent_directory(&request.to_path) {
        ensure_parent_directories_exist(&state, &parent_path).await?;
    }
    
    if source_metadata.is_directory {
        // Copy directory: create new directory and copy all files
        state.metadata_store.create_directory(&request.to_path).await?;
        
        let files_in_dir = state.metadata_store.get_all_files_in_directory(&request.from_path).await?;
        
        // Copy all files in the directory
        for file_path in files_in_dir {
            let relative_path = file_path.strip_prefix(&format!("{}/", request.from_path))
                .unwrap_or(&file_path);
            let new_file_path = format!("{}/{}", request.to_path, relative_path);
            
            if let Ok(Some(file_metadata)) = state.metadata_store.get_file_metadata(&file_path).await {
                if file_metadata.is_directory {
                    // Create subdirectory
                    state.metadata_store.create_directory(&new_file_path).await?;
                } else {
                    // Copy file content and create new metadata
                    let content = state.file_storage.retrieve_file(&file_path).await?;
                    state.file_storage.store_file(&new_file_path, content).await?;
                }
            }
        }
        
    } else {
        // Copy single file: retrieve content and store with new path
        let content = state.file_storage.retrieve_file(&request.from_path).await?;
        state.file_storage.store_file(&request.to_path, content).await?;
    }
    
    info!("Successfully copied {} to {}", request.from_path, request.to_path);
    
    Ok(StatusCode::CREATED.into_response())
}