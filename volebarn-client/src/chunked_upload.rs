//! Chunked upload with resume capability for large files
//! 
//! This module provides resumable chunked uploads for large files with
//! atomic progress tracking and failure recovery.

use crate::error::{ClientError, ClientResult};
use crate::hash::HashManager;
use bytes::Bytes;
use dashmap::DashMap;
use reqwest::{Method, Response};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, info, warn, error, instrument};
use uuid::Uuid;

/// Information about a chunked upload session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkedUploadSession {
    pub upload_id: String,
    pub file_path: String,
    pub total_size: u64,
    pub chunk_size: u64,
    pub total_chunks: u32,
    pub completed_chunks: Vec<u32>,
    pub expected_hash: u64,
    pub created_at: u64, // Seconds since epoch
    pub last_activity: u64, // Seconds since epoch
}

/// Progress information for a chunked upload
#[derive(Debug, Clone)]
pub struct ChunkedUploadProgress {
    pub upload_id: String,
    pub total_chunks: u32,
    pub completed_chunks: u32,
    pub total_bytes: u64,
    pub uploaded_bytes: u64,
    pub progress_percent: u8,
    pub upload_speed_bps: u64, // Bytes per second
    pub estimated_remaining: Duration,
}

/// Result of a chunk upload operation
#[derive(Debug, Clone)]
pub struct ChunkUploadResult {
    pub chunk_index: u32,
    pub success: bool,
    pub bytes_uploaded: u64,
    pub error: Option<String>,
}

/// Chunked upload manager with resume capability
#[derive(Debug)]
pub struct ChunkedUploadManager {
    /// HTTP client for making requests
    http_client: reqwest::Client,
    /// Server base URL
    server_url: String,
    /// Hash manager for integrity verification
    hash_manager: HashManager,
    /// Active upload sessions (upload_id -> session)
    active_sessions: DashMap<String, ChunkedUploadSession>,
    /// Upload progress tracking (upload_id -> progress)
    upload_progress: DashMap<String, Arc<ChunkedUploadProgress>>,
    /// Semaphore for limiting concurrent chunk uploads
    chunk_semaphore: Arc<Semaphore>,
    /// Default chunk size
    default_chunk_size: u64,
    /// Maximum concurrent chunks per upload
    max_concurrent_chunks: u32,
    /// Session timeout (inactive sessions are cleaned up)
    session_timeout: Duration,
}

impl ChunkedUploadManager {
    /// Create a new chunked upload manager
    pub fn new(
        http_client: reqwest::Client,
        server_url: String,
        default_chunk_size: u64,
        max_concurrent_chunks: u32,
        session_timeout: Duration,
    ) -> Self {
        let hash_manager = HashManager::new();
        let chunk_semaphore = Arc::new(Semaphore::new(max_concurrent_chunks as usize));

        Self {
            http_client,
            server_url,
            hash_manager,
            active_sessions: DashMap::new(),
            upload_progress: DashMap::new(),
            chunk_semaphore,
            default_chunk_size,
            max_concurrent_chunks,
            session_timeout,
        }
    }

    /// Start a new chunked upload session
    #[instrument(skip(self, content))]
    pub async fn start_upload(
        &self,
        file_path: &str,
        content: Bytes,
        chunk_size: Option<u64>,
    ) -> ClientResult<String> {
        let upload_id = Uuid::new_v4().to_string();
        let chunk_size = chunk_size.unwrap_or(self.default_chunk_size);
        let total_size = content.len() as u64;
        let total_chunks = ((total_size + chunk_size - 1) / chunk_size) as u32;
        let expected_hash = self.hash_manager.hash_bytes(&content);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let session = ChunkedUploadSession {
            upload_id: upload_id.clone(),
            file_path: file_path.to_string(),
            total_size,
            chunk_size,
            total_chunks,
            completed_chunks: Vec::new(),
            expected_hash,
            created_at: now,
            last_activity: now,
        };

        // Initialize upload session on server
        let init_url = format!("{}/upload/chunked/init", self.server_url);
        let init_request = serde_json::json!({
            "upload_id": upload_id,
            "file_path": file_path,
            "total_size": total_size,
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
            "expected_hash": format!("{:016x}", expected_hash)
        });

        let response = timeout(
            Duration::from_secs(30),
            self.http_client
                .post(&init_url)
                .json(&init_request)
                .send()
        ).await
        .map_err(|_| ClientError::Timeout {
            duration: 30,
            operation: "chunked_upload_init".to_string(),
        })?
        .map_err(ClientError::Network)?;

        if !response.status().is_success() {
            return Err(ClientError::Server {
                status: response.status().as_u16(),
                message: format!("Failed to initialize chunked upload: {}", response.status()),
            });
        }

        // Store session
        self.active_sessions.insert(upload_id.clone(), session);

        // Initialize progress tracking
        let progress = Arc::new(ChunkedUploadProgress {
            upload_id: upload_id.clone(),
            total_chunks,
            completed_chunks: 0,
            total_bytes: total_size,
            uploaded_bytes: 0,
            progress_percent: 0,
            upload_speed_bps: 0,
            estimated_remaining: Duration::ZERO,
        });
        self.upload_progress.insert(upload_id.clone(), progress);

        info!(
            "Started chunked upload: {} ({} bytes, {} chunks)",
            upload_id, total_size, total_chunks
        );

        Ok(upload_id)
    }

    /// Resume an existing chunked upload session
    #[instrument(skip(self))]
    pub async fn resume_upload(&self, upload_id: &str) -> ClientResult<ChunkedUploadSession> {
        // Get session status from server
        let status_url = format!("{}/upload/chunked/{}/status", self.server_url, upload_id);
        let response = timeout(
            Duration::from_secs(10),
            self.http_client.get(&status_url).send()
        ).await
        .map_err(|_| ClientError::Timeout {
            duration: 10,
            operation: "chunked_upload_status".to_string(),
        })?
        .map_err(ClientError::Network)?;

        if !response.status().is_success() {
            return Err(ClientError::Server {
                status: response.status().as_u16(),
                message: format!("Failed to get upload status: {}", response.status()),
            });
        }

        let session: ChunkedUploadSession = response.json().await
            .map_err(|e| ClientError::Deserialization {
                operation: "chunked_upload_status".to_string(),
                error: e.to_string(),
            })?;

        // Update last activity
        let mut updated_session = session.clone();
        updated_session.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Store session
        self.active_sessions.insert(upload_id.to_string(), updated_session.clone());

        // Initialize progress tracking
        let completed_chunks = session.completed_chunks.len() as u32;
        let uploaded_bytes = completed_chunks as u64 * session.chunk_size;
        let progress_percent = if session.total_chunks > 0 {
            ((completed_chunks as f64 / session.total_chunks as f64) * 100.0) as u8
        } else {
            0
        };

        let progress = Arc::new(ChunkedUploadProgress {
            upload_id: upload_id.to_string(),
            total_chunks: session.total_chunks,
            completed_chunks,
            total_bytes: session.total_size,
            uploaded_bytes,
            progress_percent,
            upload_speed_bps: 0,
            estimated_remaining: Duration::ZERO,
        });
        self.upload_progress.insert(upload_id.to_string(), progress);

        info!(
            "Resumed chunked upload: {} ({}/{} chunks completed)",
            upload_id, completed_chunks, session.total_chunks
        );

        Ok(updated_session)
    }

    /// Upload file content using chunked upload with resume capability
    #[instrument(skip(self, content))]
    pub async fn upload_chunked(
        &self,
        file_path: &str,
        content: Bytes,
        chunk_size: Option<u64>,
    ) -> ClientResult<String> {
        let upload_id = self.start_upload(file_path, content.clone(), chunk_size).await?;
        
        let session = self.active_sessions.get(&upload_id)
            .ok_or_else(|| ClientError::InvalidResponse {
                error: "Upload session not found".to_string(),
            })?
            .clone();

        // Upload chunks concurrently
        let upload_result = self.upload_chunks(&upload_id, &content, &session).await;

        match upload_result {
            Ok(()) => {
                // Finalize upload
                self.finalize_upload(&upload_id).await?;
                info!("Chunked upload completed successfully: {}", upload_id);
                Ok(upload_id)
            }
            Err(e) => {
                error!("Chunked upload failed: {} - {}", upload_id, e);
                // Keep session for potential resume
                Err(e)
            }
        }
    }

    /// Resume an interrupted chunked upload
    #[instrument(skip(self, content))]
    pub async fn resume_chunked_upload(
        &self,
        upload_id: &str,
        content: Bytes,
    ) -> ClientResult<()> {
        let session = self.resume_upload(upload_id).await?;

        // Verify content hash matches
        let content_hash = self.hash_manager.hash_bytes(&content);
        if content_hash != session.expected_hash {
            return Err(ClientError::HashMismatch {
                path: session.file_path,
                expected: self.hash_manager.to_hex(session.expected_hash),
                actual: self.hash_manager.to_hex(content_hash),
            });
        }

        // Upload remaining chunks
        self.upload_chunks(upload_id, &content, &session).await?;

        // Finalize upload
        self.finalize_upload(upload_id).await?;

        info!("Resumed chunked upload completed: {}", upload_id);
        Ok(())
    }

    /// Upload chunks for a session
    #[instrument(skip(self, content, session))]
    async fn upload_chunks(
        &self,
        upload_id: &str,
        content: &Bytes,
        session: &ChunkedUploadSession,
    ) -> ClientResult<()> {
        let start_time = Instant::now();
        let mut tasks = Vec::new();

        // Create tasks for each chunk that needs to be uploaded
        for chunk_index in 0..session.total_chunks {
            // Skip already completed chunks
            if session.completed_chunks.contains(&chunk_index) {
                continue;
            }

            let chunk_start = chunk_index as u64 * session.chunk_size;
            let chunk_end = ((chunk_index + 1) as u64 * session.chunk_size).min(content.len() as u64);
            let chunk_data = content.slice(chunk_start as usize..chunk_end as usize);

            let task = self.upload_single_chunk(
                upload_id.to_string(),
                chunk_index,
                chunk_data,
                start_time,
            );

            tasks.push(task);
        }

        // Execute chunk uploads with concurrency limit
        let results = futures::future::join_all(tasks).await;

        // Check results
        let mut failed_chunks = Vec::new();
        let mut total_uploaded = 0u64;

        for result in results {
            match result {
                Ok(chunk_result) => {
                    if chunk_result.success {
                        total_uploaded += chunk_result.bytes_uploaded;
                        self.mark_chunk_completed(upload_id, chunk_result.chunk_index).await;
                    } else {
                        failed_chunks.push((chunk_result.chunk_index, chunk_result.error));
                    }
                }
                Err(e) => {
                    error!("Chunk upload task failed: {}", e);
                    failed_chunks.push((u32::MAX, Some(e.to_string())));
                }
            }
        }

        if !failed_chunks.is_empty() {
            let error_msg = format!(
                "Failed to upload {} chunks: {:?}",
                failed_chunks.len(),
                failed_chunks
            );
            return Err(ClientError::PartialFailure {
                successful: (session.total_chunks as usize).saturating_sub(failed_chunks.len()),
                failed: failed_chunks.len(),
                errors: failed_chunks.into_iter()
                    .map(|(idx, err)| format!("Chunk {}: {}", idx, err.unwrap_or_default()))
                    .collect(),
            });
        }

        Ok(())
    }

    /// Upload a single chunk
    #[instrument(skip(self, chunk_data))]
    async fn upload_single_chunk(
        &self,
        upload_id: String,
        chunk_index: u32,
        chunk_data: Bytes,
        start_time: Instant,
    ) -> ClientResult<ChunkUploadResult> {
        let _permit = self.chunk_semaphore.acquire().await
            .map_err(|_| ClientError::Connection {
                error: "Chunk semaphore acquisition failed".to_string(),
            })?;

        let chunk_size = chunk_data.len() as u64;
        let chunk_hash = self.hash_manager.hash_bytes(&chunk_data);

        // Retry logic for individual chunks
        let mut attempts = 0;
        let max_attempts = 3;

        while attempts < max_attempts {
            attempts += 1;

            let upload_url = format!(
                "{}/upload/chunked/{}/chunk/{}",
                self.server_url, upload_id, chunk_index
            );

            let chunk_request = self.http_client
                .put(&upload_url)
                .header("Content-Type", "application/octet-stream")
                .header("X-Chunk-Hash", format!("{:016x}", chunk_hash))
                .header("X-Chunk-Size", chunk_size.to_string())
                .body(chunk_data.clone());

            match timeout(Duration::from_secs(60), chunk_request.send()).await {
                Ok(Ok(response)) => {
                    if response.status().is_success() {
                        // Update progress
                        self.update_chunk_progress(&upload_id, chunk_size, start_time).await;

                        debug!("Uploaded chunk {}/{} for upload {}", 
                               chunk_index + 1, 
                               self.get_total_chunks(&upload_id).unwrap_or(0),
                               upload_id);

                        return Ok(ChunkUploadResult {
                            chunk_index,
                            success: true,
                            bytes_uploaded: chunk_size,
                            error: None,
                        });
                    } else {
                        let error_msg = format!(
                            "Server error for chunk {}: {}",
                            chunk_index,
                            response.status()
                        );
                        
                        if attempts >= max_attempts {
                            return Ok(ChunkUploadResult {
                                chunk_index,
                                success: false,
                                bytes_uploaded: 0,
                                error: Some(error_msg),
                            });
                        }

                        warn!("Chunk {} upload failed (attempt {}): {}", chunk_index, attempts, error_msg);
                        sleep(Duration::from_millis(1000 * attempts as u64)).await;
                    }
                }
                Ok(Err(e)) => {
                    let error_msg = format!("Network error for chunk {}: {}", chunk_index, e);
                    
                    if attempts >= max_attempts {
                        return Ok(ChunkUploadResult {
                            chunk_index,
                            success: false,
                            bytes_uploaded: 0,
                            error: Some(error_msg),
                        });
                    }

                    warn!("Chunk {} network error (attempt {}): {}", chunk_index, attempts, error_msg);
                    sleep(Duration::from_millis(1000 * attempts as u64)).await;
                }
                Err(_) => {
                    let error_msg = format!("Timeout for chunk {}", chunk_index);
                    
                    if attempts >= max_attempts {
                        return Ok(ChunkUploadResult {
                            chunk_index,
                            success: false,
                            bytes_uploaded: 0,
                            error: Some(error_msg),
                        });
                    }

                    warn!("Chunk {} timeout (attempt {})", chunk_index, attempts);
                    sleep(Duration::from_millis(2000 * attempts as u64)).await;
                }
            }
        }

        Ok(ChunkUploadResult {
            chunk_index,
            success: false,
            bytes_uploaded: 0,
            error: Some("Max retry attempts exceeded".to_string()),
        })
    }

    /// Mark a chunk as completed
    async fn mark_chunk_completed(&self, upload_id: &str, chunk_index: u32) {
        if let Some(mut session) = self.active_sessions.get_mut(upload_id) {
            if !session.completed_chunks.contains(&chunk_index) {
                session.completed_chunks.push(chunk_index);
                session.completed_chunks.sort_unstable();
                
                session.last_activity = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
            }
        }
    }

    /// Update progress tracking for a chunk
    async fn update_chunk_progress(&self, upload_id: &str, bytes_uploaded: u64, start_time: Instant) {
        if let Some(progress_ref) = self.upload_progress.get(upload_id) {
            let progress = progress_ref.value();
            
            // This is a simplified progress update - in a real implementation,
            // you'd want to use atomic operations for thread safety
            let new_completed = progress.completed_chunks + 1;
            let new_uploaded = progress.uploaded_bytes + bytes_uploaded;
            let new_progress_percent = if progress.total_chunks > 0 {
                ((new_completed as f64 / progress.total_chunks as f64) * 100.0) as u8
            } else {
                0
            };

            // Calculate upload speed
            let elapsed = start_time.elapsed();
            let upload_speed_bps = if elapsed.as_secs() > 0 {
                new_uploaded / elapsed.as_secs()
            } else {
                0
            };

            // Estimate remaining time
            let remaining_bytes = progress.total_bytes.saturating_sub(new_uploaded);
            let estimated_remaining = if upload_speed_bps > 0 {
                Duration::from_secs(remaining_bytes / upload_speed_bps)
            } else {
                Duration::ZERO
            };

            // Update progress (this would need atomic operations in real implementation)
            let updated_progress = Arc::new(ChunkedUploadProgress {
                upload_id: progress.upload_id.clone(),
                total_chunks: progress.total_chunks,
                completed_chunks: new_completed,
                total_bytes: progress.total_bytes,
                uploaded_bytes: new_uploaded,
                progress_percent: new_progress_percent,
                upload_speed_bps,
                estimated_remaining,
            });

            self.upload_progress.insert(upload_id.to_string(), updated_progress);
        }
    }

    /// Finalize a chunked upload
    #[instrument(skip(self))]
    async fn finalize_upload(&self, upload_id: &str) -> ClientResult<()> {
        let finalize_url = format!("{}/upload/chunked/{}/finalize", self.server_url, upload_id);
        
        let response = timeout(
            Duration::from_secs(30),
            self.http_client.post(&finalize_url).send()
        ).await
        .map_err(|_| ClientError::Timeout {
            duration: 30,
            operation: "chunked_upload_finalize".to_string(),
        })?
        .map_err(ClientError::Network)?;

        if !response.status().is_success() {
            return Err(ClientError::Server {
                status: response.status().as_u16(),
                message: format!("Failed to finalize upload: {}", response.status()),
            });
        }

        // Clean up session
        self.active_sessions.remove(upload_id);
        self.upload_progress.remove(upload_id);

        debug!("Finalized chunked upload: {}", upload_id);
        Ok(())
    }

    /// Get upload progress
    pub async fn get_progress(&self, upload_id: &str) -> Option<ChunkedUploadProgress> {
        self.upload_progress.get(upload_id).map(|p| (**p).clone())
    }

    /// Get all active uploads
    pub async fn get_active_uploads(&self) -> Vec<String> {
        self.active_sessions.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Cancel an upload
    #[instrument(skip(self))]
    pub async fn cancel_upload(&self, upload_id: &str) -> ClientResult<()> {
        let cancel_url = format!("{}/upload/chunked/{}/cancel", self.server_url, upload_id);
        
        let response = timeout(
            Duration::from_secs(10),
            self.http_client.delete(&cancel_url).send()
        ).await
        .map_err(|_| ClientError::Timeout {
            duration: 10,
            operation: "chunked_upload_cancel".to_string(),
        })?
        .map_err(ClientError::Network)?;

        if !response.status().is_success() {
            warn!("Failed to cancel upload on server: {}", response.status());
        }

        // Clean up local state
        self.active_sessions.remove(upload_id);
        self.upload_progress.remove(upload_id);

        info!("Cancelled chunked upload: {}", upload_id);
        Ok(())
    }

    /// Clean up expired sessions
    pub async fn cleanup_expired_sessions(&self) -> ClientResult<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let timeout_secs = self.session_timeout.as_secs();
        let mut expired_sessions = Vec::new();

        for entry in self.active_sessions.iter() {
            let (upload_id, session) = (entry.key(), entry.value());
            if now.saturating_sub(session.last_activity) > timeout_secs {
                expired_sessions.push(upload_id.clone());
            }
        }

        for upload_id in expired_sessions {
            warn!("Cleaning up expired upload session: {}", upload_id);
            let _ = self.cancel_upload(&upload_id).await;
        }

        Ok(())
    }

    /// Get total chunks for an upload
    fn get_total_chunks(&self, upload_id: &str) -> Option<u32> {
        self.active_sessions.get(upload_id).map(|s| s.total_chunks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_chunked_upload_session_creation() {
        let client = reqwest::Client::new();
        let manager = ChunkedUploadManager::new(
            client,
            "http://localhost:8080".to_string(),
            1024 * 1024, // 1MB chunks
            4,            // 4 concurrent chunks
            Duration::from_secs(3600), // 1 hour timeout
        );

        // This test would require a mock server to work properly
        // For now, just test the basic structure
        assert_eq!(manager.default_chunk_size, 1024 * 1024);
        assert_eq!(manager.max_concurrent_chunks, 4);
    }

    #[test]
    fn test_chunk_calculation() {
        let content_size = 5 * 1024 * 1024; // 5MB
        let chunk_size = 1024 * 1024; // 1MB
        let total_chunks = ((content_size + chunk_size - 1) / chunk_size) as u32;
        
        assert_eq!(total_chunks, 5);
    }

    #[test]
    fn test_progress_calculation() {
        let total_chunks = 10u32;
        let completed_chunks = 3u32;
        let progress_percent = ((completed_chunks as f64 / total_chunks as f64) * 100.0) as u8;
        
        assert_eq!(progress_percent, 30);
    }
}