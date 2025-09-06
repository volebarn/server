//! Comprehensive monitoring and metrics collection
//! 
//! This module provides detailed metrics collection and monitoring capabilities
//! for the Volebarn client with structured logging and performance tracking.

use crate::error::ClientError;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::interval;
use tracing::{debug, info, warn, error, instrument, Span};

/// Metrics for different operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetrics {
    pub operation_type: String,
    pub total_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub total_duration_ms: u64,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    pub avg_duration_ms: u64,
    pub p95_duration_ms: u64,
    pub p99_duration_ms: u64,
    pub bytes_transferred: u64,
    pub last_success: Option<u64>, // Timestamp
    pub last_failure: Option<u64>, // Timestamp
}

/// Network performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    pub requests_sent: u64,
    pub requests_successful: u64,
    pub requests_failed: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub connection_errors: u64,
    pub timeout_errors: u64,
    pub server_errors: u64,
    pub avg_response_time_ms: u64,
    pub connection_pool_active: u32,
    pub connection_pool_idle: u32,
}

/// Circuit breaker metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerMetrics {
    pub state: String, // "closed", "open", "half_open"
    pub failure_count: u32,
    pub success_count: u32,
    pub trip_count: u32,
    pub last_trip_time: Option<u64>,
    pub recovery_attempts: u32,
    pub time_in_open_state_ms: u64,
}

/// Cache performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetrics {
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_rate: f64,
    pub eviction_count: u64,
    pub cache_size_bytes: u64,
    pub cache_entries: u32,
    pub avg_access_time_ms: u64,
}

/// Sync operation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMetrics {
    pub sync_operations: u64,
    pub files_uploaded: u64,
    pub files_downloaded: u64,
    pub files_deleted: u64,
    pub conflicts_resolved: u64,
    pub sync_errors: u64,
    pub avg_sync_duration_ms: u64,
    pub last_sync_time: Option<u64>,
}

/// Offline queue metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineQueueMetrics {
    pub queued_operations: u32,
    pub processed_operations: u64,
    pub failed_operations: u64,
    pub queue_size_bytes: u64,
    pub avg_processing_time_ms: u64,
    pub oldest_operation_age_ms: u64,
}

/// Comprehensive client metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMetrics {
    pub timestamp: u64,
    pub uptime_ms: u64,
    pub operations: Vec<OperationMetrics>,
    pub network: NetworkMetrics,
    pub circuit_breaker: CircuitBreakerMetrics,
    pub cache: CacheMetrics,
    pub sync: SyncMetrics,
    pub offline_queue: OfflineQueueMetrics,
    pub degradation_mode: String,
    pub health_status: String,
}

/// Performance measurement for individual operations
#[derive(Debug)]
pub struct PerformanceMeasurement {
    pub operation: String,
    pub start_time: Instant,
    pub bytes_transferred: u64,
    pub success: bool,
    pub error_type: Option<String>,
    pub span: Span,
}

impl PerformanceMeasurement {
    /// Start measuring an operation
    pub fn start(operation: &str) -> Self {
        let span = tracing::info_span!("operation", op = operation);
        
        Self {
            operation: operation.to_string(),
            start_time: Instant::now(),
            bytes_transferred: 0,
            success: false,
            error_type: None,
            span,
        }
    }

    /// Record bytes transferred
    pub fn add_bytes(&mut self, bytes: u64) {
        self.bytes_transferred += bytes;
    }

    /// Mark operation as successful
    pub fn success(mut self) -> CompletedMeasurement {
        self.success = true;
        CompletedMeasurement::from(self)
    }

    /// Mark operation as failed
    pub fn failure(mut self, error: &ClientError) -> CompletedMeasurement {
        self.success = false;
        self.error_type = Some(format!("{:?}", error.error_code()));
        CompletedMeasurement::from(self)
    }
}

/// Completed performance measurement
#[derive(Debug)]
pub struct CompletedMeasurement {
    pub operation: String,
    pub duration: Duration,
    pub bytes_transferred: u64,
    pub success: bool,
    pub error_type: Option<String>,
}

impl From<PerformanceMeasurement> for CompletedMeasurement {
    fn from(measurement: PerformanceMeasurement) -> Self {
        let duration = measurement.start_time.elapsed();
        
        // Log the completed operation
        let _enter = measurement.span.enter();
        if measurement.success {
            info!(
                duration_ms = duration.as_millis(),
                bytes = measurement.bytes_transferred,
                "Operation completed successfully"
            );
        } else {
            warn!(
                duration_ms = duration.as_millis(),
                bytes = measurement.bytes_transferred,
                error = measurement.error_type.as_deref().unwrap_or("unknown"),
                "Operation failed"
            );
        }

        Self {
            operation: measurement.operation,
            duration,
            bytes_transferred: measurement.bytes_transferred,
            success: measurement.success,
            error_type: measurement.error_type,
        }
    }
}

/// Metrics collector with atomic counters and lock-free operations
#[derive(Debug)]
pub struct MetricsCollector {
    /// Start time for uptime calculation
    start_time: Instant,
    /// Operation metrics by type
    operation_metrics: DashMap<String, Arc<OperationMetricsInternal>>,
    /// Network metrics
    network_requests_sent: Arc<AtomicU64>,
    network_requests_successful: Arc<AtomicU64>,
    network_requests_failed: Arc<AtomicU64>,
    network_bytes_uploaded: Arc<AtomicU64>,
    network_bytes_downloaded: Arc<AtomicU64>,
    network_connection_errors: Arc<AtomicU64>,
    network_timeout_errors: Arc<AtomicU64>,
    network_server_errors: Arc<AtomicU64>,
    network_total_response_time_ms: Arc<AtomicU64>,
    /// Circuit breaker metrics
    cb_failure_count: Arc<AtomicU32>,
    cb_success_count: Arc<AtomicU32>,
    cb_trip_count: Arc<AtomicU32>,
    cb_recovery_attempts: Arc<AtomicU32>,
    cb_state: Arc<AtomicU32>, // 0=closed, 1=open, 2=half_open
    cb_last_trip_time: Arc<AtomicU64>,
    cb_open_state_start: Arc<AtomicU64>,
    /// Cache metrics
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
    cache_evictions: Arc<AtomicU64>,
    cache_size_bytes: Arc<AtomicU64>,
    cache_entries: Arc<AtomicU32>,
    cache_total_access_time_ms: Arc<AtomicU64>,
    cache_access_count: Arc<AtomicU64>,
    /// Sync metrics
    sync_operations: Arc<AtomicU64>,
    sync_files_uploaded: Arc<AtomicU64>,
    sync_files_downloaded: Arc<AtomicU64>,
    sync_files_deleted: Arc<AtomicU64>,
    sync_conflicts_resolved: Arc<AtomicU64>,
    sync_errors: Arc<AtomicU64>,
    sync_total_duration_ms: Arc<AtomicU64>,
    sync_last_time: Arc<AtomicU64>,
    /// Offline queue metrics
    offline_queued_operations: Arc<AtomicU32>,
    offline_processed_operations: Arc<AtomicU64>,
    offline_failed_operations: Arc<AtomicU64>,
    offline_queue_size_bytes: Arc<AtomicU64>,
    offline_total_processing_time_ms: Arc<AtomicU64>,
    offline_oldest_operation_time: Arc<AtomicU64>,
    /// Current state
    degradation_mode: Arc<AtomicU32>,
    health_status: Arc<AtomicU32>, // 0=unknown, 1=healthy, 2=unhealthy
    /// Shutdown signal
    shutdown_tx: Arc<broadcast::Sender<()>>,
}

/// Internal operation metrics with atomic counters
#[derive(Debug)]
struct OperationMetricsInternal {
    total_count: AtomicU64,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    total_duration_ms: AtomicU64,
    min_duration_ms: AtomicU64,
    max_duration_ms: AtomicU64,
    bytes_transferred: AtomicU64,
    last_success: AtomicU64,
    last_failure: AtomicU64,
    /// Duration samples for percentile calculation (simplified)
    duration_samples: DashMap<u64, u32>, // duration_ms -> count
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);

        Self {
            start_time: Instant::now(),
            operation_metrics: DashMap::new(),
            network_requests_sent: Arc::new(AtomicU64::new(0)),
            network_requests_successful: Arc::new(AtomicU64::new(0)),
            network_requests_failed: Arc::new(AtomicU64::new(0)),
            network_bytes_uploaded: Arc::new(AtomicU64::new(0)),
            network_bytes_downloaded: Arc::new(AtomicU64::new(0)),
            network_connection_errors: Arc::new(AtomicU64::new(0)),
            network_timeout_errors: Arc::new(AtomicU64::new(0)),
            network_server_errors: Arc::new(AtomicU64::new(0)),
            network_total_response_time_ms: Arc::new(AtomicU64::new(0)),
            cb_failure_count: Arc::new(AtomicU32::new(0)),
            cb_success_count: Arc::new(AtomicU32::new(0)),
            cb_trip_count: Arc::new(AtomicU32::new(0)),
            cb_recovery_attempts: Arc::new(AtomicU32::new(0)),
            cb_state: Arc::new(AtomicU32::new(0)),
            cb_last_trip_time: Arc::new(AtomicU64::new(0)),
            cb_open_state_start: Arc::new(AtomicU64::new(0)),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            cache_evictions: Arc::new(AtomicU64::new(0)),
            cache_size_bytes: Arc::new(AtomicU64::new(0)),
            cache_entries: Arc::new(AtomicU32::new(0)),
            cache_total_access_time_ms: Arc::new(AtomicU64::new(0)),
            cache_access_count: Arc::new(AtomicU64::new(0)),
            sync_operations: Arc::new(AtomicU64::new(0)),
            sync_files_uploaded: Arc::new(AtomicU64::new(0)),
            sync_files_downloaded: Arc::new(AtomicU64::new(0)),
            sync_files_deleted: Arc::new(AtomicU64::new(0)),
            sync_conflicts_resolved: Arc::new(AtomicU64::new(0)),
            sync_errors: Arc::new(AtomicU64::new(0)),
            sync_total_duration_ms: Arc::new(AtomicU64::new(0)),
            sync_last_time: Arc::new(AtomicU64::new(0)),
            offline_queued_operations: Arc::new(AtomicU32::new(0)),
            offline_processed_operations: Arc::new(AtomicU64::new(0)),
            offline_failed_operations: Arc::new(AtomicU64::new(0)),
            offline_queue_size_bytes: Arc::new(AtomicU64::new(0)),
            offline_total_processing_time_ms: Arc::new(AtomicU64::new(0)),
            offline_oldest_operation_time: Arc::new(AtomicU64::new(0)),
            degradation_mode: Arc::new(AtomicU32::new(0)),
            health_status: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(shutdown_tx),
        }
    }

    /// Record a completed operation
    #[instrument(skip(self))]
    pub fn record_operation(&self, measurement: CompletedMeasurement) {
        let operation_type = measurement.operation.clone();
        
        // Get or create operation metrics
        let metrics = self.operation_metrics
            .entry(operation_type)
            .or_insert_with(|| Arc::new(OperationMetricsInternal {
                total_count: AtomicU64::new(0),
                success_count: AtomicU64::new(0),
                failure_count: AtomicU64::new(0),
                total_duration_ms: AtomicU64::new(0),
                min_duration_ms: AtomicU64::new(u64::MAX),
                max_duration_ms: AtomicU64::new(0),
                bytes_transferred: AtomicU64::new(0),
                last_success: AtomicU64::new(0),
                last_failure: AtomicU64::new(0),
                duration_samples: DashMap::new(),
            }))
            .clone();

        let duration_ms = measurement.duration.as_millis() as u64;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update counters
        metrics.total_count.fetch_add(1, Ordering::Relaxed);
        metrics.total_duration_ms.fetch_add(duration_ms, Ordering::Relaxed);
        metrics.bytes_transferred.fetch_add(measurement.bytes_transferred, Ordering::Relaxed);

        if measurement.success {
            metrics.success_count.fetch_add(1, Ordering::Relaxed);
            metrics.last_success.store(now, Ordering::Relaxed);
        } else {
            metrics.failure_count.fetch_add(1, Ordering::Relaxed);
            metrics.last_failure.store(now, Ordering::Relaxed);
        }

        // Update min/max duration
        let current_min = metrics.min_duration_ms.load(Ordering::Relaxed);
        if duration_ms < current_min {
            metrics.min_duration_ms.store(duration_ms, Ordering::Relaxed);
        }

        let current_max = metrics.max_duration_ms.load(Ordering::Relaxed);
        if duration_ms > current_max {
            metrics.max_duration_ms.store(duration_ms, Ordering::Relaxed);
        }

        // Add duration sample for percentile calculation
        let bucket = (duration_ms / 10) * 10; // Round to nearest 10ms
        metrics.duration_samples
            .entry(bucket)
            .and_modify(|count| *count += 1)
            .or_insert(1);

        debug!(
            operation = measurement.operation,
            duration_ms = duration_ms,
            success = measurement.success,
            bytes = measurement.bytes_transferred,
            "Recorded operation metrics"
        );
    }

    /// Record network metrics
    pub fn record_network_request(&self, success: bool, duration: Duration, bytes_uploaded: u64, bytes_downloaded: u64, error: Option<&ClientError>) {
        self.network_requests_sent.fetch_add(1, Ordering::Relaxed);
        self.network_total_response_time_ms.fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
        self.network_bytes_uploaded.fetch_add(bytes_uploaded, Ordering::Relaxed);
        self.network_bytes_downloaded.fetch_add(bytes_downloaded, Ordering::Relaxed);

        if success {
            self.network_requests_successful.fetch_add(1, Ordering::Relaxed);
        } else {
            self.network_requests_failed.fetch_add(1, Ordering::Relaxed);
            
            if let Some(error) = error {
                match error {
                    ClientError::Network(_) | ClientError::Connection { .. } => {
                        self.network_connection_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    ClientError::Timeout { .. } => {
                        self.network_timeout_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    ClientError::Server { .. } => {
                        self.network_server_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
        }
    }

    /// Record circuit breaker state change
    pub fn record_circuit_breaker_state(&self, state: &str, failure_count: u32, success_count: u32) {
        let state_value = match state {
            "closed" => 0,
            "open" => 1,
            "half_open" => 2,
            _ => 0,
        };

        let old_state = self.cb_state.swap(state_value, Ordering::Relaxed);
        self.cb_failure_count.store(failure_count, Ordering::Relaxed);
        self.cb_success_count.store(success_count, Ordering::Relaxed);

        // Track state transitions
        if old_state == 0 && state_value == 1 {
            // Closed -> Open
            self.cb_trip_count.fetch_add(1, Ordering::Relaxed);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.cb_last_trip_time.store(now, Ordering::Relaxed);
            self.cb_open_state_start.store(now, Ordering::Relaxed);
        } else if old_state == 1 && state_value == 2 {
            // Open -> Half-Open
            self.cb_recovery_attempts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record cache operation
    pub fn record_cache_operation(&self, hit: bool, access_time: Duration, cache_size_bytes: u64, cache_entries: u32) {
        if hit {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        self.cache_total_access_time_ms.fetch_add(access_time.as_millis() as u64, Ordering::Relaxed);
        self.cache_access_count.fetch_add(1, Ordering::Relaxed);
        self.cache_size_bytes.store(cache_size_bytes, Ordering::Relaxed);
        self.cache_entries.store(cache_entries, Ordering::Relaxed);
    }

    /// Record cache eviction
    pub fn record_cache_eviction(&self) {
        self.cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record sync operation
    pub fn record_sync_operation(&self, duration: Duration, files_uploaded: u64, files_downloaded: u64, files_deleted: u64, conflicts_resolved: u64, errors: u64) {
        self.sync_operations.fetch_add(1, Ordering::Relaxed);
        self.sync_files_uploaded.fetch_add(files_uploaded, Ordering::Relaxed);
        self.sync_files_downloaded.fetch_add(files_downloaded, Ordering::Relaxed);
        self.sync_files_deleted.fetch_add(files_deleted, Ordering::Relaxed);
        self.sync_conflicts_resolved.fetch_add(conflicts_resolved, Ordering::Relaxed);
        self.sync_errors.fetch_add(errors, Ordering::Relaxed);
        self.sync_total_duration_ms.fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.sync_last_time.store(now, Ordering::Relaxed);
    }

    /// Record offline queue metrics
    pub fn record_offline_queue(&self, queued: u32, processed: u64, failed: u64, size_bytes: u64, processing_time: Duration, oldest_age: Duration) {
        self.offline_queued_operations.store(queued, Ordering::Relaxed);
        self.offline_processed_operations.store(processed, Ordering::Relaxed);
        self.offline_failed_operations.store(failed, Ordering::Relaxed);
        self.offline_queue_size_bytes.store(size_bytes, Ordering::Relaxed);
        self.offline_total_processing_time_ms.fetch_add(processing_time.as_millis() as u64, Ordering::Relaxed);
        self.offline_oldest_operation_time.store(oldest_age.as_millis() as u64, Ordering::Relaxed);
    }

    /// Set degradation mode
    pub fn set_degradation_mode(&self, mode: &str) {
        let mode_value = match mode {
            "normal" => 0,
            "read_only" => 1,
            "offline" => 2,
            "emergency" => 3,
            _ => 0,
        };
        self.degradation_mode.store(mode_value, Ordering::Relaxed);
    }

    /// Set health status
    pub fn set_health_status(&self, healthy: bool) {
        let status_value = if healthy { 1 } else { 2 };
        self.health_status.store(status_value, Ordering::Relaxed);
    }

    /// Get comprehensive metrics snapshot
    pub async fn get_metrics(&self) -> ClientMetrics {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let uptime_ms = self.start_time.elapsed().as_millis() as u64;

        // Collect operation metrics
        let mut operations = Vec::new();
        for entry in self.operation_metrics.iter() {
            let (op_type, metrics) = (entry.key(), entry.value());
            let total_count = metrics.total_count.load(Ordering::Relaxed);
            let success_count = metrics.success_count.load(Ordering::Relaxed);
            let failure_count = metrics.failure_count.load(Ordering::Relaxed);
            let total_duration_ms = metrics.total_duration_ms.load(Ordering::Relaxed);
            let min_duration_ms = metrics.min_duration_ms.load(Ordering::Relaxed);
            let max_duration_ms = metrics.max_duration_ms.load(Ordering::Relaxed);
            let bytes_transferred = metrics.bytes_transferred.load(Ordering::Relaxed);
            let last_success = metrics.last_success.load(Ordering::Relaxed);
            let last_failure = metrics.last_failure.load(Ordering::Relaxed);

            let avg_duration_ms = if total_count > 0 {
                total_duration_ms / total_count
            } else {
                0
            };

            // Calculate percentiles (simplified)
            let (p95_duration_ms, p99_duration_ms) = self.calculate_percentiles(&metrics.duration_samples);

            operations.push(OperationMetrics {
                operation_type: op_type.clone(),
                total_count,
                success_count,
                failure_count,
                total_duration_ms,
                min_duration_ms: if min_duration_ms == u64::MAX { 0 } else { min_duration_ms },
                max_duration_ms,
                avg_duration_ms,
                p95_duration_ms,
                p99_duration_ms,
                bytes_transferred,
                last_success: if last_success > 0 { Some(last_success) } else { None },
                last_failure: if last_failure > 0 { Some(last_failure) } else { None },
            });
        }

        // Network metrics
        let requests_sent = self.network_requests_sent.load(Ordering::Relaxed);
        let total_response_time = self.network_total_response_time_ms.load(Ordering::Relaxed);
        let avg_response_time_ms = if requests_sent > 0 {
            total_response_time / requests_sent
        } else {
            0
        };

        let network = NetworkMetrics {
            requests_sent,
            requests_successful: self.network_requests_successful.load(Ordering::Relaxed),
            requests_failed: self.network_requests_failed.load(Ordering::Relaxed),
            bytes_uploaded: self.network_bytes_uploaded.load(Ordering::Relaxed),
            bytes_downloaded: self.network_bytes_downloaded.load(Ordering::Relaxed),
            connection_errors: self.network_connection_errors.load(Ordering::Relaxed),
            timeout_errors: self.network_timeout_errors.load(Ordering::Relaxed),
            server_errors: self.network_server_errors.load(Ordering::Relaxed),
            avg_response_time_ms,
            connection_pool_active: 0, // Would be provided by HTTP client
            connection_pool_idle: 0,   // Would be provided by HTTP client
        };

        // Circuit breaker metrics
        let cb_state_value = self.cb_state.load(Ordering::Relaxed);
        let cb_state = match cb_state_value {
            0 => "closed",
            1 => "open",
            2 => "half_open",
            _ => "unknown",
        }.to_string();

        let cb_open_start = self.cb_open_state_start.load(Ordering::Relaxed);
        let time_in_open_state_ms = if cb_state_value == 1 && cb_open_start > 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            (now.saturating_sub(cb_open_start)) * 1000
        } else {
            0
        };

        let circuit_breaker = CircuitBreakerMetrics {
            state: cb_state,
            failure_count: self.cb_failure_count.load(Ordering::Relaxed),
            success_count: self.cb_success_count.load(Ordering::Relaxed),
            trip_count: self.cb_trip_count.load(Ordering::Relaxed),
            last_trip_time: {
                let time = self.cb_last_trip_time.load(Ordering::Relaxed);
                if time > 0 { Some(time) } else { None }
            },
            recovery_attempts: self.cb_recovery_attempts.load(Ordering::Relaxed),
            time_in_open_state_ms,
        };

        // Cache metrics
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_cache_requests = cache_hits + cache_misses;
        let hit_rate = if total_cache_requests > 0 {
            cache_hits as f64 / total_cache_requests as f64
        } else {
            0.0
        };

        let cache_access_count = self.cache_access_count.load(Ordering::Relaxed);
        let total_access_time = self.cache_total_access_time_ms.load(Ordering::Relaxed);
        let avg_access_time_ms = if cache_access_count > 0 {
            total_access_time / cache_access_count
        } else {
            0
        };

        let cache = CacheMetrics {
            hit_count: cache_hits,
            miss_count: cache_misses,
            hit_rate,
            eviction_count: self.cache_evictions.load(Ordering::Relaxed),
            cache_size_bytes: self.cache_size_bytes.load(Ordering::Relaxed),
            cache_entries: self.cache_entries.load(Ordering::Relaxed),
            avg_access_time_ms,
        };

        // Sync metrics
        let sync_ops = self.sync_operations.load(Ordering::Relaxed);
        let sync_total_duration = self.sync_total_duration_ms.load(Ordering::Relaxed);
        let avg_sync_duration_ms = if sync_ops > 0 {
            sync_total_duration / sync_ops
        } else {
            0
        };

        let sync = SyncMetrics {
            sync_operations: sync_ops,
            files_uploaded: self.sync_files_uploaded.load(Ordering::Relaxed),
            files_downloaded: self.sync_files_downloaded.load(Ordering::Relaxed),
            files_deleted: self.sync_files_deleted.load(Ordering::Relaxed),
            conflicts_resolved: self.sync_conflicts_resolved.load(Ordering::Relaxed),
            sync_errors: self.sync_errors.load(Ordering::Relaxed),
            avg_sync_duration_ms,
            last_sync_time: {
                let time = self.sync_last_time.load(Ordering::Relaxed);
                if time > 0 { Some(time) } else { None }
            },
        };

        // Offline queue metrics
        let offline_processed = self.offline_processed_operations.load(Ordering::Relaxed);
        let offline_total_time = self.offline_total_processing_time_ms.load(Ordering::Relaxed);
        let avg_processing_time_ms = if offline_processed > 0 {
            offline_total_time / offline_processed
        } else {
            0
        };

        let offline_queue = OfflineQueueMetrics {
            queued_operations: self.offline_queued_operations.load(Ordering::Relaxed),
            processed_operations: offline_processed,
            failed_operations: self.offline_failed_operations.load(Ordering::Relaxed),
            queue_size_bytes: self.offline_queue_size_bytes.load(Ordering::Relaxed),
            avg_processing_time_ms,
            oldest_operation_age_ms: self.offline_oldest_operation_time.load(Ordering::Relaxed),
        };

        // Current state
        let degradation_mode = match self.degradation_mode.load(Ordering::Relaxed) {
            0 => "normal",
            1 => "read_only",
            2 => "offline",
            3 => "emergency",
            _ => "unknown",
        }.to_string();

        let health_status = match self.health_status.load(Ordering::Relaxed) {
            0 => "unknown",
            1 => "healthy",
            2 => "unhealthy",
            _ => "unknown",
        }.to_string();

        ClientMetrics {
            timestamp,
            uptime_ms,
            operations,
            network,
            circuit_breaker,
            cache,
            sync,
            offline_queue,
            degradation_mode,
            health_status,
        }
    }

    /// Calculate percentiles from duration samples (simplified implementation)
    fn calculate_percentiles(&self, samples: &DashMap<u64, u32>) -> (u64, u64) {
        if samples.is_empty() {
            return (0, 0);
        }

        let mut durations: Vec<(u64, u32)> = samples.iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        durations.sort_by_key(|(duration, _)| *duration);

        let total_samples: u32 = durations.iter().map(|(_, count)| *count).sum();
        let p95_target = (total_samples as f64 * 0.95) as u32;
        let p99_target = (total_samples as f64 * 0.99) as u32;

        let mut cumulative = 0u32;
        let mut p95_duration = 0u64;
        let mut p99_duration = 0u64;

        for (duration, count) in durations {
            cumulative += count;
            
            if p95_duration == 0 && cumulative >= p95_target {
                p95_duration = duration;
            }
            
            if p99_duration == 0 && cumulative >= p99_target {
                p99_duration = duration;
                break;
            }
        }

        (p95_duration, p99_duration)
    }

    /// Start periodic metrics logging
    pub async fn start_periodic_logging(&self, interval_duration: Duration) -> Result<(), ClientError> {
        let collector = self.clone_for_background();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = interval(interval_duration);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let metrics = collector.get_metrics().await;
                        info!(
                            uptime_ms = metrics.uptime_ms,
                            operations = metrics.operations.len(),
                            network_requests = metrics.network.requests_sent,
                            cache_hit_rate = metrics.cache.hit_rate,
                            degradation_mode = metrics.degradation_mode,
                            health_status = metrics.health_status,
                            "Periodic metrics report"
                        );
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Metrics logging task shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Export metrics in Prometheus format
    pub async fn export_prometheus(&self) -> String {
        let metrics = self.get_metrics().await;
        let mut output = String::new();

        // Add timestamp
        output.push_str(&format!("# HELP volebarn_uptime_seconds Client uptime in seconds\n"));
        output.push_str(&format!("# TYPE volebarn_uptime_seconds counter\n"));
        output.push_str(&format!("volebarn_uptime_seconds {}\n", metrics.uptime_ms / 1000));

        // Network metrics
        output.push_str(&format!("# HELP volebarn_network_requests_total Total network requests\n"));
        output.push_str(&format!("# TYPE volebarn_network_requests_total counter\n"));
        output.push_str(&format!("volebarn_network_requests_total{{status=\"success\"}} {}\n", metrics.network.requests_successful));
        output.push_str(&format!("volebarn_network_requests_total{{status=\"failure\"}} {}\n", metrics.network.requests_failed));

        // Cache metrics
        output.push_str(&format!("# HELP volebarn_cache_hit_rate Cache hit rate\n"));
        output.push_str(&format!("# TYPE volebarn_cache_hit_rate gauge\n"));
        output.push_str(&format!("volebarn_cache_hit_rate {}\n", metrics.cache.hit_rate));

        // Operation metrics
        for op in &metrics.operations {
            output.push_str(&format!("# HELP volebarn_operation_duration_ms Operation duration in milliseconds\n"));
            output.push_str(&format!("# TYPE volebarn_operation_duration_ms histogram\n"));
            output.push_str(&format!("volebarn_operation_duration_ms{{operation=\"{}\",quantile=\"0.95\"}} {}\n", op.operation_type, op.p95_duration_ms));
            output.push_str(&format!("volebarn_operation_duration_ms{{operation=\"{}\",quantile=\"0.99\"}} {}\n", op.operation_type, op.p99_duration_ms));
        }

        output
    }

    /// Stop metrics collection
    pub async fn stop(&self) -> Result<(), ClientError> {
        let _ = self.shutdown_tx.send(());
        info!("Metrics collector stopped");
        Ok(())
    }

    /// Clone for background tasks
    fn clone_for_background(&self) -> Self {
        Self {
            start_time: self.start_time,
            operation_metrics: self.operation_metrics.clone(),
            network_requests_sent: self.network_requests_sent.clone(),
            network_requests_successful: self.network_requests_successful.clone(),
            network_requests_failed: self.network_requests_failed.clone(),
            network_bytes_uploaded: self.network_bytes_uploaded.clone(),
            network_bytes_downloaded: self.network_bytes_downloaded.clone(),
            network_connection_errors: self.network_connection_errors.clone(),
            network_timeout_errors: self.network_timeout_errors.clone(),
            network_server_errors: self.network_server_errors.clone(),
            network_total_response_time_ms: self.network_total_response_time_ms.clone(),
            cb_failure_count: self.cb_failure_count.clone(),
            cb_success_count: self.cb_success_count.clone(),
            cb_trip_count: self.cb_trip_count.clone(),
            cb_recovery_attempts: self.cb_recovery_attempts.clone(),
            cb_state: self.cb_state.clone(),
            cb_last_trip_time: self.cb_last_trip_time.clone(),
            cb_open_state_start: self.cb_open_state_start.clone(),
            cache_hits: self.cache_hits.clone(),
            cache_misses: self.cache_misses.clone(),
            cache_evictions: self.cache_evictions.clone(),
            cache_size_bytes: self.cache_size_bytes.clone(),
            cache_entries: self.cache_entries.clone(),
            cache_total_access_time_ms: self.cache_total_access_time_ms.clone(),
            cache_access_count: self.cache_access_count.clone(),
            sync_operations: self.sync_operations.clone(),
            sync_files_uploaded: self.sync_files_uploaded.clone(),
            sync_files_downloaded: self.sync_files_downloaded.clone(),
            sync_files_deleted: self.sync_files_deleted.clone(),
            sync_conflicts_resolved: self.sync_conflicts_resolved.clone(),
            sync_errors: self.sync_errors.clone(),
            sync_total_duration_ms: self.sync_total_duration_ms.clone(),
            sync_last_time: self.sync_last_time.clone(),
            offline_queued_operations: self.offline_queued_operations.clone(),
            offline_processed_operations: self.offline_processed_operations.clone(),
            offline_failed_operations: self.offline_failed_operations.clone(),
            offline_queue_size_bytes: self.offline_queue_size_bytes.clone(),
            offline_total_processing_time_ms: self.offline_total_processing_time_ms.clone(),
            offline_oldest_operation_time: self.offline_oldest_operation_time.clone(),
            degradation_mode: self.degradation_mode.clone(),
            health_status: self.health_status.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }
}

impl Drop for MetricsCollector {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_operation_metrics() {
        let collector = MetricsCollector::new();
        
        // Record a successful operation
        let measurement = PerformanceMeasurement::start("upload");
        let completed = measurement.success();
        collector.record_operation(completed);

        let metrics = collector.get_metrics().await;
        assert_eq!(metrics.operations.len(), 1);
        
        let upload_metrics = &metrics.operations[0];
        assert_eq!(upload_metrics.operation_type, "upload");
        assert_eq!(upload_metrics.total_count, 1);
        assert_eq!(upload_metrics.success_count, 1);
        assert_eq!(upload_metrics.failure_count, 0);
    }

    #[tokio::test]
    async fn test_network_metrics() {
        let collector = MetricsCollector::new();
        
        collector.record_network_request(
            true,
            Duration::from_millis(100),
            1024,
            2048,
            None
        );

        let metrics = collector.get_metrics().await;
        assert_eq!(metrics.network.requests_sent, 1);
        assert_eq!(metrics.network.requests_successful, 1);
        assert_eq!(metrics.network.bytes_uploaded, 1024);
        assert_eq!(metrics.network.bytes_downloaded, 2048);
    }

    #[tokio::test]
    async fn test_cache_metrics() {
        let collector = MetricsCollector::new();
        
        collector.record_cache_operation(true, Duration::from_millis(5), 1024, 10);
        collector.record_cache_operation(false, Duration::from_millis(10), 1024, 10);

        let metrics = collector.get_metrics().await;
        assert_eq!(metrics.cache.hit_count, 1);
        assert_eq!(metrics.cache.miss_count, 1);
        assert_eq!(metrics.cache.hit_rate, 0.5);
    }

    #[test]
    fn test_prometheus_export() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let collector = MetricsCollector::new();
            let prometheus_output = collector.export_prometheus().await;
            
            assert!(prometheus_output.contains("volebarn_uptime_seconds"));
            assert!(prometheus_output.contains("volebarn_network_requests_total"));
            assert!(prometheus_output.contains("volebarn_cache_hit_rate"));
        });
    }
}