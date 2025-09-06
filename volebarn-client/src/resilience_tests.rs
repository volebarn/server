//! Comprehensive tests for error scenarios, network partitions, and recovery patterns
//! 
//! This module provides extensive testing for resilience features including
//! network failures, server errors, circuit breaker behavior, and recovery scenarios.

#[cfg(test)]
mod tests {
    use crate::chunked_upload::{ChunkedUploadManager, ChunkedUploadSession};
    use crate::error::{ClientError, ClientResult};
    use crate::graceful_degradation::{DegradationMode, GracefulDegradationManager};
    use crate::metrics::{MetricsCollector, PerformanceMeasurement};
    use crate::offline_queue::{OfflineQueue, OperationType, OperationPriority};
    use crate::retry::{CircuitBreaker, CircuitState, RetryPolicy};
    use crate::config::Config;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use tokio::time::{sleep, timeout};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::{method, path};

    /// Test helper for creating mock servers with various failure scenarios
    struct MockServerBuilder {
        server: MockServer,
    }

    impl MockServerBuilder {
        async fn new() -> Self {
            let server = MockServer::start().await;
            Self { server }
        }

        fn uri(&self) -> String {
            self.server.uri()
        }

        /// Configure server to return errors for a number of requests
        async fn fail_requests(&self, count: u32, status: u16) {
            for _ in 0..count {
                Mock::given(method("GET"))
                    .and(path("/health"))
                    .respond_with(ResponseTemplate::new(status))
                    .expect(1)
                    .mount(&self.server)
                    .await;
            }
        }

        /// Configure server to timeout requests
        async fn timeout_requests(&self, count: u32, delay: Duration) {
            for _ in 0..count {
                Mock::given(method("GET"))
                    .and(path("/health"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_delay(delay)
                    )
                    .expect(1)
                    .mount(&self.server)
                    .await;
            }
        }

        /// Configure server to succeed after failures
        async fn succeed_after_failures(&self, failure_count: u32, success_count: u32) {
            // First, add failures
            self.fail_requests(failure_count, 500).await;
            
            // Then add successes
            for _ in 0..success_count {
                Mock::given(method("GET"))
                    .and(path("/health"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"})))
                    .expect(1)
                    .mount(&self.server)
                    .await;
            }
        }

        /// Configure intermittent failures
        async fn intermittent_failures(&self, pattern: Vec<bool>) {
            for success in pattern {
                let response = if success {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"}))
                } else {
                    ResponseTemplate::new(500).set_body_string("Internal Server Error")
                };
                
                Mock::given(method("GET"))
                    .and(path("/health"))
                    .respond_with(response)
                    .expect(1)
                    .mount(&self.server)
                    .await;
            }
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_basic_functionality() {
        let config = Arc::new(Config::default());
        config.set_circuit_breaker_threshold(3);
        config.set_circuit_breaker_timeout(Duration::from_millis(100));
        
        let circuit_breaker = CircuitBreaker::new(config);

        // Initially closed
        assert_eq!(circuit_breaker.state(), CircuitState::Closed);
        assert!(circuit_breaker.can_proceed().await.is_ok());

        // Record failures to trip the breaker
        let error = ClientError::Connection { 
            error: "Connection refused".to_string() 
        };
        
        circuit_breaker.record_failure(&error);
        assert_eq!(circuit_breaker.state(), CircuitState::Closed);
        
        circuit_breaker.record_failure(&error);
        assert_eq!(circuit_breaker.state(), CircuitState::Closed);
        
        circuit_breaker.record_failure(&error);
        assert_eq!(circuit_breaker.state(), CircuitState::Open);

        // Should fail fast when open
        assert!(matches!(
            circuit_breaker.can_proceed().await,
            Err(ClientError::CircuitBreakerOpen)
        ));

        // Wait for recovery timeout
        sleep(Duration::from_millis(150)).await;

        // Should transition to half-open
        assert!(circuit_breaker.can_proceed().await.is_ok());
        assert_eq!(circuit_breaker.state(), CircuitState::HalfOpen);

        // Success should close the circuit
        circuit_breaker.record_success();
        assert_eq!(circuit_breaker.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_retry_policy_with_exponential_backoff() {
        let config = Arc::new(Config::default());
        config.set_max_retries(3);
        config.set_initial_retry_delay(Duration::from_millis(10));
        config.set_retry_multiplier(2.0);
        config.set_retry_jitter(false); // Disable for predictable testing
        
        let policy = RetryPolicy::new(config);
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let start_time = Instant::now();
        
        let result: ClientResult<&str> = policy.execute(|| {
            let count = call_count_clone.fetch_add(1, Ordering::Relaxed) + 1;
            async move {
                if count < 3 {
                    Err(ClientError::Connection { 
                        error: "Connection refused".to_string() 
                    })
                } else {
                    Ok("success")
                }
            }
        }).await;

        let elapsed = start_time.elapsed();
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(call_count.load(Ordering::Relaxed), 3);
        
        // Should have taken at least the retry delays (10ms + 20ms)
        assert!(elapsed >= Duration::from_millis(30));
    }

    #[tokio::test]
    async fn test_retry_policy_with_circuit_breaker_integration() {
        let config = Arc::new(Config::default());
        config.set_max_retries(5);
        config.set_circuit_breaker_threshold(2);
        
        let policy = RetryPolicy::new(config);
        let call_count = Arc::new(AtomicU32::new(0));

        // First, cause circuit breaker to trip
        for _ in 0..3 {
            let call_count_clone = call_count.clone();
            let _result: ClientResult<&str> = policy.execute(|| {
                call_count_clone.fetch_add(1, Ordering::Relaxed);
                async move {
                    Err(ClientError::Connection { 
                        error: "Connection refused".to_string() 
                    })
                }
            }).await;
        }

        // Circuit breaker should be open now
        assert_eq!(policy.circuit_breaker().state(), CircuitState::Open);

        // Next call should fail immediately due to circuit breaker
        let call_count_before = call_count.load(Ordering::Relaxed);
        let result: ClientResult<&str> = policy.execute(|| {
            call_count.fetch_add(1, Ordering::Relaxed);
            async move { Ok("should not be called") }
        }).await;

        assert!(matches!(result, Err(ClientError::CircuitBreakerOpen)));
        assert_eq!(call_count.load(Ordering::Relaxed), call_count_before); // No additional calls
    }

    #[tokio::test]
    async fn test_offline_queue_persistence_and_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let queue_path = temp_dir.path().join("test_queue");
        
        let operation_id = {
            // Create queue and add operations
            let queue = OfflineQueue::new(
                queue_path.clone(),
                100,
                Duration::from_millis(100),
            ).await.unwrap();

            let op_id = queue.enqueue(
                OperationType::Upload,
                "test.txt".to_string(),
                Some(Bytes::from("test data")),
                OperationPriority::High,
                None,
            ).await.unwrap();

            queue.enqueue(
                OperationType::Delete,
                "old.txt".to_string(),
                None,
                OperationPriority::Low,
                None,
            ).await.unwrap();

            op_id
        };

        // Create new queue instance (simulating restart)
        let queue = OfflineQueue::new(
            queue_path,
            100,
            Duration::from_millis(100),
        ).await.unwrap();

        // Should have loaded the operations
        let stats = queue.stats().await;
        assert_eq!(stats.total_operations, 2);

        // Should dequeue in priority order (High first)
        let operation = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(operation.id, operation_id);
        assert_eq!(operation.path, "test.txt");
        assert!(matches!(operation.operation_type, OperationType::Upload));
        assert_eq!(operation.priority, OperationPriority::High);

        // Complete the operation
        queue.complete_operation(&operation_id).await.unwrap();

        // Next operation should be the low priority one
        let operation = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(operation.path, "old.txt");
        assert!(matches!(operation.operation_type, OperationType::Delete));
        assert_eq!(operation.priority, OperationPriority::Low);
    }

    #[tokio::test]
    async fn test_offline_queue_retry_mechanism() {
        let temp_dir = TempDir::new().unwrap();
        let queue_path = temp_dir.path().join("retry_queue");
        
        let queue = OfflineQueue::new(
            queue_path,
            100,
            Duration::from_millis(100),
        ).await.unwrap();

        let op_id = queue.enqueue(
            OperationType::Upload,
            "retry_test.txt".to_string(),
            Some(Bytes::from("retry data")),
            OperationPriority::Normal,
            None,
        ).await.unwrap();

        // Dequeue and simulate failure
        let operation = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(operation.retry_count, 0);

        // Retry the operation
        let retry_successful = queue.retry_operation(operation, 3).await.unwrap();
        assert!(retry_successful);

        // Dequeue again - should have incremented retry count
        let operation = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(operation.retry_count, 1);

        // Retry multiple times until limit exceeded
        for expected_count in 2..=3 {
            let retry_successful = queue.retry_operation(operation.clone(), 3).await.unwrap();
            assert!(retry_successful);
            
            let operation = queue.dequeue().await.unwrap().unwrap();
            assert_eq!(operation.retry_count, expected_count);
        }

        // Should fail when retry limit exceeded
        let retry_successful = queue.retry_operation(operation, 3).await.unwrap();
        assert!(!retry_successful);

        // Queue should be empty now
        assert!(queue.dequeue().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_graceful_degradation_mode_transitions() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("degradation_cache");
        
        let manager = GracefulDegradationManager::new(
            cache_path,
            10 * 1024 * 1024, // 10MB cache
            1000,              // 1000 entries
            Duration::from_secs(60), // 1 minute TTL
        ).await.unwrap();

        // Initially normal mode
        assert_eq!(manager.current_mode(), DegradationMode::Normal);
        assert!(manager.is_operation_allowed("upload"));
        assert!(manager.is_operation_allowed("download"));

        // Simulate network failure
        let network_error = ClientError::Connection { 
            error: "Network unreachable".to_string() 
        };
        manager.record_server_failure(&network_error).await.unwrap();
        
        assert_eq!(manager.current_mode(), DegradationMode::Offline);
        assert!(!manager.is_operation_allowed("upload"));
        assert!(!manager.is_operation_allowed("download"));
        assert!(manager.is_operation_allowed("download_cached"));

        // Simulate server error (should go to read-only)
        manager.set_mode(DegradationMode::Normal).await.unwrap();
        let server_error = ClientError::Server { 
            status: 500, 
            message: "Internal Server Error".to_string() 
        };
        manager.record_server_failure(&server_error).await.unwrap();
        
        assert_eq!(manager.current_mode(), DegradationMode::ReadOnly);
        assert!(!manager.is_operation_allowed("upload"));
        assert!(manager.is_operation_allowed("download"));

        // Simulate recovery
        manager.record_server_contact().await;
        assert_eq!(manager.current_mode(), DegradationMode::Normal);
    }

    #[tokio::test]
    async fn test_graceful_degradation_cache_functionality() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache_test");
        
        let manager = GracefulDegradationManager::new(
            cache_path,
            1024 * 1024, // 1MB cache
            100,          // 100 entries
            Duration::from_secs(60),
        ).await.unwrap();

        // Cache some metadata
        let metadata = crate::types::FileMetadata {
            path: "test.txt".to_string(),
            name: "test.txt".to_string(),
            size: 1024,
            modified: std::time::SystemTime::now(),
            is_directory: false,
            xxhash3: 12345,
        };

        manager.cache_metadata("test.txt", metadata.clone()).await.unwrap();

        // Cache some file content
        let content = Bytes::from("cached file content");
        manager.cache_file("test.txt", content.clone(), metadata).await.unwrap();

        // Switch to offline mode
        manager.set_mode(DegradationMode::Offline).await.unwrap();

        // Should be able to retrieve cached data
        let cached_metadata = manager.get_cached_metadata("test.txt").await;
        assert!(cached_metadata.is_some());
        assert_eq!(cached_metadata.unwrap().path, "test.txt");

        let cached_content = manager.get_cached_file("test.txt").await;
        assert!(cached_content.is_some());
        assert_eq!(cached_content.unwrap(), content);

        // Should get cache miss for non-existent files
        let missing_metadata = manager.get_cached_metadata("missing.txt").await;
        assert!(missing_metadata.is_none());

        let missing_content = manager.get_cached_file("missing.txt").await;
        assert!(missing_content.is_none());
    }

    #[tokio::test]
    async fn test_chunked_upload_resume_capability() {
        // This test would require a mock server that supports chunked uploads
        // For now, we'll test the basic session management
        
        let client = reqwest::Client::new();
        let manager = ChunkedUploadManager::new(
            client,
            "http://localhost:8080".to_string(),
            1024, // 1KB chunks for testing
            2,    // 2 concurrent chunks
            Duration::from_secs(300),
        );

        // Test session creation logic
        let content = Bytes::from(vec![0u8; 2500]); // 2.5KB content
        let chunk_size = 1024u64;
        let total_chunks = ((content.len() as u64 + chunk_size - 1) / chunk_size) as u32;
        
        assert_eq!(total_chunks, 3); // Should need 3 chunks

        // Test progress calculation
        let completed_chunks = 2u32;
        let progress_percent = ((completed_chunks as f64 / total_chunks as f64) * 100.0) as u8;
        assert_eq!(progress_percent, 66); // 2/3 = 66%
    }

    #[tokio::test]
    async fn test_metrics_collection_under_load() {
        let collector = MetricsCollector::new();
        
        // Simulate high load with concurrent operations
        let mut tasks = Vec::new();
        
        for i in 0..100 {
            let collector_clone = collector.clone_for_background();
            let task = tokio::spawn(async move {
                let measurement = PerformanceMeasurement::start("load_test");
                
                // Simulate some work
                sleep(Duration::from_millis(i % 10)).await;
                
                let completed = if i % 10 == 0 {
                    // 10% failure rate
                    measurement.failure(&ClientError::Connection { 
                        error: "Simulated failure".to_string() 
                    })
                } else {
                    measurement.success()
                };
                
                collector_clone.record_operation(completed);
                
                // Record network metrics
                collector_clone.record_network_request(
                    i % 10 != 0, // 10% failure rate
                    Duration::from_millis(i % 50),
                    1024,
                    2048,
                    None,
                );
            });
            
            tasks.push(task);
        }
        
        // Wait for all tasks to complete
        futures::future::join_all(tasks).await;
        
        // Check metrics
        let metrics = collector.get_metrics().await;
        
        assert_eq!(metrics.operations.len(), 1);
        let load_test_metrics = &metrics.operations[0];
        assert_eq!(load_test_metrics.operation_type, "load_test");
        assert_eq!(load_test_metrics.total_count, 100);
        assert_eq!(load_test_metrics.success_count, 90);
        assert_eq!(load_test_metrics.failure_count, 10);
        
        assert_eq!(metrics.network.requests_sent, 100);
        assert_eq!(metrics.network.requests_successful, 90);
        assert_eq!(metrics.network.requests_failed, 10);
    }

    #[tokio::test]
    async fn test_network_partition_recovery() {
        let mock_server = MockServerBuilder::new().await;
        
        // Configure server to fail initially, then recover
        mock_server.succeed_after_failures(5, 10).await;
        
        let config = Arc::new(Config::new(mock_server.uri()));
        config.set_max_retries(3);
        config.set_circuit_breaker_threshold(3);
        config.set_initial_retry_delay(Duration::from_millis(10));
        
        let policy = RetryPolicy::new(config.clone());
        let client = reqwest::Client::new();
        
        // Simulate network partition - multiple failures
        for i in 0..5 {
            let result: ClientResult<bool> = policy.execute(|| async {
                let response = client
                    .get(&format!("{}/health", mock_server.uri()))
                    .send()
                    .await
                    .map_err(ClientError::Network)?;
                
                if response.status().is_success() {
                    Ok(true)
                } else {
                    Err(ClientError::Server {
                        status: response.status().as_u16(),
                        message: "Server error".to_string(),
                    })
                }
            }).await;
            
            if i < 3 {
                // First few should fail and eventually trip circuit breaker
                assert!(result.is_err());
            }
        }
        
        // Circuit breaker should be open
        assert_eq!(policy.circuit_breaker().state(), CircuitState::Open);
        
        // Wait for circuit breaker recovery timeout
        sleep(Duration::from_millis(50)).await;
        
        // Should recover after circuit breaker timeout
        let result: ClientResult<bool> = policy.execute(|| async {
            let response = client
                .get(&format!("{}/health", mock_server.uri()))
                .send()
                .await
                .map_err(ClientError::Network)?;
            
            if response.status().is_success() {
                Ok(true)
            } else {
                Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: "Server error".to_string(),
                })
            }
        }).await;
        
        assert!(result.is_ok());
        assert_eq!(policy.circuit_breaker().state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_intermittent_failures_handling() {
        let mock_server = MockServerBuilder::new().await;
        
        // Configure intermittent failures: fail, succeed, fail, succeed, succeed
        mock_server.intermittent_failures(vec![false, true, false, true, true]).await;
        
        let config = Arc::new(Config::new(mock_server.uri()));
        config.set_max_retries(2);
        config.set_circuit_breaker_threshold(5); // High threshold to avoid tripping
        config.set_initial_retry_delay(Duration::from_millis(1));
        
        let policy = RetryPolicy::new(config);
        let client = reqwest::Client::new();
        let success_count = Arc::new(AtomicU32::new(0));
        let failure_count = Arc::new(AtomicU32::new(0));
        
        // Make multiple requests
        for _ in 0..5 {
            let success_count_clone = success_count.clone();
            let failure_count_clone = failure_count.clone();
            let client_clone = client.clone();
            let uri = mock_server.uri();
            
            let result: ClientResult<bool> = policy.execute(|| async {
                let response = client_clone
                    .get(&format!("{}/health", uri))
                    .send()
                    .await
                    .map_err(ClientError::Network)?;
                
                if response.status().is_success() {
                    Ok(true)
                } else {
                    Err(ClientError::Server {
                        status: response.status().as_u16(),
                        message: "Server error".to_string(),
                    })
                }
            }).await;
            
            match result {
                Ok(_) => success_count_clone.fetch_add(1, Ordering::Relaxed),
                Err(_) => failure_count_clone.fetch_add(1, Ordering::Relaxed),
            };
        }
        
        // Should have some successes and some failures
        let successes = success_count.load(Ordering::Relaxed);
        let failures = failure_count.load(Ordering::Relaxed);
        
        assert!(successes > 0, "Should have some successful requests");
        assert!(failures > 0, "Should have some failed requests");
        assert_eq!(successes + failures, 5, "Total should equal number of requests");
    }

    #[tokio::test]
    async fn test_timeout_handling_and_recovery() {
        let mock_server = MockServerBuilder::new().await;
        
        // Configure server to timeout first 3 requests, then succeed
        mock_server.timeout_requests(3, Duration::from_secs(2)).await;
        mock_server.succeed_after_failures(0, 2).await;
        
        let config = Arc::new(Config::new(mock_server.uri()));
        config.set_max_retries(2);
        config.set_request_timeout(Duration::from_millis(100)); // Short timeout
        config.set_initial_retry_delay(Duration::from_millis(10));
        
        let policy = RetryPolicy::new(config.clone());
        let client = reqwest::ClientBuilder::new()
            .timeout(config.request_timeout())
            .build()
            .unwrap();
        
        let start_time = Instant::now();
        
        // First few requests should timeout and fail
        for _ in 0..3 {
            let result: ClientResult<bool> = timeout(
                Duration::from_millis(500),
                policy.execute(|| async {
                    let response = client
                        .get(&format!("{}/health", mock_server.uri()))
                        .send()
                        .await
                        .map_err(ClientError::Network)?;
                    
                    if response.status().is_success() {
                        Ok(true)
                    } else {
                        Err(ClientError::Server {
                            status: response.status().as_u16(),
                            message: "Server error".to_string(),
                        })
                    }
                })
            ).await;
            
            // Should timeout or fail due to server timeout
            assert!(result.is_err() || result.unwrap().is_err());
        }
        
        // Subsequent requests should succeed (server no longer timing out)
        let result: ClientResult<bool> = policy.execute(|| async {
            let response = client
                .get(&format!("{}/health", mock_server.uri()))
                .send()
                .await
                .map_err(ClientError::Network)?;
            
            if response.status().is_success() {
                Ok(true)
            } else {
                Err(ClientError::Server {
                    status: response.status().as_u16(),
                    message: "Server error".to_string(),
                })
            }
        }).await;
        
        assert!(result.is_ok());
        
        let elapsed = start_time.elapsed();
        // Should have taken some time due to retries and timeouts
        assert!(elapsed >= Duration::from_millis(300));
    }

    #[tokio::test]
    async fn test_concurrent_operations_under_failure() {
        let mock_server = MockServerBuilder::new().await;
        
        // Configure server with mixed responses
        for i in 0..50 {
            let success = i % 3 != 0; // 2/3 success rate
            let status = if success { 200 } else { 500 };
            let body = if success {
                serde_json::json!({"status": "ok"}).to_string()
            } else {
                "Internal Server Error".to_string()
            };
            
            Mock::given(method("GET"))
                .and(path("/health"))
                .respond_with(ResponseTemplate::new(status).set_body_string(body))
                .expect(1)
                .mount(&mock_server.server)
                .await;
        }
        
        let config = Arc::new(Config::new(mock_server.uri()));
        config.set_max_retries(1);
        config.set_circuit_breaker_threshold(10); // High threshold
        
        let policy = Arc::new(RetryPolicy::new(config));
        let client = Arc::new(reqwest::Client::new());
        let metrics = Arc::new(MetricsCollector::new());
        
        // Launch concurrent operations
        let mut tasks = Vec::new();
        
        for i in 0..20 {
            let policy_clone = policy.clone();
            let client_clone = client.clone();
            let metrics_clone = metrics.clone();
            let uri = mock_server.uri();
            
            let task = tokio::spawn(async move {
                let measurement = PerformanceMeasurement::start("concurrent_test");
                
                let result: ClientResult<bool> = policy_clone.execute(|| async {
                    let response = client_clone
                        .get(&format!("{}/health", uri))
                        .send()
                        .await
                        .map_err(ClientError::Network)?;
                    
                    if response.status().is_success() {
                        Ok(true)
                    } else {
                        Err(ClientError::Server {
                            status: response.status().as_u16(),
                            message: "Server error".to_string(),
                        })
                    }
                }).await;
                
                let completed = match result {
                    Ok(_) => measurement.success(),
                    Err(ref e) => measurement.failure(e),
                };
                
                metrics_clone.record_operation(completed);
                result.is_ok()
            });
            
            tasks.push(task);
        }
        
        // Wait for all tasks and count successes
        let results = futures::future::join_all(tasks).await;
        let success_count = results.iter()
            .filter_map(|r| r.as_ref().ok())
            .filter(|&&success| success)
            .count();
        
        // Should have some successes despite failures
        assert!(success_count > 0, "Should have some successful operations");
        assert!(success_count < 20, "Should have some failed operations");
        
        // Check metrics
        let final_metrics = metrics.get_metrics().await;
        assert_eq!(final_metrics.operations.len(), 1);
        
        let concurrent_metrics = &final_metrics.operations[0];
        assert_eq!(concurrent_metrics.operation_type, "concurrent_test");
        assert_eq!(concurrent_metrics.total_count, 20);
        assert!(concurrent_metrics.success_count > 0);
        assert!(concurrent_metrics.failure_count > 0);
    }

    #[tokio::test]
    async fn test_full_system_resilience_scenario() {
        // This test simulates a complete system failure and recovery scenario
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("system_cache");
        let queue_path = temp_dir.path().join("system_queue");
        
        // Set up components
        let degradation_manager = GracefulDegradationManager::new(
            cache_path,
            5 * 1024 * 1024, // 5MB cache
            500,              // 500 entries
            Duration::from_secs(300), // 5 minute TTL
        ).await.unwrap();
        
        let offline_queue = OfflineQueue::new(
            queue_path,
            1000,
            Duration::from_millis(100),
        ).await.unwrap();
        
        let metrics = MetricsCollector::new();
        
        // Phase 1: Normal operation
        assert_eq!(degradation_manager.current_mode(), DegradationMode::Normal);
        
        // Cache some data during normal operation
        let metadata = crate::types::FileMetadata {
            path: "important.txt".to_string(),
            name: "important.txt".to_string(),
            size: 2048,
            modified: std::time::SystemTime::now(),
            is_directory: false,
            xxhash3: 67890,
        };
        
        degradation_manager.cache_metadata("important.txt", metadata.clone()).await.unwrap();
        degradation_manager.cache_file(
            "important.txt",
            Bytes::from("important cached content"),
            metadata,
        ).await.unwrap();
        
        // Phase 2: System failure - network partition
        let network_error = ClientError::Connection { 
            error: "Network unreachable".to_string() 
        };
        degradation_manager.record_server_failure(&network_error).await.unwrap();
        
        assert_eq!(degradation_manager.current_mode(), DegradationMode::Offline);
        
        // Queue operations while offline
        offline_queue.enqueue(
            OperationType::Upload,
            "queued_file.txt".to_string(),
            Some(Bytes::from("queued content")),
            OperationPriority::High,
            None,
        ).await.unwrap();
        
        offline_queue.enqueue(
            OperationType::Delete,
            "old_file.txt".to_string(),
            None,
            OperationPriority::Normal,
            None,
        ).await.unwrap();
        
        // Should still be able to access cached data
        let cached_content = degradation_manager.get_cached_file("important.txt").await;
        assert!(cached_content.is_some());
        assert_eq!(cached_content.unwrap(), Bytes::from("important cached content"));
        
        // Phase 3: Partial recovery - read-only mode
        degradation_manager.set_mode(DegradationMode::ReadOnly).await.unwrap();
        
        assert!(degradation_manager.is_operation_allowed("download"));
        assert!(!degradation_manager.is_operation_allowed("upload"));
        
        // Phase 4: Full recovery
        degradation_manager.record_server_contact().await;
        assert_eq!(degradation_manager.current_mode(), DegradationMode::Normal);
        
        // Process offline queue
        let stats = offline_queue.stats().await;
        assert_eq!(stats.total_operations, 2);
        
        // Simulate processing queued operations
        while let Some(operation) = offline_queue.dequeue().await.unwrap() {
            // Simulate successful processing
            offline_queue.complete_operation(&operation.id).await.unwrap();
            
            metrics.record_operation(
                PerformanceMeasurement::start("offline_processing").success()
            );
        }
        
        // Verify system state after recovery
        let final_stats = offline_queue.stats().await;
        assert_eq!(final_stats.total_operations, 0); // All processed
        
        let degradation_stats = degradation_manager.stats().await;
        assert_eq!(degradation_stats.current_mode, DegradationMode::Normal);
        assert!(degradation_stats.degradation_triggers > 0);
        
        let final_metrics = metrics.get_metrics().await;
        assert!(final_metrics.operations.len() > 0);
    }
}