//! Tests for async file system monitoring

use super::*;
use std::fs;
use tempfile::TempDir;
use tokio::time::{sleep, timeout, Instant};


/// Helper function to create a test directory structure
async fn create_test_dir() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let path = temp_dir.path().to_path_buf();
    
    // Create some initial structure
    fs::create_dir_all(path.join("subdir")).expect("Failed to create subdir");
    fs::write(path.join("file1.txt"), "content1").expect("Failed to write file1.txt");
    fs::write(path.join("subdir/file2.txt"), "content2").expect("Failed to write file2.txt");
    
    (temp_dir, path)
}

/// Helper function to wait for and collect events with timeout
async fn collect_events(
    receiver: &mut mpsc::UnboundedReceiver<FileEvent>,
    expected_count: usize,
    timeout_duration: Duration,
) -> Vec<FileEvent> {
    let mut events = Vec::new();
    let start = Instant::now();
    
    while events.len() < expected_count && start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(50), receiver.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                // Timeout on individual recv, but continue trying until overall timeout
                continue;
            }
        }
    }
    
    // Collect any remaining events that arrived
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
        if events.len() >= expected_count {
            break;
        }
    }
    
    events
}

/// Helper function to create a test config without filtering
fn test_config() -> FileWatcherConfig {
    FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ignore_patterns: vec![], // No ignore patterns for testing
        watch_hidden: true, // Watch hidden files for testing
        ..Default::default()
    }
}

/// Helper function to compare paths handling canonicalization
fn paths_equal(path1: &Path, path2: &Path) -> bool {
    let canonical1 = path1.canonicalize().unwrap_or_else(|_| path1.to_path_buf());
    let canonical2 = path2.canonicalize().unwrap_or_else(|_| path2.to_path_buf());
    canonical1 == canonical2
}

#[tokio::test]
async fn test_file_watcher_creation() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig::default();
    
    let (watcher, _receiver) = FileWatcher::new(&path, config);
    
    assert_eq!(watcher.watch_path, path);
    assert!(!watcher.is_running());
}

#[tokio::test]
async fn test_file_watcher_start_stop() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig::default();
    
    let (watcher, _receiver) = FileWatcher::new(&path, config);
    
    // Start the watcher
    watcher.start().await.expect("Failed to start watcher");
    assert!(watcher.is_running());
    
    // Stop the watcher
    watcher.stop().await;
    assert!(!watcher.is_running());
}

#[tokio::test]
async fn test_file_watcher_invalid_path() {
    let config = FileWatcherConfig::default();
    let invalid_path = PathBuf::from("/nonexistent/path");
    
    let (watcher, _receiver) = FileWatcher::new(&invalid_path, config);
    
    let result = watcher.start().await;
    assert!(result.is_err());
    assert!(!watcher.is_running());
}

#[tokio::test]
async fn test_file_creation_detection() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ignore_patterns: vec![], // No ignore patterns for testing
        watch_hidden: true, // Watch hidden files for testing
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(500)).await;
    
    // Create a new file
    let new_file = path.join("new_file.txt");
    fs::write(&new_file, "new content").expect("Failed to create new file");
    
    // Give more time for the event to be processed
    sleep(Duration::from_millis(200)).await;
    
    // Wait for the event
    let events = collect_events(&mut receiver, 1, Duration::from_secs(5)).await;
    
    watcher.stop().await;
    
    assert!(!events.is_empty(), "No file creation events received");
    let event = &events[0];
    assert!(matches!(event, FileEvent::Created { .. }));
    
    // Compare canonical paths to handle symlink resolution
    let expected_canonical = new_file.canonicalize().expect("Failed to canonicalize expected path");
    let actual_canonical = event.path().canonicalize().unwrap_or_else(|_| event.path().to_path_buf());
    assert_eq!(actual_canonical, expected_canonical);
}

#[tokio::test]
async fn test_file_modification_detection() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = test_config();
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(500)).await;
    
    // Modify existing file
    let existing_file = path.join("file1.txt");
    fs::write(&existing_file, "modified content").expect("Failed to modify file");
    
    // Give more time for the event to be processed
    sleep(Duration::from_millis(200)).await;
    
    // Wait for the event
    let events = collect_events(&mut receiver, 1, Duration::from_secs(5)).await;
    
    watcher.stop().await;
    
    assert!(!events.is_empty(), "No file modification events received");
    let event = &events[0];
    
    // File writes can be reported as either Modified or Created events depending on the platform
    assert!(
        matches!(event, FileEvent::Modified { .. }) || matches!(event, FileEvent::Created { .. }),
        "Expected Modified or Created event, got: {:?}", event
    );
    assert!(paths_equal(event.path(), &existing_file));
}

#[tokio::test]
async fn test_file_deletion_detection() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Delete existing file
    let existing_file = path.join("file1.txt");
    fs::remove_file(&existing_file).expect("Failed to delete file");
    
    // Wait for the event
    let events = collect_events(&mut receiver, 1, Duration::from_secs(2)).await;
    
    watcher.stop().await;
    
    assert!(!events.is_empty());
    let event = &events[0];
    assert!(matches!(event, FileEvent::Deleted { .. }));
    assert_eq!(event.path(), &existing_file);
}

#[tokio::test]
async fn test_directory_creation_detection() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Create a new directory
    let new_dir = path.join("new_directory");
    fs::create_dir(&new_dir).expect("Failed to create directory");
    
    // Wait for the event
    let events = collect_events(&mut receiver, 1, Duration::from_secs(2)).await;
    
    watcher.stop().await;
    
    assert!(!events.is_empty());
    let event = &events[0];
    assert!(matches!(event, FileEvent::DirectoryCreated { .. }));
    assert_eq!(event.path(), &new_dir);
}

#[tokio::test]
async fn test_directory_deletion_detection() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Delete existing directory
    let existing_dir = path.join("subdir");
    fs::remove_dir_all(&existing_dir).expect("Failed to delete directory");
    
    // Wait for events (might get multiple events for directory and its contents)
    let events = collect_events(&mut receiver, 1, Duration::from_secs(2)).await;
    
    watcher.stop().await;
    
    assert!(!events.is_empty());
    // Should have at least one directory deletion event
    let has_dir_delete = events.iter().any(|e| {
        matches!(e, FileEvent::DirectoryDeleted { .. }) || 
        matches!(e, FileEvent::Deleted { path } if path == &existing_dir)
    });
    assert!(has_dir_delete);
}

#[tokio::test]
async fn test_recursive_watching() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Create a file in a subdirectory
    let nested_file = path.join("subdir/nested_file.txt");
    fs::write(&nested_file, "nested content").expect("Failed to create nested file");
    
    // Wait for the event
    let events = collect_events(&mut receiver, 1, Duration::from_secs(2)).await;
    
    watcher.stop().await;
    
    assert!(!events.is_empty());
    let event = &events[0];
    assert!(matches!(event, FileEvent::Created { .. }));
    assert_eq!(event.path(), &nested_file);
}

#[tokio::test]
async fn test_ignore_patterns() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ignore_patterns: vec!["*.tmp".to_string(), "*.swp".to_string()],
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Create files that should be ignored
    fs::write(path.join("temp.tmp"), "temp").expect("Failed to create temp file");
    fs::write(path.join("swap.swp"), "swap").expect("Failed to create swap file");
    
    // Create a file that should not be ignored
    fs::write(path.join("normal.txt"), "normal").expect("Failed to create normal file");
    
    // Wait for events
    let events = collect_events(&mut receiver, 1, Duration::from_secs(2)).await;
    
    watcher.stop().await;
    
    // Should only get the normal file event
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert!(matches!(event, FileEvent::Created { .. }));
    assert!(event.path().to_string_lossy().contains("normal.txt"));
}

#[tokio::test]
async fn test_hidden_files_filtering() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        watch_hidden: false,
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Create hidden files that should be ignored
    fs::write(path.join(".hidden"), "hidden").expect("Failed to create hidden file");
    fs::create_dir(path.join(".hidden_dir")).expect("Failed to create hidden dir");
    
    // Create a normal file that should not be ignored
    fs::write(path.join("visible.txt"), "visible").expect("Failed to create visible file");
    
    // Wait for events
    let events = collect_events(&mut receiver, 1, Duration::from_secs(2)).await;
    
    watcher.stop().await;
    
    // Should only get the visible file event
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert!(matches!(event, FileEvent::Created { .. }));
    assert!(event.path().to_string_lossy().contains("visible.txt"));
}

#[tokio::test]
async fn test_debouncing() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(200),
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    let test_file = path.join("debounce_test.txt");
    
    // Rapidly modify the same file multiple times
    for i in 0..5 {
        fs::write(&test_file, format!("content {}", i)).expect("Failed to write file");
        sleep(Duration::from_millis(50)).await; // Less than debounce delay
    }
    
    // Wait for debounce period to pass
    sleep(Duration::from_millis(300)).await;
    
    // Collect events
    let events = collect_events(&mut receiver, 10, Duration::from_millis(500)).await;
    
    watcher.stop().await;
    
    // Should have fewer events than modifications due to debouncing
    assert!(events.len() < 5);
    assert!(!events.is_empty());
}

#[tokio::test]
async fn test_watcher_statistics() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Create some files to generate events
    fs::write(path.join("stats1.txt"), "content1").expect("Failed to create file");
    fs::write(path.join("stats2.txt"), "content2").expect("Failed to create file");
    
    // Wait for events
    let _events = collect_events(&mut receiver, 2, Duration::from_secs(2)).await;
    
    let stats = watcher.stats();
    watcher.stop().await;
    
    // Should have some statistics
    assert!(stats.raw_events_received > 0);
    assert!(stats.events_processed > 0);
}

#[tokio::test]
async fn test_concurrent_event_handling() {
    let (_temp_dir, path) = create_test_dir().await;
    let config = FileWatcherConfig {
        debounce_delay: Duration::from_millis(50),
        ..Default::default()
    };
    
    let (watcher, mut receiver) = FileWatcher::new(&path, config);
    
    watcher.start().await.expect("Failed to start watcher");
    
    // Give the watcher time to initialize
    sleep(Duration::from_millis(100)).await;
    
    // Create multiple files concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let file_path = path.join(format!("concurrent_{}.txt", i));
        let handle = tokio::spawn(async move {
            fs::write(&file_path, format!("content {}", i)).expect("Failed to write file");
        });
        handles.push(handle);
    }
    
    // Wait for all file operations to complete
    for handle in handles {
        handle.await.expect("Task failed");
    }
    
    // Collect events
    let events = collect_events(&mut receiver, 10, Duration::from_secs(3)).await;
    
    watcher.stop().await;
    
    // Should receive events for all files (though order may vary)
    assert_eq!(events.len(), 10);
    
    // All events should be file creations
    for event in &events {
        assert!(matches!(event, FileEvent::Created { .. }));
    }
}

#[tokio::test]
async fn test_pattern_matching() {
    // Test the pattern matching function directly
    assert!(FileWatcher::matches_pattern("file.tmp", "*.tmp"));
    assert!(FileWatcher::matches_pattern("path/file.tmp", "*.tmp"));
    assert!(!FileWatcher::matches_pattern("file.txt", "*.tmp"));
    
    assert!(FileWatcher::matches_pattern("temp_file", "temp*"));
    assert!(!FileWatcher::matches_pattern("file_temp", "temp*"));
    
    assert!(FileWatcher::matches_pattern("file.backup", "*backup"));
    assert!(!FileWatcher::matches_pattern("backup.file", "*backup"));
    
    assert!(FileWatcher::matches_pattern("test.log.old", "*.log.*"));
    assert!(!FileWatcher::matches_pattern("test.txt", "*.log.*"));
    
    assert!(FileWatcher::matches_pattern("exact_match", "exact_match"));
    assert!(FileWatcher::matches_pattern("path/exact_match", "exact_match"));
    assert!(!FileWatcher::matches_pattern("not_exact", "exact_match"));
}

#[tokio::test]
async fn test_file_event_methods() {
    let path = PathBuf::from("/test/path");
    let from_path = PathBuf::from("/test/from");
    let to_path = PathBuf::from("/test/to");
    
    // Test path() method
    let created = FileEvent::Created { path: path.clone() };
    assert_eq!(created.path(), &path);
    
    let modified = FileEvent::Modified { path: path.clone() };
    assert_eq!(modified.path(), &path);
    
    let deleted = FileEvent::Deleted { path: path.clone() };
    assert_eq!(deleted.path(), &path);
    
    let moved = FileEvent::Moved { from: from_path, to: to_path.clone() };
    assert_eq!(moved.path(), &to_path);
    
    let dir_created = FileEvent::DirectoryCreated { path: path.clone() };
    assert_eq!(dir_created.path(), &path);
    
    let dir_deleted = FileEvent::DirectoryDeleted { path: path.clone() };
    assert_eq!(dir_deleted.path(), &path);
    
    // Test is_directory_event() method
    assert!(!created.is_directory_event());
    assert!(!modified.is_directory_event());
    assert!(!deleted.is_directory_event());
    assert!(!moved.is_directory_event());
    assert!(dir_created.is_directory_event());
    assert!(dir_deleted.is_directory_event());
}