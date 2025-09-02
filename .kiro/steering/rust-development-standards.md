# Rust Development Standards

## Core Technologies

- **Runtime**: Use Tokio for async operations
- **Web Framework**: Use Axum for HTTP server implementation
- **Performance**: Implement zero-copy optimizations where possible
- **Architecture**: Separate concerns into distinct crates for modularity

## Project Structure

- Server logic in separate crate
- Client library in separate crate  
- Console application in separate crate
- Shared types and utilities where appropriate

## Async and Concurrency Guidelines

- **All operations must be async** - Use async/await throughout
- **Zero-lock concurrent data structures**: Use DashMap, atomic types, and lock-free patterns exclusively
- **No mutexes or RwLocks** - Avoid Arc<Mutex<>> and Arc<RwLock<>> completely
- **Zero-copy patterns**: Use `Bytes` type for file content, avoid unnecessary cloning
- **Atomic operations**: Use atomic types (AtomicU64, AtomicBool, AtomicUsize) for all shared counters/flags
- **Lock-free collections**: Use DashMap for concurrent HashMap, crossbeam collections where needed
- **Channel communication**: Use tokio::sync channels for async communication between tasks
- **Streaming**: Use tokio::io::AsyncRead/AsyncWrite for large file operations
- **Memory efficiency**: Prefer stack allocation and zero-copy transfers over heap allocations

## Performance Guidelines

- **Zero-lock architecture**: No mutexes, RwLocks, or blocking synchronization primitives
- **Persistent metadata storage**: Use RocksDB for metadata, file system for content
- **Content-addressable storage**: Deduplicate files by hash, atomic file operations
- **Zero-copy file operations**: Use `Bytes` type exclusively for file content
- **Lock-free data structures**: DashMap for temporary request-scoped state, atomic types for counters
- **Efficient serialization**: Use serde_json for API layer, bincode for storage layer
- **Streaming I/O**: Use tokio::io for large file operations without buffering entire files
- **Atomic state management**: AtomicU64 for file sizes, AtomicBool for flags, AtomicUsize for counters
- **Channel-based communication**: Use tokio::sync::mpsc for task coordination
- **Database optimization**: Use RocksDB column families and proper indexing for fast queries
- **Storage separation**: RocksDB for persistent metadata, file system for content, DashMap only for temporary state

## Error Handling

- Use custom error types with proper error propagation
- Implement async retry mechanisms with exponential backoff using tokio::time
- Provide clear error messages for configuration issues
- Use thiserror for error type definitions

## Recommended Crates

- **tokio** - Async runtime and utilities
- **axum** - HTTP server framework
- **reqwest** - HTTP client with async support
- **rocksdb** - Persistent key-value storage for metadata
- **dashmap** - Lock-free concurrent HashMap for in-memory indexes
- **bytes** - Zero-copy byte buffers
- **serde** - Serialization framework
- **serde_json** - JSON serialization for API layer
- **bincode** - Binary serialization for storage layer
- **notify** - File system watching
- **rustls** - TLS implementation
- **xxhash-rust** - Fast hashing
- **thiserror** - Error handling
- **tracing** - Structured logging
- **crossbeam** - Lock-free data structures and utilities
- **uuid** - UUID generation for temporary files
- **tokio-util** - Additional Tokio utilities

## Forbidden Patterns

- **Arc<Mutex<T>>** - Use DashMap or atomic types instead
- **Arc<RwLock<T>>** - Use DashMap or atomic types instead
- **std::sync::Mutex** - Use async alternatives or atomic types
- **std::sync::RwLock** - Use DashMap or atomic types instead
- **Blocking I/O** - Use tokio::io exclusively