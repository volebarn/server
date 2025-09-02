# Volebarn File Synchronization System

A high-performance file synchronization system built with Rust, featuring zero-copy operations, lock-free data structures, and async I/O throughout.

## Architecture

Volebarn consists of three separate Rust crates:

- **volebarn-server**: Backend HTTP server with persistent storage
- **volebarn-client**: Reusable async client library  
- **volebarn-console**: Console application for file monitoring and sync

## Project Structure

```
volebarn/
├── Cargo.toml                 # Workspace configuration
├── storage/                   # Server storage directory
│   ├── metadata/             # RocksDB database files
│   ├── files/                # Content-addressable file storage
│   ├── temp/                 # Temporary files during operations
│   └── config/               # Server configuration
├── volebarn-server/          # Backend server crate
│   ├── src/
│   │   ├── lib.rs           # Library entry point
│   │   ├── main.rs          # Server binary
│   │   ├── error.rs         # Error types
│   │   ├── storage.rs       # Storage layer
│   │   ├── handlers.rs      # HTTP handlers
│   │   ├── server.rs        # Main server logic
│   │   ├── types.rs         # Data types
│   │   ├── hash.rs          # Hash utilities
│   │   └── tls.rs           # TLS configuration
│   └── Cargo.toml
├── volebarn-client/          # Client library crate
│   ├── src/
│   │   ├── lib.rs           # Library entry point
│   │   ├── client.rs        # Main client implementation
│   │   ├── error.rs         # Error types
│   │   ├── hash.rs          # Hash utilities
│   │   ├── config.rs        # Configuration
│   │   ├── retry.rs         # Retry logic
│   │   └── types.rs         # Data types
│   └── Cargo.toml
└── volebarn-console/         # Console application crate
    ├── src/
    │   ├── lib.rs           # Library entry point
    │   ├── main.rs          # Console binary
    │   ├── app.rs           # Main application
    │   ├── config.rs        # Configuration
    │   ├── error.rs         # Error types
    │   ├── file_index.rs    # File indexing
    │   ├── file_watcher.rs  # File system monitoring
    │   ├── local_file_manager.rs # Local file operations
    │   ├── sync_manager.rs  # Sync coordination
    │   └── conflict_resolver.rs # Conflict resolution
    └── Cargo.toml
```

## Key Technologies

- **Tokio**: Async runtime for all I/O operations
- **Axum**: High-performance HTTP server framework
- **RocksDB**: Persistent metadata storage with column families
- **DashMap**: Lock-free concurrent HashMap for temporary state
- **Bytes**: Zero-copy byte buffers for file content
- **xxHash3**: Fast file integrity verification
- **rustls**: TLS 1.3 implementation
- **notify**: File system monitoring

## Development Standards

- **Zero-lock architecture**: No mutexes or RwLocks, only atomic types and DashMap
- **Zero-copy operations**: Use `Bytes` type for all file content transfers
- **Async throughout**: All operations use async/await patterns
- **Persistent storage**: RocksDB for metadata, file system for content
- **Lock-free patterns**: DashMap for temporary state, atomic types for counters

## Building

```bash
# Check all crates
cargo check --workspace

# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run server
cargo run --bin volebarn-server

# Run console client
cargo run --bin volebarn-console -- --server https://localhost:8080 --folder ./sync
```

## Storage Architecture

The server uses a hybrid storage approach:
- **RocksDB**: Persistent metadata with column families for files, directories, and indexes
- **File System**: Content-addressable storage for actual file content
- **DashMap**: Temporary request-scoped state (not persistent)

Files are deduplicated using xxHash3 and stored in a balanced directory tree structure for optimal filesystem performance.

## License

This project is licensed under the [Apache License 2.0](LICENSE).