# Volebarn Storage Directory Structure

This directory contains the persistent storage for the Volebarn server.

## Directory Layout

- `metadata/` - RocksDB database files for file metadata and indexes
- `files/` - Content-addressable file storage organized by hash
- `temp/` - Temporary files during upload operations
- `config/` - Server configuration files

## File Storage Organization

Files are stored using content-addressable storage based on xxHash3:
```
files/
├── ab/cd/ef/abcdef1234567890  # First 6 chars create directory structure
├── 12/34/56/1234567890abcdef
└── ...
```

This structure provides:
- Deduplication (identical files share the same storage)
- Fast lookup by hash
- Balanced directory tree for filesystem performance
- Atomic operations using temp files and rename

## RocksDB Column Families

The metadata database uses the following column families:
- `files` - File metadata indexed by path
- `directories` - Directory structure and metadata
- `hash_index` - Hash to path mapping for deduplication
- `modified_index` - Time-based indexing for sync operations