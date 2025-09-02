# Requirements Document

## Introduction

This feature implements Volebarn, a minimal file synchronization system consisting of three separate Rust crates: a backend server using Tokio and Axum with in-memory storage, a console client application for file synchronization, and a shared client library for cross-platform compatibility. The system provides basic file operations (upload, download, delete, update) without authentication, focusing on demonstrating core functionality with zero-copy optimizations where possible.

## Requirements

### Requirement 1

**User Story:** As a developer, I want a lightweight backend server that handles file operations, so that I can store and retrieve files through a REST API.

#### Acceptance Criteria

1. WHEN the server starts THEN it SHALL bind to a configurable IP and port using Axum
2. WHEN a file upload request is received with a path THEN the server SHALL store the file in memory with full path support
3. WHEN a file download request is received with a path THEN the server SHALL return the file content using zero-copy techniques
4. WHEN a file delete request is received THEN the server SHALL remove the file from memory storage
5. WHEN a file update request is received THEN the server SHALL replace the existing file content
6. WHEN a directory list request is received THEN the server SHALL return metadata for all files and subdirectories
7. WHEN a directory creation request is received THEN the server SHALL create the directory structure
8. WHEN a directory deletion request is received THEN the server SHALL recursively remove the directory and its contents
9. WHEN a bulk upload request is received THEN the server SHALL process multiple files with their directory structure
10. WHEN a bulk download request is received THEN the server SHALL return an archive containing the requested directory tree
11. WHEN a sync request is received with a client manifest THEN the server SHALL compare with its manifest and return a sync plan treating server as source of truth
12. WHEN a sync plan is applied THEN the server SHALL receive file uploads from client as specified in the plan
13. WHEN a bulk delete request is received THEN the server SHALL delete multiple files and directories efficiently
14. WHEN a file metadata request is received THEN the server SHALL return file information without transferring content
15. WHEN a move/rename request is received THEN the server SHALL relocate the file or directory to the new path
16. WHEN a copy request is received THEN the server SHALL duplicate the file or directory to the new path
17. WHEN a search request is received THEN the server SHALL return files matching the specified pattern
18. WHEN files are uploaded or downloaded THEN the server SHALL verify file integrity using xxHash3
19. WHEN the server starts THEN it SHALL use TLS 1.3 with rustls for secure communication

### Requirement 2

**User Story:** As a user, I want a console application that monitors a local folder, so that I can automatically sync files with the backend server.

#### Acceptance Criteria

1. WHEN the console app starts THEN it SHALL read configuration for server IP, port, and monitored folder path
2. WHEN a file is added to any subdirectory in the monitored folder THEN the app SHALL upload it to the backend server with correct path
3. WHEN a file is modified in any subdirectory THEN the app SHALL update it on the backend server
4. WHEN a file is deleted from any subdirectory THEN the app SHALL delete it from the backend server
5. WHEN a directory is created locally THEN the app SHALL create the corresponding directory on the server
6. WHEN a directory is deleted locally THEN the app SHALL delete the corresponding directory on the server
7. WHEN the app starts THEN it SHALL maintain an in-memory index of the entire directory tree with checksums for change detection
8. WHEN performing full sync THEN the app SHALL treat server as source of truth and sync local state to match server
9. WHEN files exist locally but not on server THEN the app SHALL upload them to server
10. WHEN files exist on server but not locally THEN the app SHALL download them locally
11. WHEN files are deleted on server THEN the app SHALL delete them locally during sync
12. WHEN sync conflicts occur (same file modified on both sides) THEN the app SHALL resolve them based on configured strategy
10. WHEN files are moved or renamed locally THEN the app SHALL update the corresponding files on the server
13. WHEN transferring files THEN the app SHALL verify file integrity using xxHash3 comparison
14. WHEN network errors occur THEN the app SHALL retry operations with exponential backoff

### Requirement 3

**User Story:** As a developer, I want a shared client library, so that I can reuse the same logic across different client applications (console, desktop, etc.).

#### Acceptance Criteria

1. WHEN the client library is used THEN it SHALL provide async methods for all file operations including bulk operations
2. WHEN making HTTP requests THEN the library SHALL use efficient serialization and zero-copy where possible
3. WHEN handling file operations THEN the library SHALL provide error handling and retry mechanisms
4. WHEN managing file metadata THEN the library SHALL maintain consistent data structures across platforms
5. WHEN streaming files THEN the library SHALL support chunked uploads and downloads for large files
6. WHEN performing bulk operations THEN the library SHALL efficiently handle multiple files in single requests
7. WHEN performing full sync THEN the library SHALL compare local and remote manifests with server as source of truth
8. WHEN executing sync plans THEN the library SHALL handle uploads to server, downloads from server, and local deletions
9. WHEN sync conflicts occur THEN the library SHALL resolve them based on configured strategy or return for manual resolution
10. WHEN downloading multiple files THEN the library SHALL use zero-copy patterns and return individual files without archiving
11. WHEN getting file metadata THEN the library SHALL provide efficient HEAD requests without downloading content
12. WHEN moving or copying files THEN the library SHALL support both single file and directory operations
13. WHEN searching for files THEN the library SHALL support pattern matching within specified directories
14. WHEN transferring files THEN the library SHALL verify integrity using xxHash3 and detect corruption
15. WHEN connecting to server THEN the library SHALL use TLS 1.3 with rustls for secure communication

### Requirement 4

**User Story:** As a system administrator, I want configurable server and client settings, so that I can deploy the system in different environments.

#### Acceptance Criteria

1. WHEN the server starts THEN it SHALL read configuration from environment variables or config files
2. WHEN the console client starts THEN it SHALL read configuration for server endpoint and local folder
3. WHEN configuration is invalid THEN the applications SHALL display clear error messages and exit gracefully
4. WHEN no configuration is provided THEN the applications SHALL use sensible defaults

### Requirement 5

**User Story:** As a developer, I want the system to use modern Rust practices, so that the code is maintainable and performant.

#### Acceptance Criteria

1. WHEN implementing the backend THEN it SHALL use Tokio for async runtime and Axum for HTTP handling
2. WHEN handling file operations THEN the system SHALL implement zero-copy optimizations where applicable
3. WHEN structuring the project THEN it SHALL use separate crates for server, client library, and console app
4. WHEN managing dependencies THEN it SHALL use minimal necessary dependencies for each crate
5. WHEN handling errors THEN it SHALL use proper Rust error handling patterns with custom error types