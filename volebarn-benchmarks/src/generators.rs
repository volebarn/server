use crate::test_data::*;
use bytes::Bytes;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// Test file generator for creating benchmark files
pub struct TestFileGenerator {
    temp_dir: TempDir,
}

impl TestFileGenerator {
    pub fn new() -> std::io::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        Ok(Self { temp_dir })
    }
    
    pub fn temp_path(&self) -> &Path {
        self.temp_dir.path()
    }
    
    /// Generate all test files for benchmarking
    pub fn generate_all_test_files(&self) -> std::io::Result<Vec<TestFile>> {
        let mut test_files = Vec::new();
        
        for &size in TEST_FILE_SIZES {
            for content_type in [
                ContentType::Text,
                ContentType::Binary,
                ContentType::Mixed,
                ContentType::Code,
                ContentType::Json,
                ContentType::Logs,
            ] {
                let test_file = self.generate_test_file(size, content_type)?;
                test_files.push(test_file);
            }
        }
        
        Ok(test_files)
    }
    
    /// Generate a single test file
    pub fn generate_test_file(&self, size: usize, content_type: ContentType) -> std::io::Result<TestFile> {
        let filename = format!("test_{}_{:?}.dat", size, content_type);
        let file_path = self.temp_dir.path().join(&filename);
        
        // Use a deterministic seed based on size and content type
        let seed = (size as u64).wrapping_mul(match content_type {
            ContentType::Text => 1,
            ContentType::Binary => 2,
            ContentType::Mixed => 3,
            ContentType::Code => 4,
            ContentType::Json => 5,
            ContentType::Logs => 6,
        });
        
        let content = generate_file_content(size, content_type, seed);
        fs::write(&file_path, &content)?;
        
        Ok(TestFile {
            path: file_path,
            size,
            content_type,
            content,
        })
    }
    
    /// Generate test data structures for serialization benchmarks
    pub fn generate_test_structures(&self) -> TestStructures {
        TestStructures {
            small_metadata: generate_file_metadata(10, 12345),
            medium_metadata: generate_file_metadata(100, 23456),
            large_metadata: generate_file_metadata(1000, 34567),
            
            small_directory: generate_directory_listing(50, 45678),
            medium_directory: generate_directory_listing(500, 56789),
            large_directory: generate_directory_listing(5000, 67890),
            
            small_manifest: generate_file_manifest(25, 78901),
            medium_manifest: generate_file_manifest(250, 89012),
            large_manifest: generate_file_manifest(2500, 90123),
            
            small_sync_plan: generate_sync_plan(20, 11111),
            medium_sync_plan: generate_sync_plan(200, 22222),
            large_sync_plan: generate_sync_plan(2000, 33333),
        }
    }
}

/// Test file information
#[derive(Debug, Clone)]
pub struct TestFile {
    pub path: std::path::PathBuf,
    pub size: usize,
    pub content_type: ContentType,
    pub content: Bytes,
}

/// Collection of test data structures for benchmarking
#[derive(Debug)]
pub struct TestStructures {
    // FileMetadata collections of different sizes
    pub small_metadata: Vec<crate::FileMetadata>,
    pub medium_metadata: Vec<crate::FileMetadata>,
    pub large_metadata: Vec<crate::FileMetadata>,
    
    // DirectoryListing of different sizes
    pub small_directory: crate::DirectoryListing,
    pub medium_directory: crate::DirectoryListing,
    pub large_directory: crate::DirectoryListing,
    
    // FileManifest of different sizes
    pub small_manifest: crate::FileManifest,
    pub medium_manifest: crate::FileManifest,
    pub large_manifest: crate::FileManifest,
    
    // SyncPlan of different sizes
    pub small_sync_plan: crate::SyncPlan,
    pub medium_sync_plan: crate::SyncPlan,
    pub large_sync_plan: crate::SyncPlan,
}

impl TestStructures {
    /// Get all metadata collections as a slice
    pub fn all_metadata(&self) -> [&Vec<crate::FileMetadata>; 3] {
        [&self.small_metadata, &self.medium_metadata, &self.large_metadata]
    }
    
    /// Get all directory listings as a slice
    pub fn all_directories(&self) -> [&crate::DirectoryListing; 3] {
        [&self.small_directory, &self.medium_directory, &self.large_directory]
    }
    
    /// Get all manifests as a slice
    pub fn all_manifests(&self) -> [&crate::FileManifest; 3] {
        [&self.small_manifest, &self.medium_manifest, &self.large_manifest]
    }
    
    /// Get all sync plans as a slice
    pub fn all_sync_plans(&self) -> [&crate::SyncPlan; 3] {
        [&self.small_sync_plan, &self.medium_sync_plan, &self.large_sync_plan]
    }
}