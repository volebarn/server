use crate::*;
use bytes::Bytes;
use chrono::Utc;
use fake::{Fake, Faker};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use xxhash_rust::xxh3::xxh3_64;

/// Test file sizes for benchmarking
pub const TEST_FILE_SIZES: &[usize] = &[
    1024 * 1024,      // 1MB
    5 * 1024 * 1024,  // 5MB
    10 * 1024 * 1024, // 10MB
];

/// Content types for test files
#[derive(Debug, Clone, Copy)]
pub enum ContentType {
    Text,
    Binary,
    Mixed,
    Code,
    Json,
    Logs,
}

/// Generate test file content of specified size and type
pub fn generate_file_content(size: usize, content_type: ContentType, seed: u64) -> Bytes {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    
    match content_type {
        ContentType::Text => generate_text_content(size, &mut rng),
        ContentType::Binary => generate_binary_content(size, &mut rng),
        ContentType::Mixed => generate_mixed_content(size, &mut rng),
        ContentType::Code => generate_code_content(size, &mut rng),
        ContentType::Json => generate_json_content(size, &mut rng),
        ContentType::Logs => generate_log_content(size, &mut rng),
    }
}

fn generate_text_content(size: usize, rng: &mut ChaCha8Rng) -> Bytes {
    let words = [
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
        "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",
        "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore",
        "et", "dolore", "magna", "aliqua", "enim", "ad", "minim", "veniam",
    ];
    
    let mut content = String::with_capacity(size);
    while content.len() < size {
        let word = words[rng.gen_range(0..words.len())];
        content.push_str(word);
        if rng.gen_bool(0.15) {
            content.push('\n');
        } else {
            content.push(' ');
        }
    }
    
    content.truncate(size);
    Bytes::from(content)
}

fn generate_binary_content(size: usize, rng: &mut ChaCha8Rng) -> Bytes {
    let mut content = vec![0u8; size];
    rng.fill_bytes(&mut content);
    Bytes::from(content)
}

fn generate_mixed_content(size: usize, rng: &mut ChaCha8Rng) -> Bytes {
    let mut content = Vec::with_capacity(size);
    let mut pos = 0;
    
    while pos < size {
        let chunk_size = rng.gen_range(100..1000).min(size - pos);
        
        if rng.gen_bool(0.6) {
            // Text chunk
            let text = generate_text_content(chunk_size, rng);
            content.extend_from_slice(&text);
        } else {
            // Binary chunk
            let mut binary = vec![0u8; chunk_size];
            rng.fill_bytes(&mut binary);
            content.extend_from_slice(&binary);
        }
        
        pos += chunk_size;
    }
    
    content.truncate(size);
    Bytes::from(content)
}

fn generate_code_content(size: usize, rng: &mut ChaCha8Rng) -> Bytes {
    let code_patterns = [
        "fn main() {\n    println!(\"Hello, world!\");\n}\n\n",
        "use std::collections::HashMap;\n\n",
        "impl MyStruct {\n    pub fn new() -> Self {\n        Self { data: Vec::new() }\n    }\n}\n\n",
        "async fn process_data(input: &[u8]) -> Result<Vec<u8>, Error> {\n    // Processing logic here\n    Ok(input.to_vec())\n}\n\n",
        "#[derive(Debug, Clone, Serialize, Deserialize)]\npub struct Config {\n    pub host: String,\n    pub port: u16,\n}\n\n",
        "// TODO: Implement error handling\n// FIXME: Memory leak in this function\n",
        "const MAX_BUFFER_SIZE: usize = 1024 * 1024;\n\n",
    ];
    
    let mut content = String::with_capacity(size);
    while content.len() < size {
        let pattern = code_patterns[rng.gen_range(0..code_patterns.len())];
        content.push_str(pattern);
    }
    
    content.truncate(size);
    Bytes::from(content)
}

fn generate_json_content(size: usize, rng: &mut ChaCha8Rng) -> Bytes {
    let mut content = String::from("{\n");
    let mut current_size = content.len();
    
    while current_size < size - 10 {
        let key: String = (0..rng.gen_range(5..15))
            .map(|_| rng.gen_range(b'a'..=b'z') as char)
            .collect();
        
        let value = match rng.gen_range(0..4) {
            0 => format!("\"{}\"", Faker.fake::<String>()),
            1 => rng.gen::<i32>().to_string(),
            2 => rng.gen::<bool>().to_string(),
            _ => format!("[{}, {}, {}]", rng.gen::<i32>(), rng.gen::<i32>(), rng.gen::<i32>()),
        };
        
        let line = format!("  \"{}\": {},\n", key, value);
        if current_size + line.len() > size - 10 {
            break;
        }
        
        content.push_str(&line);
        current_size += line.len();
    }
    
    // Remove trailing comma and close JSON
    if content.ends_with(",\n") {
        content.pop();
        content.pop();
        content.push('\n');
    }
    content.push('}');
    
    Bytes::from(content)
}

fn generate_log_content(size: usize, rng: &mut ChaCha8Rng) -> Bytes {
    let log_levels = ["INFO", "WARN", "ERROR", "DEBUG", "TRACE"];
    let components = ["server", "client", "storage", "network", "auth", "sync"];
    let messages = [
        "Request processed successfully",
        "Connection established",
        "File uploaded",
        "Sync operation completed",
        "Error processing request",
        "Timeout occurred",
        "Retrying operation",
        "Cache miss",
        "Database query executed",
        "Memory usage: {}MB",
    ];
    
    let mut content = String::with_capacity(size);
    
    while content.len() < size {
        let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let level = log_levels[rng.gen_range(0..log_levels.len())];
        let component = components[rng.gen_range(0..components.len())];
        let message = messages[rng.gen_range(0..messages.len())];
        
        let log_line = if message.contains("{}") {
            format!("{} [{}] {}: {}\n", timestamp, level, component, 
                   message.replace("{}", &rng.gen_range(10..1000).to_string()))
        } else {
            format!("{} [{}] {}: {}\n", timestamp, level, component, message)
        };
        
        content.push_str(&log_line);
    }
    
    content.truncate(size);
    Bytes::from(content)
}

/// Generate test FileMetadata with realistic data
pub fn generate_file_metadata(count: usize, seed: u64) -> Vec<FileMetadata> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut metadata = Vec::with_capacity(count);
    
    for i in 0..count {
        let path = format!("/test/files/file_{:06}.txt", i);
        let name = format!("file_{:06}.txt", i);
        let size = rng.gen_range(1024..10_000_000);
        let modified = UNIX_EPOCH + std::time::Duration::from_secs(rng.gen_range(1600000000..1700000000));
        let created = modified - std::time::Duration::from_secs(rng.gen_range(0..86400));
        let accessed = modified + std::time::Duration::from_secs(rng.gen_range(0..86400));
        
        // Generate fake content to calculate hash
        let content = generate_file_content(size as usize, ContentType::Text, seed + i as u64);
        let xxhash3 = xxh3_64(&content);
        
        metadata.push(FileMetadata {
            path,
            name,
            size,
            modified,
            is_directory: false,
            xxhash3,
            storage_path: Some(format!("/storage/{:016x}", xxhash3)),
            permissions: 0o644,
            created,
            accessed,
        });
    }
    
    metadata
}

/// Generate test DirectoryListing
pub fn generate_directory_listing(file_count: usize, seed: u64) -> DirectoryListing {
    let files = generate_file_metadata(file_count, seed);
    let total_size = files.iter().map(|f| f.size).sum();
    
    DirectoryListing {
        path: "/test/files".to_string(),
        entries: files,
        total_size,
        file_count,
        directory_count: 0,
    }
}

/// Generate test FileManifest
pub fn generate_file_manifest(file_count: usize, seed: u64) -> FileManifest {
    let files = generate_file_metadata(file_count, seed);
    let mut file_map = HashMap::new();
    
    for file in files {
        file_map.insert(file.path.clone(), file);
    }
    
    let manifest_data = serde_json::to_vec(&file_map).unwrap();
    let checksum = xxh3_64(&manifest_data);
    
    FileManifest {
        files: file_map,
        version: 1,
        created_at: Utc::now(),
        checksum,
    }
}

/// Generate test SyncPlan
pub fn generate_sync_plan(operation_count: usize, seed: u64) -> SyncPlan {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    
    let client_upload: Vec<String> = (0..operation_count / 4)
        .map(|i| format!("/upload/file_{}.txt", i))
        .collect();
    
    let client_download: Vec<String> = (0..operation_count / 4)
        .map(|i| format!("/download/file_{}.txt", i))
        .collect();
    
    let client_delete: Vec<String> = (0..operation_count / 4)
        .map(|i| format!("/delete/file_{}.txt", i))
        .collect();
    
    let client_create_dirs: Vec<String> = (0..operation_count / 8)
        .map(|i| format!("/dirs/new_dir_{}", i))
        .collect();
    
    let conflicts: Vec<SyncConflict> = (0..operation_count / 8)
        .map(|i| {
            let now = SystemTime::now();
            SyncConflict {
                path: format!("/conflicts/file_{}.txt", i),
                local_modified: now - std::time::Duration::from_secs(rng.gen_range(0..86400)),
                remote_modified: now - std::time::Duration::from_secs(rng.gen_range(0..86400)),
                local_size: rng.gen_range(1024..10000),
                remote_size: rng.gen_range(1024..10000),
                local_hash: rng.gen(),
                remote_hash: rng.gen(),
            }
        })
        .collect();
    
    let total_operations = client_upload.len() + client_download.len() + 
                          client_delete.len() + client_create_dirs.len() + conflicts.len();
    
    SyncPlan {
        client_upload,
        client_download,
        client_delete,
        client_create_dirs,
        conflicts,
        total_operations,
        estimated_bytes: rng.gen_range(1_000_000..100_000_000),
    }
}