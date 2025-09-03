use std::time::Instant;
use volebarn_benchmarks::{
    generators::TestFileGenerator,
    simd_utils::{CompressionBenchmarker, SimdCapabilities},
    test_data::ContentType,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Compression Algorithm Performance Benchmarks\n");
    
    // Detect SIMD capabilities
    let capabilities = SimdCapabilities::detect();
    println!("💻 System: {} {}", std::env::consts::ARCH, std::env::consts::OS);
    
    let simd_info = match capabilities.best_available() {
        volebarn_benchmarks::simd_utils::SimdLevel::Avx512 => "AVX-512 (Intel/AMD high-end)",
        volebarn_benchmarks::simd_utils::SimdLevel::Avx2 => "AVX2 (Intel/AMD modern)",
        volebarn_benchmarks::simd_utils::SimdLevel::Neon => "NEON (ARM64)",
        volebarn_benchmarks::simd_utils::SimdLevel::Scalar => "No SIMD acceleration",
    };
    println!("⚡ SIMD: {}\n", simd_info);
    
    // Initialize benchmarker and test data generator
    let benchmarker = CompressionBenchmarker::new();
    let generator = TestFileGenerator::new()?;
    
    // Test different file sizes and content types
    let test_configs = [
        (1024 * 1024, ContentType::Text, "1MB Text"),
        (1024 * 1024, ContentType::Binary, "1MB Binary"),
        (1024 * 1024, ContentType::Code, "1MB Code"),
        (1024 * 1024, ContentType::Json, "1MB JSON"),
        (5 * 1024 * 1024, ContentType::Mixed, "5MB Mixed"),
    ];
    
    println!("📊 Compression Results\n");
    
    let mut overall_results = Vec::new();
    
    for (size, content_type, description) in test_configs {
        let test_file = generator.generate_test_file(size, content_type)?;
        let analysis = benchmarker.analyze_compression_ratios(&test_file.content);
        overall_results.push((description, analysis));
    }
    
    // Show results in a more readable format
    for (description, analysis) in &overall_results {
        println!("📁 {}", description);
        println!("   Original: {}", format_size(analysis.original_size));
        
        let mut algorithms = vec![
            ("Zstd", analysis.zstd_size, analysis.zstd_ratio),
            ("LZ4", analysis.lz4_size, analysis.lz4_ratio),
            ("Brotli", analysis.brotli_size, analysis.brotli_ratio),
            ("Snappy", analysis.snappy_size, analysis.snappy_ratio),
        ];
        
        // Sort by compression ratio (best first)
        algorithms.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());
        
        for (name, size, ratio) in algorithms {
            let savings = if size < analysis.original_size {
                ((analysis.original_size - size) as f64 / analysis.original_size as f64) * 100.0
            } else {
                0.0
            };
            println!("   {} → {} ({:.1}x smaller, {:.0}% saved)", 
                     name, format_size(size), ratio, savings);
        }
        println!();
    }
    
    // Performance timing tests
    println!("⚡ Speed Tests (1MB Mixed Content)\n");
    let test_file = generator.generate_test_file(1024 * 1024, ContentType::Mixed)?;
    
    let mut speed_results = Vec::new();
    
    // Test each algorithm
    speed_results.push(test_compression_speed(&benchmarker, &test_file.content, "Zstd", |data| {
        benchmarker.compress_zstd(data, 3)
    }));
    speed_results.push(test_compression_speed(&benchmarker, &test_file.content, "LZ4", |data| {
        benchmarker.compress_lz4(data)
    }));
    speed_results.push(test_compression_speed(&benchmarker, &test_file.content, "Brotli", |data| {
        benchmarker.compress_brotli(data, 6)
    }));
    speed_results.push(test_compression_speed(&benchmarker, &test_file.content, "Snappy", |data| {
        benchmarker.compress_snappy(data)
    }));
    
    // Sort by speed (fastest first)
    speed_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    
    println!("🏃 Compression Speed Rankings:");
    for (i, (name, speed, ratio, size)) in speed_results.iter().enumerate() {
        let medal = match i {
            0 => "🥇",
            1 => "🥈", 
            2 => "🥉",
            _ => "  ",
        };
        println!("{}  {} → {:.0} MB/s ({:.1}x compression, {})", 
                 medal, name, speed, ratio, format_size(*size));
    }
    
    // Hash verification speed test
    println!("\n🔍 Hash Verification (xxHash3 with SIMD):");
    let iterations = 100;
    let start = Instant::now();
    for _ in 0..iterations {
        let _hash = benchmarker.hash_simd_optimized(&test_file.content);
    }
    let duration = start.elapsed();
    let throughput = (test_file.content.len() as f64 * iterations as f64) / duration.as_secs_f64();
    println!("   {:.0} MB/s (tested with {} iterations)", throughput / (1024.0 * 1024.0), iterations);
    
    println!("\n🎯 What Should You Use?\n");
    
    println!("📱 For mobile/battery devices:");
    println!("   → Snappy (fastest, least CPU usage)");
    
    println!("\n💾 For saving storage space:");
    println!("   → Brotli (best compression ratios)");
    
    println!("\n⚖️  For balanced performance:");
    println!("   → Zstd (good speed + compression)");
    
    println!("\n🚀 For maximum speed:");
    println!("   → LZ4 (fastest overall)");
    
    println!("\n📈 Real-world scenarios:");
    println!("   • Syncing photos/videos → Snappy (already compressed)");
    println!("   • Syncing code files → Brotli (excellent compression)");
    println!("   • Real-time chat/logs → LZ4 (speed matters)");
    println!("   • General file backup → Zstd (best balance)");
    
    Ok(())
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

fn test_compression_speed<F>(
    _benchmarker: &CompressionBenchmarker,
    data: &[u8],
    name: &str,
    compress_fn: F,
) -> (String, f64, f64, usize)
where
    F: Fn(&[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>>,
{
    let iterations = 10;
    let start = Instant::now();
    let mut total_compressed_size = 0;
    
    for _ in 0..iterations {
        match compress_fn(data) {
            Ok(compressed) => {
                total_compressed_size += compressed.len();
            },
            Err(_) => {
                return (name.to_string(), 0.0, 1.0, data.len());
            }
        }
    }
    
    let duration = start.elapsed();
    let throughput = (data.len() as f64 * iterations as f64) / duration.as_secs_f64();
    let avg_compressed_size = total_compressed_size / iterations;
    let ratio = data.len() as f64 / avg_compressed_size as f64;
    let throughput_mb = throughput / (1024.0 * 1024.0);
    
    (name.to_string(), throughput_mb, ratio, avg_compressed_size)
}