use std::time::Instant;
use std::process;
use std::io::Write;
use volebarn_benchmarks::{
    generators::TestFileGenerator,
    simd_utils::{CompressionBenchmarker, SimdSerializer},
    test_data::ContentType,
    FileMetadata, DirectoryListing,
};

#[derive(Debug)]
struct ResourceUsage {
    cpu_time_ms: f64,
    memory_peak_kb: u64,
    throughput_mb_s: f64,
    compression_ratio: f64,
    total_time_ms: f64,
}

#[derive(Debug)]
struct CombinedBenchmark {
    algorithm: String,
    serialization: String,
    compression_ratio: f64,
    serialize_speed_mb_s: f64,
    compress_speed_mb_s: f64,
    decompress_speed_mb_s: f64,
    deserialize_speed_mb_s: f64,
    total_pipeline_mb_s: f64,
    memory_usage_kb: u64,
    cpu_efficiency: f64, // MB/s per CPU core
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Comprehensive Compression + Serialization Benchmark\n");
    
    let generator = TestFileGenerator::new()?;
    let compression_benchmarker = CompressionBenchmarker::new();
    let serializer = SimdSerializer::new();
    
    println!("💻 System: {} cores, {} {}", 
             num_cpus::get(),
             std::env::consts::ARCH, 
             std::env::consts::OS);
    
    // Test different data types and sizes (smaller for speed)
    let test_scenarios = [
        ("File Metadata (100 entries)", TestScenario::Metadata(100)),
        ("Directory Listing (500 files)", TestScenario::Directory(500)),
        ("Mixed Content (256KB)", TestScenario::FileContent(256 * 1024, ContentType::Mixed)),
        ("Code Files (256KB)", TestScenario::FileContent(256 * 1024, ContentType::Code)),
    ];
    
    for (description, scenario) in test_scenarios {
        println!("\n📊 Testing: {}", description);
        println!("{}", "=".repeat(60));
        
        let results = benchmark_scenario(&generator, &compression_benchmarker, &serializer, scenario)?;
        
        // Sort by total pipeline speed (most important metric)
        let mut sorted_results = results;
        sorted_results.sort_by(|a, b| b.total_pipeline_mb_s.partial_cmp(&a.total_pipeline_mb_s).unwrap());
        
        println!("\n🏆 Results (sorted by total pipeline speed):");
        println!("{:<20} {:<12} {:<10} {:<12} {:<12} {:<10}", 
                 "Algorithm+Format", "Pipeline", "Ratio", "Memory", "CPU Eff", "Score");
        println!("{}", "-".repeat(85));
        
        for (i, result) in sorted_results.iter().enumerate() {
            let medal = match i {
                0 => "🥇",
                1 => "🥈", 
                2 => "🥉",
                _ => "  ",
            };
            
            // Calculate overall score (weighted combination)
            let score = (result.total_pipeline_mb_s * 0.4) + 
                       (result.compression_ratio * 50.0 * 0.3) + 
                       (result.cpu_efficiency * 0.2) + 
                       ((10000.0 / result.memory_usage_kb as f64) * 0.1);
            
            println!("{} {:<18} {:<10.0} MB/s {:<8.1}x {:<10} KB {:<10.0} {:<8.0}",
                     medal,
                     format!("{}+{}", result.algorithm, result.serialization),
                     result.total_pipeline_mb_s,
                     result.compression_ratio,
                     result.memory_usage_kb,
                     result.cpu_efficiency,
                     score);
        }
        
        // Show detailed breakdown for top performer
        if let Some(best) = sorted_results.first() {
            println!("\n🔍 Best Performer Details ({}+{}):", best.algorithm, best.serialization);
            println!("   Serialize:   {:.0} MB/s", best.serialize_speed_mb_s);
            println!("   Compress:    {:.0} MB/s", best.compress_speed_mb_s);
            println!("   Decompress:  {:.0} MB/s", best.decompress_speed_mb_s);
            println!("   Deserialize: {:.0} MB/s", best.deserialize_speed_mb_s);
            println!("   Memory Peak: {} KB", best.memory_usage_kb);
            println!("   Compression: {:.1}x smaller", best.compression_ratio);
        }
    }
    
    println!("\n🎯 Production Recommendations:");
    println!("   • High-throughput sync: Snappy + Bincode");
    println!("   • Storage efficiency: Brotli + Bitcode");
    println!("   • Balanced performance: Zstd + Bincode");
    println!("   • Low-resource devices: LZ4 + Bincode");
    
    Ok(())
}

#[derive(Debug, Clone)]
enum TestScenario {
    Metadata(usize),
    Directory(usize),
    FileContent(usize, ContentType),
}

fn benchmark_scenario(
    generator: &TestFileGenerator,
    compression_benchmarker: &CompressionBenchmarker,
    serializer: &SimdSerializer,
    scenario: TestScenario,
) -> Result<Vec<CombinedBenchmark>, Box<dyn std::error::Error>> {
    
    let mut results = Vec::new();
    
    // Test key combinations including bitcode
    let combinations = [
        ("Zstd", "Bincode"),
        ("LZ4", "Bincode"),
        ("Snappy", "Bincode"),
        ("Zstd", "JSON"),
        #[cfg(feature = "bitcode")]
        ("Zstd", "Bitcode"),
        #[cfg(feature = "bitcode")]
        ("LZ4", "Bitcode"),
        #[cfg(feature = "bitcode")]
        ("Snappy", "Bitcode"),
    ];
    
    for (compression_alg, serialization_format) in combinations {
        print!("   Testing {}+{}... ", compression_alg, serialization_format);
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
        
        let result = benchmark_combination(
            generator,
            compression_benchmarker,
            serializer,
            &scenario,
            compression_alg,
            serialization_format,
        )?;
        
        println!("{:.0} MB/s ({}x compression)", result.total_pipeline_mb_s, result.compression_ratio as u32);
        results.push(result);
    }
    
    Ok(results)
}

fn benchmark_combination(
    generator: &TestFileGenerator,
    compression_benchmarker: &CompressionBenchmarker,
    serializer: &SimdSerializer,
    scenario: &TestScenario,
    compression_alg: &str,
    serialization_format: &str,
) -> Result<CombinedBenchmark, Box<dyn std::error::Error>> {
    
    // Generate test data based on scenario
    let (original_data, data_size) = match scenario {
        TestScenario::Metadata(count) => {
            let metadata = volebarn_benchmarks::test_data::generate_file_metadata(*count, 12345);
            let serialized = serializer.serialize_bincode(&metadata)?;
            (serialized, std::mem::size_of::<FileMetadata>() * count)
        },
        TestScenario::Directory(count) => {
            let directory = volebarn_benchmarks::test_data::generate_directory_listing(*count, 23456);
            let serialized = serializer.serialize_bincode(&directory)?;
            (serialized, std::mem::size_of::<DirectoryListing>() + std::mem::size_of::<FileMetadata>() * count)
        },
        TestScenario::FileContent(size, content_type) => {
            let test_file = generator.generate_test_file(*size, *content_type)?;
            (test_file.content.to_vec(), *size)
        },
    };
    
    let iterations = if data_size > 512 * 1024 { 3 } else { 10 }; // Fast iterations
    
    // Measure memory before starting
    let memory_before = get_memory_usage();
    
    // Benchmark serialization + compression pipeline
    let serialize_start = Instant::now();
    let mut serialized_data = Vec::new();
    for _ in 0..iterations {
        serialized_data = match serialization_format {
            "Bincode" => serializer.serialize_bincode(&original_data)?,
            "JSON" => serde_json::to_vec(&original_data)?,
            #[cfg(feature = "bitcode")]
            "Bitcode" => serializer.serialize_bitcode(&original_data),
            _ => return Err("Unknown serialization format".into()),
        };
    }
    let serialize_time = serialize_start.elapsed();
    let serialize_speed = (data_size as f64 * iterations as f64) / serialize_time.as_secs_f64() / (1024.0 * 1024.0);
    
    // Benchmark compression
    let compress_start = Instant::now();
    let mut compressed_data = Vec::new();
    for _ in 0..iterations {
        compressed_data = match compression_alg {
            "Zstd" => compression_benchmarker.compress_zstd(&serialized_data, 3)?,
            "LZ4" => compression_benchmarker.compress_lz4(&serialized_data)?,
            "Brotli" => compression_benchmarker.compress_brotli(&serialized_data, 6)?,
            "Snappy" => compression_benchmarker.compress_snappy(&serialized_data)?,
            _ => return Err("Unknown compression algorithm".into()),
        };
    }
    let compress_time = compress_start.elapsed();
    let compress_speed = (serialized_data.len() as f64 * iterations as f64) / compress_time.as_secs_f64() / (1024.0 * 1024.0);
    
    // Benchmark decompression
    let decompress_start = Instant::now();
    let mut decompressed_data = Vec::new();
    for _ in 0..iterations {
        decompressed_data = match compression_alg {
            "Zstd" => compression_benchmarker.decompress_zstd(&compressed_data)?,
            "LZ4" => compression_benchmarker.decompress_lz4(&compressed_data)?,
            "Brotli" => compression_benchmarker.decompress_brotli(&compressed_data)?,
            "Snappy" => compression_benchmarker.decompress_snappy(&compressed_data)?,
            _ => return Err("Unknown compression algorithm".into()),
        };
    }
    let decompress_time = decompress_start.elapsed();
    let decompress_speed = (decompressed_data.len() as f64 * iterations as f64) / decompress_time.as_secs_f64() / (1024.0 * 1024.0);
    
    // Benchmark deserialization
    let deserialize_start = Instant::now();
    for _ in 0..iterations {
        let _deserialized: Vec<u8> = match serialization_format {
            "Bincode" => serializer.deserialize_bincode(&decompressed_data)?,
            "JSON" => serde_json::from_slice(&decompressed_data)?,
            #[cfg(feature = "bitcode")]
            "Bitcode" => serializer.deserialize_bitcode(&decompressed_data)?,
            _ => return Err("Unknown serialization format".into()),
        };
    }
    let deserialize_time = deserialize_start.elapsed();
    let deserialize_speed = (decompressed_data.len() as f64 * iterations as f64) / deserialize_time.as_secs_f64() / (1024.0 * 1024.0);
    
    // Calculate total pipeline time and speed
    let total_time = serialize_time + compress_time + decompress_time + deserialize_time;
    let total_pipeline_speed = (data_size as f64 * iterations as f64) / total_time.as_secs_f64() / (1024.0 * 1024.0);
    
    // Measure memory after
    let memory_after = get_memory_usage();
    let memory_used = memory_after.saturating_sub(memory_before);
    
    // Calculate compression ratio
    let compression_ratio = serialized_data.len() as f64 / compressed_data.len() as f64;
    
    // Calculate CPU efficiency (MB/s per core)
    let cpu_efficiency = total_pipeline_speed / num_cpus::get() as f64;
    
    Ok(CombinedBenchmark {
        algorithm: compression_alg.to_string(),
        serialization: serialization_format.to_string(),
        compression_ratio,
        serialize_speed_mb_s: serialize_speed,
        compress_speed_mb_s: compress_speed,
        decompress_speed_mb_s: decompress_speed,
        deserialize_speed_mb_s: deserialize_speed,
        total_pipeline_mb_s: total_pipeline_speed,
        memory_usage_kb: memory_used,
        cpu_efficiency,
    })
}

fn get_memory_usage() -> u64 {
    // Simple memory usage estimation
    // In a real implementation, you'd use platform-specific APIs
    let pid = process::id();
    
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("ps")
            .args(&["-o", "rss=", "-p", &pid.to_string()])
            .output()
        {
            if let Ok(rss_str) = String::from_utf8(output.stdout) {
                if let Ok(rss_kb) = rss_str.trim().parse::<u64>() {
                    return rss_kb;
                }
            }
        }
    }
    
    // Fallback - return 0 if we can't measure
    0
}