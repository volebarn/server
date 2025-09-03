use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use volebarn_benchmarks::{
    generators::{TestFileGenerator, TestStructures},
    simd_utils::{SimdCapabilities, CompressionBenchmarker},
    test_data::ContentType,
};

/// Benchmark compression algorithms with SIMD optimizations
fn benchmark_compression_algorithms(c: &mut Criterion) {
    let generator = TestFileGenerator::new().expect("Failed to create test file generator");
    let benchmarker = CompressionBenchmarker::new();
    let capabilities = SimdCapabilities::detect();
    
    println!("SIMD Capabilities: {:?}", capabilities);
    println!("Testing compression algorithms with SIMD optimizations");
    
    let mut group = c.benchmark_group("compression_algorithms");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);
    
    // Test different file sizes and content types
    let test_sizes = [1024 * 1024, 5 * 1024 * 1024]; // 1MB, 5MB for reasonable benchmark times
    let content_types = [ContentType::Text, ContentType::Binary, ContentType::Code, ContentType::Json];
    
    for &size in &test_sizes {
        for content_type in content_types {
            let test_file = generator.generate_test_file(size, content_type)
                .expect("Failed to generate test file");
            
            group.throughput(Throughput::Bytes(test_file.size as u64));
            
            let benchmark_name = format!("{}MB_{:?}", size / (1024 * 1024), content_type);
            
            // Benchmark each compression algorithm
            benchmark_zstd_compression(&mut group, &benchmark_name, &test_file.content, &benchmarker);
            benchmark_lz4_compression(&mut group, &benchmark_name, &test_file.content, &benchmarker);
            benchmark_brotli_compression(&mut group, &benchmark_name, &test_file.content, &benchmarker);
            benchmark_snappy_compression(&mut group, &benchmark_name, &test_file.content, &benchmarker);
        }
    }
    
    group.finish();
}

fn benchmark_zstd_compression(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    data: &[u8],
    benchmarker: &CompressionBenchmarker,
) {
    // Compression
    group.bench_with_input(
        BenchmarkId::new("zstd_compress", name),
        data,
        |b, data| {
            b.iter(|| {
                let compressed = benchmarker.compress_zstd(black_box(data), 3).unwrap();
                black_box(compressed);
            });
        },
    );
    
    // Decompression
    let compressed = benchmarker.compress_zstd(data, 3).unwrap();
    group.bench_with_input(
        BenchmarkId::new("zstd_decompress", name),
        &compressed,
        |b, compressed| {
            b.iter(|| {
                let decompressed = benchmarker.decompress_zstd(black_box(compressed)).unwrap();
                black_box(decompressed);
            });
        },
    );
}

fn benchmark_lz4_compression(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    data: &[u8],
    benchmarker: &CompressionBenchmarker,
) {
    // Compression
    group.bench_with_input(
        BenchmarkId::new("lz4_compress", name),
        data,
        |b, data| {
            b.iter(|| {
                let compressed = benchmarker.compress_lz4(black_box(data)).unwrap();
                black_box(compressed);
            });
        },
    );
    
    // Decompression
    let compressed = benchmarker.compress_lz4(data).unwrap();
    group.bench_with_input(
        BenchmarkId::new("lz4_decompress", name),
        &compressed,
        |b, compressed| {
            b.iter(|| {
                let decompressed = benchmarker.decompress_lz4(black_box(compressed)).unwrap();
                black_box(decompressed);
            });
        },
    );
}

fn benchmark_brotli_compression(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    data: &[u8],
    benchmarker: &CompressionBenchmarker,
) {
    // Compression
    group.bench_with_input(
        BenchmarkId::new("brotli_compress", name),
        data,
        |b, data| {
            b.iter(|| {
                let compressed = benchmarker.compress_brotli(black_box(data), 6).unwrap();
                black_box(compressed);
            });
        },
    );
    
    // Decompression
    let compressed = benchmarker.compress_brotli(data, 6).unwrap();
    group.bench_with_input(
        BenchmarkId::new("brotli_decompress", name),
        &compressed,
        |b, compressed| {
            b.iter(|| {
                let decompressed = benchmarker.decompress_brotli(black_box(compressed)).unwrap();
                black_box(decompressed);
            });
        },
    );
}

fn benchmark_snappy_compression(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    data: &[u8],
    benchmarker: &CompressionBenchmarker,
) {
    // Compression
    group.bench_with_input(
        BenchmarkId::new("snappy_compress", name),
        data,
        |b, data| {
            b.iter(|| {
                let compressed = benchmarker.compress_snappy(black_box(data)).unwrap();
                black_box(compressed);
            });
        },
    );
    
    // Decompression
    let compressed = benchmarker.compress_snappy(data).unwrap();
    group.bench_with_input(
        BenchmarkId::new("snappy_decompress", name),
        &compressed,
        |b, compressed| {
            b.iter(|| {
                let decompressed = benchmarker.decompress_snappy(black_box(compressed)).unwrap();
                black_box(decompressed);
            });
        },
    );
}

/// Benchmark compression ratios and efficiency
fn benchmark_compression_ratios(c: &mut Criterion) {
    let generator = TestFileGenerator::new().expect("Failed to create test file generator");
    let benchmarker = CompressionBenchmarker::new();
    
    let mut group = c.benchmark_group("compression_ratios");
    group.measurement_time(Duration::from_secs(5));
    
    // Generate test data for ratio analysis
    let test_sizes = [1024 * 1024]; // 1MB for ratio testing
    let content_types = [ContentType::Text, ContentType::Binary, ContentType::Code, ContentType::Json];
    
    for &size in &test_sizes {
        for content_type in content_types {
            let test_file = generator.generate_test_file(size, content_type)
                .expect("Failed to generate test file");
            
            let benchmark_name = format!("{}MB_{:?}", size / (1024 * 1024), content_type);
            
            // Measure compression ratios
            group.bench_with_input(
                BenchmarkId::new("compression_ratio_analysis", &benchmark_name),
                &test_file.content,
                |b, data| {
                    b.iter(|| {
                        let results = benchmarker.analyze_compression_ratios(black_box(data));
                        black_box(results);
                    });
                },
            );
        }
    }
    
    group.finish();
}

/// Benchmark SIMD-accelerated hash verification
fn benchmark_simd_hash_verification(c: &mut Criterion) {
    let generator = TestFileGenerator::new().expect("Failed to create test file generator");
    let benchmarker = CompressionBenchmarker::new();
    let capabilities = SimdCapabilities::detect();
    
    println!("Testing SIMD hash verification - Capabilities: {:?}", capabilities);
    
    let mut group = c.benchmark_group("simd_hash_verification");
    group.measurement_time(Duration::from_secs(5));
    
    let test_sizes = [1024, 10240, 102400, 1024 * 1024]; // Various sizes
    
    for &size in &test_sizes {
        let test_file = generator.generate_test_file(size, ContentType::Binary)
            .expect("Failed to generate test file");
        
        group.throughput(Throughput::Bytes(test_file.size as u64));
        
        // Standard xxHash3
        group.bench_with_input(
            BenchmarkId::new("xxhash3_standard", size),
            &test_file.content,
            |b, data| {
                b.iter(|| {
                    let hash = xxhash_rust::xxh3::xxh3_64(black_box(data));
                    black_box(hash);
                });
            },
        );
        
        // SIMD-optimized xxHash3 (if available)
        group.bench_with_input(
            BenchmarkId::new("xxhash3_simd", size),
            &test_file.content,
            |b, data| {
                b.iter(|| {
                    let hash = benchmarker.hash_simd_optimized(black_box(data));
                    black_box(hash);
                });
            },
        );
        
        // Hash verification with compression
        let compressed_zstd = benchmarker.compress_zstd(&test_file.content, 3).unwrap();
        group.bench_with_input(
            BenchmarkId::new("hash_verify_compressed", size),
            &compressed_zstd,
            |b, compressed| {
                b.iter(|| {
                    let decompressed = benchmarker.decompress_zstd(black_box(compressed)).unwrap();
                    let hash = benchmarker.hash_simd_optimized(&decompressed);
                    black_box(hash);
                });
            },
        );
    }
    
    group.finish();
}

/// Generate comprehensive performance report
fn generate_compression_performance_report(c: &mut Criterion) {
    let generator = TestFileGenerator::new().expect("Failed to create test file generator");
    let benchmarker = CompressionBenchmarker::new();
    let capabilities = SimdCapabilities::detect();
    
    println!("\n=== Volebarn Compression Benchmark Report ===");
    println!("Architecture: {}", std::env::consts::ARCH);
    println!("SIMD Capabilities: {:?}", capabilities);
    println!("Expected Performance Improvements:");
    println!("  - AVX2: ~2.5x speedup for hash operations");
    println!("  - AVX-512: ~4.0x speedup for hash operations");
    println!("  - NEON: ~2.0x speedup for hash operations");
    println!("===============================================\n");
    
    let mut group = c.benchmark_group("performance_report");
    group.measurement_time(Duration::from_secs(3));
    
    // Generate sample data for report
    let test_file = generator.generate_test_file(1024 * 1024, ContentType::Mixed)
        .expect("Failed to generate test file");
    
    group.bench_function("generate_performance_report", |b| {
        b.iter(|| {
            let report = benchmarker.generate_performance_report(black_box(&test_file.content));
            black_box(report);
        });
    });
    
    group.finish();
}

criterion_group!(
    compression_benches,
    benchmark_compression_algorithms,
    benchmark_compression_ratios,
    benchmark_simd_hash_verification,
    generate_compression_performance_report
);
criterion_main!(compression_benches);