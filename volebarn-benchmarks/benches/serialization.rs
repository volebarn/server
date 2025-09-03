use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use volebarn_benchmarks::{
    generators::{TestFileGenerator, TestStructures},
    simd_utils::{SimdSerializer, SimdCapabilities},
    test_data::ContentType,
    FileMetadataBitcode,
    *,
};

/// Benchmark serialization formats (bincode vs bitcode) with SIMD optimizations
fn benchmark_serialization_formats(c: &mut Criterion) {
    let generator = TestFileGenerator::new().expect("Failed to create test file generator");
    let test_structures = generator.generate_test_structures();
    let simd_serializer = SimdSerializer::new();
    
    println!("SIMD Capabilities: {:?}", simd_serializer.capabilities());
    println!("Performance Info: {:?}", simd_serializer.get_performance_info());
    
    let mut group = c.benchmark_group("serialization_formats");
    group.measurement_time(Duration::from_secs(5));
    
    // Benchmark FileMetadata serialization
    benchmark_file_metadata_serialization(&mut group, &test_structures, &simd_serializer);
    
    group.finish();
}

fn benchmark_file_metadata_serialization(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    test_structures: &TestStructures,
    simd_serializer: &SimdSerializer,
) {
    let metadata_sets = [
        ("small", &test_structures.small_metadata),
        ("medium", &test_structures.medium_metadata),
        ("large", &test_structures.large_metadata),
    ];
    
    for (size_name, metadata) in metadata_sets {
        let data_size = metadata.len() * std::mem::size_of::<FileMetadata>();
        group.throughput(Throughput::Bytes(data_size as u64));
        
        // Bincode serialization
        group.bench_with_input(
            BenchmarkId::new("bincode_serialize_metadata", size_name),
            metadata,
            |b, metadata| {
                b.iter(|| {
                    let serialized = simd_serializer.serialize_bincode(black_box(metadata)).unwrap();
                    black_box(serialized);
                });
            },
        );
        
        // Bincode deserialization
        let serialized_bincode = simd_serializer.serialize_bincode(metadata).unwrap();
        group.bench_with_input(
            BenchmarkId::new("bincode_deserialize_metadata", size_name),
            &serialized_bincode,
            |b, data| {
                b.iter(|| {
                    let deserialized: Vec<FileMetadata> = simd_serializer
                        .deserialize_bincode(black_box(data))
                        .unwrap();
                    black_box(deserialized);
                });
            },
        );
        
        // Bitcode serialization (if available)
        #[cfg(feature = "bitcode")]
        {
            group.bench_with_input(
                BenchmarkId::new("bitcode_serialize_metadata", size_name),
                metadata,
                |b, metadata| {
                    b.iter(|| {
                        let serialized = simd_serializer.serialize_file_metadata_bitcode(black_box(metadata));
                        black_box(serialized);
                    });
                },
            );
            
            // Bitcode deserialization
            let serialized_bitcode = simd_serializer.serialize_file_metadata_bitcode(metadata);
            group.bench_with_input(
                BenchmarkId::new("bitcode_deserialize_metadata", size_name),
                &serialized_bitcode,
                |b, data| {
                    b.iter(|| {
                        let deserialized: Vec<FileMetadataBitcode> = simd_serializer
                            .deserialize_file_metadata_bitcode(black_box(data))
                            .unwrap();
                        black_box(deserialized);
                    });
                },
            );
        }
        
        // JSON serialization for comparison
        group.bench_with_input(
            BenchmarkId::new("json_serialize_metadata", size_name),
            metadata,
            |b, metadata| {
                b.iter(|| {
                    let serialized = serde_json::to_vec(black_box(metadata)).unwrap();
                    black_box(serialized);
                });
            },
        );
        
        let serialized_json = serde_json::to_vec(metadata).unwrap();
        group.bench_with_input(
            BenchmarkId::new("json_deserialize_metadata", size_name),
            &serialized_json,
            |b, data| {
                b.iter(|| {
                    let deserialized: Vec<FileMetadata> = serde_json::from_slice(black_box(data)).unwrap();
                    black_box(deserialized);
                });
            },
        );
    }
}

/// Benchmark file content processing with different content types
fn benchmark_file_content_processing(c: &mut Criterion) {
    let generator = TestFileGenerator::new().expect("Failed to create test file generator");
    
    let mut group = c.benchmark_group("file_content_processing");
    group.measurement_time(Duration::from_secs(5));
    
    // Test with different file sizes and content types
    let test_sizes = [1024, 10240, 102400]; // 1KB, 10KB, 100KB for faster benchmarks
    let content_types = [ContentType::Text, ContentType::Binary, ContentType::Code];
    
    for &size in &test_sizes {
        for content_type in content_types {
            let test_file = generator.generate_test_file(size, content_type)
                .expect("Failed to generate test file");
            
            group.throughput(Throughput::Bytes(test_file.size as u64));
            
            let benchmark_name = format!("{}_{:?}", test_file.size, test_file.content_type);
            
            // Hash calculation benchmark
            group.bench_with_input(
                BenchmarkId::new("xxhash3_calculation", &benchmark_name),
                &test_file.content,
                |b, content| {
                    b.iter(|| {
                        let hash = xxhash_rust::xxh3::xxh3_64(black_box(content));
                        black_box(hash);
                    });
                },
            );
            
            // Memory copy benchmark
            group.bench_with_input(
                BenchmarkId::new("memory_copy", &benchmark_name),
                &test_file.content,
                |b, content| {
                    b.iter(|| {
                        let copied = content.clone();
                        black_box(copied);
                    });
                },
            );
        }
    }
    
    group.finish();
}

/// Benchmark memory operations with SIMD optimizations
fn benchmark_simd_memory_operations(c: &mut Criterion) {
    let capabilities = SimdCapabilities::detect();
    println!("Testing SIMD capabilities: {:?}", capabilities);
    
    let mut group = c.benchmark_group("simd_memory_operations");
    group.measurement_time(Duration::from_secs(5));
    
    // Test different buffer sizes
    let buffer_sizes = [1024, 4096, 16384]; // Smaller sizes for faster benchmarks
    
    for &size in &buffer_sizes {
        group.throughput(Throughput::Bytes(size as u64));
        
        let src_buffer = vec![0xAA_u8; size];
        let mut dst_buffer = vec![0x00_u8; size];
        
        // Standard memory copy
        group.bench_with_input(
            BenchmarkId::new("standard_copy", size),
            &src_buffer,
            |b, src| {
                b.iter(|| {
                    dst_buffer.copy_from_slice(black_box(src));
                    black_box(&dst_buffer);
                });
            },
        );
        
        // SIMD-optimized memory copy
        group.bench_with_input(
            BenchmarkId::new("simd_copy", size),
            &src_buffer,
            |b, src| {
                b.iter(|| {
                    volebarn_benchmarks::simd_utils::SimdMemory::copy_optimized(
                        black_box(src),
                        black_box(&mut dst_buffer),
                    );
                    black_box(&dst_buffer);
                });
            },
        );
        
        // Memory comparison
        let cmp_buffer = src_buffer.clone();
        group.bench_with_input(
            BenchmarkId::new("standard_compare", size),
            &src_buffer,
            |b, src| {
                b.iter(|| {
                    let result = black_box(src) == black_box(&cmp_buffer);
                    black_box(result);
                });
            },
        );
        
        // SIMD-optimized memory comparison
        group.bench_with_input(
            BenchmarkId::new("simd_compare", size),
            &src_buffer,
            |b, src| {
                b.iter(|| {
                    let result = volebarn_benchmarks::simd_utils::SimdMemory::compare_optimized(
                        black_box(src),
                        black_box(&cmp_buffer),
                    );
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

/// Generate comprehensive benchmark report
fn generate_benchmark_report(c: &mut Criterion) {
    let capabilities = SimdCapabilities::detect();
    let simd_serializer = SimdSerializer::new();
    let perf_info = simd_serializer.get_performance_info();
    
    println!("\n=== Volebarn Serialization Benchmark Report ===");
    println!("Architecture: {}", perf_info.architecture);
    println!("SIMD Level: {}", perf_info.simd_level);
    println!("Expected Speedup: {:.1}x", perf_info.expected_speedup);
    println!("AVX2 Support: {}", capabilities.has_avx2);
    println!("AVX-512 Support: {}", capabilities.has_avx512);
    println!("NEON Support: {}", capabilities.has_neon);
    println!("================================================\n");
    
    // This is a placeholder - actual report generation would happen after benchmarks
    let mut group = c.benchmark_group("benchmark_report");
    group.bench_function("report_generation", |b| {
        b.iter(|| {
            // Simulate report generation
            let _report = format!(
                "Architecture: {}, SIMD: {}, Speedup: {:.1}x",
                perf_info.architecture, perf_info.simd_level, perf_info.expected_speedup
            );
            black_box(_report);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_serialization_formats,
    benchmark_file_content_processing,
    benchmark_simd_memory_operations,
    generate_benchmark_report
);
criterion_main!(benches);