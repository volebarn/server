use volebarn_benchmarks::{
    generators::TestFileGenerator,
    simd_utils::SimdSerializer,
    test_data::{ContentType, generate_file_metadata},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Quick Serialization Test ===\n");
    
    // Test SIMD capabilities
    let serializer = SimdSerializer::new();
    let capabilities = serializer.capabilities();
    let perf_info = serializer.get_performance_info();
    
    println!("System Capabilities:");
    println!("  Architecture: {}", perf_info.architecture);
    println!("  SIMD Level: {}", perf_info.simd_level);
    println!("  Expected Speedup: {:.1}x", perf_info.expected_speedup);
    println!("  AVX2: {}", capabilities.has_avx2);
    println!("  AVX-512: {}", capabilities.has_avx512);
    println!("  NEON: {}", capabilities.has_neon);
    println!();
    
    // Test file generation with smaller sizes
    println!("Testing file generation...");
    let generator = TestFileGenerator::new()?;
    
    let test_sizes = [1024, 10240]; // 1KB, 10KB for quick test
    for &size in &test_sizes {
        for content_type in [ContentType::Text, ContentType::Binary] {
            let test_file = generator.generate_test_file(size, content_type)?;
            println!("  Generated {} {:?} file: {} bytes", 
                    size, content_type, test_file.content.len());
            
            // Verify content size matches request
            assert_eq!(test_file.content.len(), size);
            assert_eq!(test_file.size, size);
        }
    }
    
    // Test data structure generation with smaller datasets
    println!("\nTesting serialization with small datasets...");
    let small_metadata = generate_file_metadata(10, 12345); // Only 10 items for quick test
    
    println!("  Generated {} metadata items", small_metadata.len());
    
    // Test bincode serialization
    let bincode_data = serializer.serialize_bincode(&small_metadata)?;
    println!("  Bincode serialized to {} bytes", bincode_data.len());
    
    let deserialized: Vec<volebarn_benchmarks::FileMetadata> = 
        serializer.deserialize_bincode(&bincode_data)?;
    assert_eq!(deserialized.len(), small_metadata.len());
    println!("  Bincode deserialization successful");
    
    // Test bitcode serialization if available
    #[cfg(feature = "bitcode")]
    {
        let bitcode_data = serializer.serialize_file_metadata_bitcode(&small_metadata);
        println!("  Bitcode serialized to {} bytes", bitcode_data.len());
        
        let deserialized: Vec<volebarn_benchmarks::FileMetadataBitcode> = 
            serializer.deserialize_file_metadata_bitcode(&bitcode_data)?;
        assert_eq!(deserialized.len(), small_metadata.len());
        println!("  Bitcode deserialization successful");
        
        // Compare sizes
        let size_ratio = bitcode_data.len() as f64 / bincode_data.len() as f64;
        println!("  Bitcode/Bincode size ratio: {:.2}", size_ratio);
    }
    
    #[cfg(not(feature = "bitcode"))]
    {
        println!("  Bitcode not available (compile with --features bitcode)");
    }
    
    // Test JSON serialization for comparison
    let json_data = serde_json::to_vec(&small_metadata)?;
    println!("  JSON serialized to {} bytes", json_data.len());
    
    let json_deserialized: Vec<volebarn_benchmarks::FileMetadata> = 
        serde_json::from_slice(&json_data)?;
    assert_eq!(json_deserialized.len(), small_metadata.len());
    println!("  JSON deserialization successful");
    
    // Size comparison
    println!("\nSerialization Size Comparison (10 metadata items):");
    println!("  Bincode: {} bytes", bincode_data.len());
    println!("  JSON: {} bytes", json_data.len());
    let json_ratio = json_data.len() as f64 / bincode_data.len() as f64;
    println!("  JSON/Bincode ratio: {:.2}x", json_ratio);
    
    #[cfg(feature = "bitcode")]
    {
        let bitcode_data = serializer.serialize_file_metadata_bitcode(&small_metadata);
        println!("  Bitcode: {} bytes", bitcode_data.len());
        let bitcode_ratio = bitcode_data.len() as f64 / bincode_data.len() as f64;
        println!("  Bitcode/Bincode ratio: {:.2}x", bitcode_ratio);
    }
    
    // Test hash calculation performance
    println!("\nTesting hash calculation...");
    let test_data = generator.generate_test_file(10240, ContentType::Text)?;
    
    let start = std::time::Instant::now();
    let hash = xxhash_rust::xxh3::xxh3_64(&test_data.content);
    let duration = start.elapsed();
    
    println!("  xxHash3 of 10KB: {:016x} (took {:?})", hash, duration);
    
    println!("\n✓ All tests passed successfully!");
    
    // Performance recommendations
    println!("\nPerformance Recommendations:");
    match perf_info.simd_level {
        volebarn_benchmarks::simd_utils::SimdLevel::Avx512 => {
            println!("  ✓ Excellent SIMD support - Use optimized serialization for large datasets");
        }
        volebarn_benchmarks::simd_utils::SimdLevel::Avx2 => {
            println!("  ✓ Good SIMD support - Recommended for medium to large datasets");
        }
        volebarn_benchmarks::simd_utils::SimdLevel::Neon => {
            println!("  ✓ ARM NEON support - Good performance on ARM platforms");
        }
        volebarn_benchmarks::simd_utils::SimdLevel::Scalar => {
            println!("  ⚠ No SIMD support - Focus on algorithmic optimizations");
        }
    }
    
    if json_ratio > 2.0 {
        println!("  ✓ Binary formats show significant space savings over JSON");
    }
    
    #[cfg(feature = "bitcode")]
    {
        let bitcode_data = serializer.serialize_file_metadata_bitcode(&small_metadata);
        let bitcode_ratio = bitcode_data.len() as f64 / bincode_data.len() as f64;
        if bitcode_ratio < 0.9 {
            println!("  ✓ Bitcode shows space savings over bincode");
        } else if bitcode_ratio > 1.1 {
            println!("  ⚠ Bincode is more compact than bitcode for this data");
        } else {
            println!("  = Bitcode and bincode have similar sizes");
        }
    }
    
    println!("\nTo run full benchmarks:");
    println!("  cargo run --bin run_benchmarks");
    println!("  cargo bench --bench serialization");
    
    Ok(())
}