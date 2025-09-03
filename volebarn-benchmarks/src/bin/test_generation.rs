use volebarn_benchmarks::{
    generators::TestFileGenerator,
    simd_utils::SimdSerializer,
    test_data::{ContentType, TEST_FILE_SIZES},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Test File Generation and Serialization Test ===\n");
    
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
    
    // Test file generation
    println!("Testing file generation...");
    let generator = TestFileGenerator::new()?;
    
    for &size in TEST_FILE_SIZES {
        for content_type in [ContentType::Text, ContentType::Binary, ContentType::Code] {
            let test_file = generator.generate_test_file(size, content_type)?;
            println!("  Generated {} {:?} file: {} bytes", 
                    size, content_type, test_file.content.len());
            
            // Verify content size matches request
            assert_eq!(test_file.content.len(), size);
            assert_eq!(test_file.size, size);
        }
    }
    
    // Test data structure generation
    println!("\nTesting data structure generation...");
    let test_structures = generator.generate_test_structures();
    
    println!("  Small metadata: {} items", test_structures.small_metadata.len());
    println!("  Medium metadata: {} items", test_structures.medium_metadata.len());
    println!("  Large metadata: {} items", test_structures.large_metadata.len());
    
    println!("  Small directory: {} entries", test_structures.small_directory.entries.len());
    println!("  Medium directory: {} entries", test_structures.medium_directory.entries.len());
    println!("  Large directory: {} entries", test_structures.large_directory.entries.len());
    
    println!("  Small manifest: {} files", test_structures.small_manifest.files.len());
    println!("  Medium manifest: {} files", test_structures.medium_manifest.files.len());
    println!("  Large manifest: {} files", test_structures.large_manifest.files.len());
    
    // Test serialization
    println!("\nTesting serialization...");
    
    // Test bincode serialization
    let metadata = &test_structures.small_metadata;
    let bincode_data = serializer.serialize_bincode(metadata)?;
    println!("  Bincode serialized {} metadata items to {} bytes", 
            metadata.len(), bincode_data.len());
    
    let deserialized: Vec<volebarn_benchmarks::FileMetadata> = 
        serializer.deserialize_bincode(&bincode_data)?;
    assert_eq!(deserialized.len(), metadata.len());
    println!("  Bincode deserialization successful");
    
    // Test bitcode serialization if available
    #[cfg(feature = "bitcode")]
    {
        let bitcode_data = serializer.serialize_file_metadata_bitcode(metadata);
        println!("  Bitcode serialized {} metadata items to {} bytes", 
                metadata.len(), bitcode_data.len());
        
        let deserialized: Vec<volebarn_benchmarks::FileMetadataBitcode> = 
            serializer.deserialize_file_metadata_bitcode(&bitcode_data)?;
        assert_eq!(deserialized.len(), metadata.len());
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
    let json_data = serde_json::to_vec(metadata)?;
    println!("  JSON serialized {} metadata items to {} bytes", 
            metadata.len(), json_data.len());
    
    let json_deserialized: Vec<volebarn_benchmarks::FileMetadata> = 
        serde_json::from_slice(&json_data)?;
    assert_eq!(json_deserialized.len(), metadata.len());
    println!("  JSON deserialization successful");
    
    // Size comparison
    println!("\nSerialization Size Comparison:");
    println!("  Bincode: {} bytes", bincode_data.len());
    println!("  JSON: {} bytes", json_data.len());
    let json_ratio = json_data.len() as f64 / bincode_data.len() as f64;
    println!("  JSON/Bincode ratio: {:.2}x", json_ratio);
    
    #[cfg(feature = "bitcode")]
    {
        let bitcode_data = serializer.serialize_file_metadata_bitcode(metadata);
        println!("  Bitcode: {} bytes", bitcode_data.len());
        let bitcode_ratio = bitcode_data.len() as f64 / bincode_data.len() as f64;
        println!("  Bitcode/Bincode ratio: {:.2}x", bitcode_ratio);
    }
    
    println!("\n✓ All tests passed successfully!");
    println!("\nTo run full benchmarks:");
    println!("  cargo run --bin run_benchmarks");
    println!("  cargo run --bin run_benchmarks --bitcode  # Include bitcode tests");
    
    Ok(())
}