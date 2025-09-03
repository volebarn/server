use std::process::Command;
use std::env;
use volebarn_benchmarks::simd_utils::{SimdCapabilities, SimdSerializer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Volebarn Serialization Benchmarks ===\n");
    
    // Detect system capabilities
    let capabilities = SimdCapabilities::detect();
    let serializer = SimdSerializer::new();
    let perf_info = serializer.get_performance_info();
    
    println!("System Information:");
    println!("  Architecture: {}", perf_info.architecture);
    println!("  SIMD Level: {}", perf_info.simd_level);
    println!("  Expected Speedup: {:.1}x", perf_info.expected_speedup);
    println!("  AVX2 Support: {}", capabilities.has_avx2);
    println!("  AVX-512 Support: {}", capabilities.has_avx512);
    println!("  NEON Support: {}", capabilities.has_neon);
    println!();
    
    // Check if bitcode feature is enabled
    let bitcode_enabled = cfg!(feature = "bitcode");
    println!("Bitcode Support: {}", if bitcode_enabled { "Enabled" } else { "Disabled" });
    
    if !bitcode_enabled {
        println!("Note: Run with --features bitcode to enable bitcode benchmarks");
    }
    println!();
    
    // Run benchmarks
    println!("Running benchmarks...");
    println!("This may take several minutes depending on your system.");
    println!();
    
    let mut cmd = Command::new("cargo");
    cmd.args(&["bench", "--bench", "serialization"]);
    
    // Add bitcode feature if requested
    if env::args().any(|arg| arg == "--bitcode") {
        cmd.args(&["--features", "bitcode"]);
    }
    
    // Set environment variables for better benchmark output
    cmd.env("CRITERION_DEBUG", "1");
    
    let output = cmd.output()?;
    
    if output.status.success() {
        println!("Benchmarks completed successfully!");
        println!("\nBenchmark output:");
        println!("{}", String::from_utf8_lossy(&output.stdout));
        
        if !output.stderr.is_empty() {
            println!("\nBenchmark warnings/errors:");
            println!("{}", String::from_utf8_lossy(&output.stderr));
        }
        
        println!("\nResults saved to target/criterion/");
        println!("Open target/criterion/report/index.html to view detailed results");
        
        // Generate summary report
        generate_summary_report(&capabilities, bitcode_enabled)?;
        
    } else {
        eprintln!("Benchmark failed!");
        eprintln!("Error output:");
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        std::process::exit(1);
    }
    
    Ok(())
}

fn generate_summary_report(capabilities: &SimdCapabilities, bitcode_enabled: bool) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark Summary Report ===");
    
    // Recommendations based on system capabilities
    println!("\nRecommendations:");
    
    match capabilities.best_available() {
        volebarn_benchmarks::simd_utils::SimdLevel::Avx512 => {
            println!("✓ AVX-512 detected - Excellent performance expected");
            println!("  Recommended: Use SIMD-optimized serialization for large datasets");
        }
        volebarn_benchmarks::simd_utils::SimdLevel::Avx2 => {
            println!("✓ AVX2 detected - Good performance expected");
            println!("  Recommended: Use SIMD-optimized serialization for medium to large datasets");
        }
        volebarn_benchmarks::simd_utils::SimdLevel::Neon => {
            println!("✓ NEON detected - Good ARM performance expected");
            println!("  Recommended: Use SIMD-optimized serialization for ARM deployments");
        }
        volebarn_benchmarks::simd_utils::SimdLevel::Scalar => {
            println!("⚠ No SIMD support detected - Standard performance expected");
            println!("  Recommended: Focus on algorithmic optimizations");
        }
    }
    
    if bitcode_enabled {
        println!("✓ Bitcode benchmarks included in comparison");
    } else {
        println!("⚠ Bitcode benchmarks not run - use --bitcode flag for complete comparison");
    }
    
    println!("\nNext Steps:");
    println!("1. Review detailed results in target/criterion/report/index.html");
    println!("2. Compare bincode vs bitcode performance for your use case");
    println!("3. Consider SIMD optimizations for production deployment");
    println!("4. Test with your specific data patterns and sizes");
    
    // Expected performance characteristics
    println!("\nExpected Performance Characteristics:");
    println!("- Small data (< 1KB): JSON may be competitive due to simplicity");
    println!("- Medium data (1KB-100KB): Binary formats should show clear advantage");
    println!("- Large data (> 100KB): SIMD optimizations should provide significant speedup");
    println!("- High-frequency operations: Binary formats with SIMD are recommended");
    
    Ok(())
}