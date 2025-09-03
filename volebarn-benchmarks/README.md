# Volebarn Serialization Benchmarks

This crate provides comprehensive benchmarks comparing different serialization formats (bincode vs bitcode) with SIMD optimizations for the Volebarn file synchronization system.

## Features

- **Test File Generation**: Creates realistic test files of varying sizes (1MB, 5MB, 10MB) with different content types:
  - Text files with realistic word patterns
  - Binary files with random data
  - Mixed content (text + binary)
  - Code files with Rust-like syntax
  - JSON data structures
  - Log files with timestamps and structured messages

- **Serialization Benchmarks**: Compares performance of:
  - **Bincode**: Fast binary serialization with serde compatibility
  - **Bitcode**: Optimized binary format with better compression
  - **JSON**: Human-readable format for comparison

- **SIMD Optimization Support**: Detects and utilizes available SIMD instructions:
  - **x86_64**: AVX2 and AVX-512 support
  - **ARM64**: NEON support
  - Automatic fallback to scalar operations

- **Data Structures**: Benchmarks realistic file system metadata structures:
  - FileMetadata with timestamps, hashes, and permissions
  - DirectoryListing with nested file entries
  - FileManifest for sync operations
  - SyncPlan with conflict resolution

## Quick Start

### Basic Test
```bash
cargo run --bin quick_test -p volebarn-benchmarks
```

### With Bitcode Support
```bash
cargo run --bin quick_test -p volebarn-benchmarks --features bitcode
```

### Full Benchmarks
```bash
cargo bench --bench serialization -p volebarn-benchmarks
```

### With Bitcode Benchmarks
```bash
cargo bench --bench serialization -p volebarn-benchmarks --features bitcode
```

## Results Summary

Based on initial testing on ARM64 (Apple Silicon):

### Serialization Size Comparison (10 metadata items)
- **JSON**: 3,846 bytes (baseline)
- **Bincode**: 1,498 bytes (2.57x smaller than JSON)
- **Bitcode**: 940 bytes (1.59x smaller than bincode, 4.09x smaller than JSON)

### Performance Characteristics
- **SIMD Support**: ARM NEON detected (2.0x expected speedup)
- **Hash Calculation**: xxHash3 processes 10KB in ~46µs
- **Memory Operations**: SIMD-optimized copy and compare functions

## Architecture Support

### x86_64 Systems
- **AVX-512**: Best performance (4.0x expected speedup)
- **AVX2**: Good performance (2.5x expected speedup)
- **Scalar**: Fallback mode (1.0x baseline)

### ARM64 Systems
- **NEON**: Good performance (2.0x expected speedup)
- **Scalar**: Fallback mode (1.0x baseline)

## Recommendations

### For Production Use
1. **Small Data (< 1KB)**: JSON may be acceptable for simplicity
2. **Medium Data (1KB-100KB)**: Binary formats show clear advantages
3. **Large Data (> 100KB)**: SIMD optimizations provide significant benefits
4. **High-Frequency Operations**: Bitcode with SIMD recommended

### Format Selection
- **Bitcode**: Best compression ratio, good performance
- **Bincode**: Good balance of speed and size, serde compatibility
- **JSON**: Human-readable, debugging-friendly, larger size

## Implementation Details

### SIMD Optimizations
- Automatic CPU feature detection
- Platform-specific optimizations (AVX2/AVX-512/NEON)
- Graceful fallback to scalar operations
- Zero-copy memory operations where possible

### Test Data Generation
- Deterministic seed-based generation for reproducible results
- Realistic content patterns matching real-world usage
- Configurable file sizes and content types
- Hash verification using xxHash3

### Benchmark Structure
- Criterion.rs for statistical analysis
- Throughput measurements in bytes/second
- Memory usage tracking
- HTML report generation

## Files Structure

```
volebarn-benchmarks/
├── src/
│   ├── lib.rs              # Core data structures
│   ├── test_data.rs        # Test file generation
│   ├── generators.rs       # File and structure generators
│   ├── simd_utils.rs       # SIMD optimization utilities
│   └── bin/
│       ├── quick_test.rs   # Quick validation test
│       └── run_benchmarks.rs # Full benchmark runner
├── benches/
│   └── serialization.rs   # Criterion benchmarks
└── README.md              # This file
```

## Dependencies

- **criterion**: Benchmarking framework
- **bincode**: Binary serialization
- **bitcode**: Optimized binary format (optional)
- **serde**: Serialization framework
- **xxhash-rust**: Fast hashing
- **fake**: Test data generation
- **wide**: SIMD utilities

## Usage in Volebarn

These benchmarks inform the serialization strategy for:
- **Storage Layer**: RocksDB metadata serialization
- **Network Layer**: API request/response optimization
- **Client Library**: Efficient data transfer
- **Sync Operations**: Manifest and plan serialization

The results guide the selection of optimal serialization formats based on data size, frequency of operations, and target architecture.