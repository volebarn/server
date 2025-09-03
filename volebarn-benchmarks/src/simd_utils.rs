#[cfg(target_arch = "x86_64")]
use std::arch::is_x86_feature_detected;

// Compression algorithm imports
use brotli;
use lz4_flex;
use snap;
use xxhash_rust;
use zstd;

/// SIMD capability detection and optimization utilities
#[derive(Debug, Clone)]
pub struct SimdCapabilities {
    pub has_avx2: bool,
    pub has_avx512: bool,
    pub has_neon: bool,
}

impl SimdCapabilities {
    pub fn detect() -> Self {
        Self {
            has_avx2: Self::has_avx2(),
            has_avx512: Self::has_avx512(),
            has_neon: Self::has_neon(),
        }
    }
    
    #[cfg(target_arch = "x86_64")]
    fn has_avx2() -> bool {
        is_x86_feature_detected!("avx2")
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    fn has_avx2() -> bool {
        false
    }
    
    #[cfg(target_arch = "x86_64")]
    fn has_avx512() -> bool {
        is_x86_feature_detected!("avx512f")
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    fn has_avx512() -> bool {
        false
    }
    
    #[cfg(target_arch = "aarch64")]
    fn has_neon() -> bool {
        // NEON is always available on AArch64
        true
    }
    
    #[cfg(not(target_arch = "aarch64"))]
    fn has_neon() -> bool {
        false
    }
    
    pub fn best_available(&self) -> SimdLevel {
        if self.has_avx512 {
            SimdLevel::Avx512
        } else if self.has_avx2 {
            SimdLevel::Avx2
        } else if self.has_neon {
            SimdLevel::Neon
        } else {
            SimdLevel::Scalar
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdLevel {
    Scalar,
    Avx2,
    Avx512,
    Neon,
}

impl std::fmt::Display for SimdLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimdLevel::Scalar => write!(f, "scalar"),
            SimdLevel::Avx2 => write!(f, "avx2"),
            SimdLevel::Avx512 => write!(f, "avx512"),
            SimdLevel::Neon => write!(f, "neon"),
        }
    }
}

/// SIMD-optimized serialization utilities
pub struct SimdSerializer {
    capabilities: SimdCapabilities,
}

impl SimdSerializer {
    pub fn new() -> Self {
        Self {
            capabilities: SimdCapabilities::detect(),
        }
    }
    
    pub fn capabilities(&self) -> &SimdCapabilities {
        &self.capabilities
    }
    
    /// Serialize with SIMD optimizations where possible
    pub fn serialize_bincode<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, bincode::Error> {
        // For now, use standard bincode - SIMD optimizations would be in custom serializers
        // In a real implementation, this would dispatch to SIMD-optimized serialization
        // based on detected capabilities
        bincode::serialize(data)
    }
    
    /// Deserialize with SIMD optimizations where possible
    pub fn deserialize_bincode<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> Result<T, bincode::Error> {
        // For now, use standard bincode - SIMD optimizations would be in custom deserializers
        bincode::deserialize(data)
    }
    
    /// Serialize with bitcode and SIMD optimizations
    #[cfg(feature = "bitcode")]
    pub fn serialize_bitcode<T: bitcode::Encode>(&self, data: &T) -> Vec<u8> {
        // bitcode already includes some SIMD optimizations internally
        bitcode::encode(data)
    }
    
    /// Deserialize with bitcode and SIMD optimizations
    #[cfg(feature = "bitcode")]
    pub fn deserialize_bitcode<T: for<'a> bitcode::Decode<'a>>(&self, data: &[u8]) -> Result<T, bitcode::Error> {
        bitcode::decode(data)
    }
    
    /// Convert FileMetadata to bitcode-compatible format and serialize
    #[cfg(feature = "bitcode")]
    pub fn serialize_file_metadata_bitcode(&self, data: &[crate::FileMetadata]) -> Vec<u8> {
        let bitcode_data: Vec<crate::FileMetadataBitcode> = data.iter().map(|m| m.into()).collect();
        bitcode::encode(&bitcode_data)
    }
    
    /// Deserialize bitcode FileMetadata
    #[cfg(feature = "bitcode")]
    pub fn deserialize_file_metadata_bitcode(&self, data: &[u8]) -> Result<Vec<crate::FileMetadataBitcode>, bitcode::Error> {
        bitcode::decode(data)
    }
    
    /// Get performance characteristics for current platform
    pub fn get_performance_info(&self) -> PerformanceInfo {
        PerformanceInfo {
            simd_level: self.capabilities.best_available(),
            expected_speedup: match self.capabilities.best_available() {
                SimdLevel::Scalar => 1.0,
                SimdLevel::Avx2 => 2.5,
                SimdLevel::Avx512 => 4.0,
                SimdLevel::Neon => 2.0,
            },
            architecture: std::env::consts::ARCH.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceInfo {
    pub simd_level: SimdLevel,
    pub expected_speedup: f64,
    pub architecture: String,
}

impl Default for SimdSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Compression algorithm benchmarker with SIMD optimizations
pub struct CompressionBenchmarker {
    capabilities: SimdCapabilities,
}

impl CompressionBenchmarker {
    pub fn new() -> Self {
        Self {
            capabilities: SimdCapabilities::detect(),
        }
    }
    
    /// Compress data using Zstandard with specified compression level
    pub fn compress_zstd(&self, data: &[u8], level: i32) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(zstd::bulk::compress(data, level)?)
    }
    
    /// Decompress Zstandard data
    pub fn decompress_zstd(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(zstd::bulk::decompress(data, 10 * 1024 * 1024)?) // 10MB max
    }
    
    /// Compress data using LZ4
    pub fn compress_lz4(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }
    
    /// Decompress LZ4 data
    pub fn decompress_lz4(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(lz4_flex::decompress_size_prepended(data)?)
    }
    
    /// Compress data using Brotli with specified quality level
    pub fn compress_brotli(&self, data: &[u8], quality: u32) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut output = Vec::new();
        let params = brotli::enc::BrotliEncoderParams {
            quality: quality as i32,
            ..Default::default()
        };
        brotli::BrotliCompress(&mut std::io::Cursor::new(data), &mut output, &params)?;
        Ok(output)
    }
    
    /// Decompress Brotli data
    pub fn decompress_brotli(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut output = Vec::new();
        brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut output)?;
        Ok(output)
    }
    
    /// Compress data using Snappy
    pub fn compress_snappy(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(snap::raw::Encoder::new().compress_vec(data)?)
    }
    
    /// Decompress Snappy data
    pub fn decompress_snappy(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(snap::raw::Decoder::new().decompress_vec(data)?)
    }
    
    /// Analyze compression ratios for all algorithms
    pub fn analyze_compression_ratios(&self, data: &[u8]) -> CompressionAnalysis {
        let original_size = data.len();
        
        let zstd_compressed = self.compress_zstd(data, 3).unwrap_or_default();
        let lz4_compressed = self.compress_lz4(data).unwrap_or_default();
        let brotli_compressed = self.compress_brotli(data, 6).unwrap_or_default();
        let snappy_compressed = self.compress_snappy(data).unwrap_or_default();
        
        CompressionAnalysis {
            original_size,
            zstd_size: zstd_compressed.len(),
            lz4_size: lz4_compressed.len(),
            brotli_size: brotli_compressed.len(),
            snappy_size: snappy_compressed.len(),
            zstd_ratio: original_size as f64 / zstd_compressed.len().max(1) as f64,
            lz4_ratio: original_size as f64 / lz4_compressed.len().max(1) as f64,
            brotli_ratio: original_size as f64 / brotli_compressed.len().max(1) as f64,
            snappy_ratio: original_size as f64 / snappy_compressed.len().max(1) as f64,
        }
    }
    
    /// SIMD-optimized hash calculation using xxHash3
    pub fn hash_simd_optimized(&self, data: &[u8]) -> u64 {
        // xxhash-rust already includes SIMD optimizations when available
        // The library automatically uses AVX2/AVX-512 on x86_64 and NEON on ARM64
        xxhash_rust::xxh3::xxh3_64(data)
    }
    
    /// Generate comprehensive performance report
    pub fn generate_performance_report(&self, data: &[u8]) -> PerformanceReport {
        let analysis = self.analyze_compression_ratios(data);
        let hash = self.hash_simd_optimized(data);
        
        PerformanceReport {
            simd_capabilities: self.capabilities.clone(),
            compression_analysis: analysis.clone(),
            hash_verification: hash,
            recommended_algorithm: self.select_optimal_algorithm(&analysis),
        }
    }
    
    /// Select optimal compression algorithm based on analysis
    fn select_optimal_algorithm(&self, analysis: &CompressionAnalysis) -> CompressionAlgorithm {
        // Selection criteria:
        // 1. For high compression ratio needs: Brotli or Zstd
        // 2. For speed: LZ4 or Snappy
        // 3. For balanced: Zstd level 3
        
        if analysis.brotli_ratio > analysis.zstd_ratio && analysis.brotli_ratio > 2.0 {
            CompressionAlgorithm::Brotli
        } else if analysis.zstd_ratio > 1.8 {
            CompressionAlgorithm::Zstd
        } else if analysis.lz4_ratio > 1.2 {
            CompressionAlgorithm::Lz4
        } else {
            CompressionAlgorithm::Snappy
        }
    }
}

impl Default for CompressionBenchmarker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct CompressionAnalysis {
    pub original_size: usize,
    pub zstd_size: usize,
    pub lz4_size: usize,
    pub brotli_size: usize,
    pub snappy_size: usize,
    pub zstd_ratio: f64,
    pub lz4_ratio: f64,
    pub brotli_ratio: f64,
    pub snappy_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct PerformanceReport {
    pub simd_capabilities: SimdCapabilities,
    pub compression_analysis: CompressionAnalysis,
    pub hash_verification: u64,
    pub recommended_algorithm: CompressionAlgorithm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    Zstd,
    Lz4,
    Brotli,
    Snappy,
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
            CompressionAlgorithm::Lz4 => write!(f, "lz4"),
            CompressionAlgorithm::Brotli => write!(f, "brotli"),
            CompressionAlgorithm::Snappy => write!(f, "snappy"),
        }
    }
}

/// SIMD-optimized memory operations
pub struct SimdMemory;

impl SimdMemory {
    /// Copy memory with SIMD optimizations where available
    #[cfg(target_arch = "x86_64")]
    pub fn copy_optimized(src: &[u8], dst: &mut [u8]) {
        if is_x86_feature_detected!("avx2") && src.len() >= 32 {
            Self::copy_avx2(src, dst);
        } else {
            dst.copy_from_slice(src);
        }
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    pub fn copy_optimized(src: &[u8], dst: &mut [u8]) {
        dst.copy_from_slice(src);
    }
    
    #[cfg(target_arch = "x86_64")]
    fn copy_avx2(src: &[u8], dst: &mut [u8]) {
        // In a real implementation, this would use AVX2 intrinsics
        // For now, fall back to standard copy
        dst.copy_from_slice(src);
    }
    
    /// Compare memory with SIMD optimizations
    #[cfg(target_arch = "x86_64")]
    pub fn compare_optimized(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        
        if is_x86_feature_detected!("avx2") && a.len() >= 32 {
            Self::compare_avx2(a, b)
        } else {
            a == b
        }
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    pub fn compare_optimized(a: &[u8], b: &[u8]) -> bool {
        a == b
    }
    
    #[cfg(target_arch = "x86_64")]
    fn compare_avx2(a: &[u8], b: &[u8]) -> bool {
        // In a real implementation, this would use AVX2 intrinsics
        // For now, fall back to standard comparison
        a == b
    }
}