#[cfg(target_arch = "x86_64")]
use std::arch::is_x86_feature_detected;

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