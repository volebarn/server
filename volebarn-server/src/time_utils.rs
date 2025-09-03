//! Time utilities for bitcode serialization
//! 
//! SystemTime doesn't implement bitcode's Encode/Decode traits, so we need
//! to convert to/from a serializable format (u64 seconds since UNIX_EPOCH).

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Serializable wrapper for SystemTime that works with bitcode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct SerializableSystemTime(u64);

impl From<SystemTime> for SerializableSystemTime {
    fn from(time: SystemTime) -> Self {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
        Self(duration.as_secs())
    }
}

impl From<SerializableSystemTime> for SystemTime {
    fn from(time: SerializableSystemTime) -> Self {
        UNIX_EPOCH + Duration::from_secs(time.0)
    }
}

impl SerializableSystemTime {
    /// Create from SystemTime
    pub fn new(time: SystemTime) -> Self {
        time.into()
    }
    
    /// Convert to SystemTime
    pub fn to_system_time(self) -> SystemTime {
        self.into()
    }
    
    /// Get the underlying seconds value
    pub fn as_secs(self) -> u64 {
        self.0
    }
    
    /// Create from seconds since UNIX_EPOCH
    pub fn from_secs(secs: u64) -> Self {
        Self(secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_time_conversion() {
        let now = SystemTime::now();
        let serializable = SerializableSystemTime::from(now);
        let converted_back = SystemTime::from(serializable);
        
        // Should be within 1 second (due to truncation to seconds)
        let diff = now.duration_since(converted_back)
            .or_else(|_| converted_back.duration_since(now))
            .unwrap();
        assert!(diff.as_secs() <= 1);
    }

    #[test]
    fn test_unix_epoch() {
        let epoch = UNIX_EPOCH;
        let serializable = SerializableSystemTime::from(epoch);
        let converted_back = SystemTime::from(serializable);
        
        assert_eq!(epoch, converted_back);
        assert_eq!(serializable.as_secs(), 0);
    }

    #[test]
    fn test_specific_time() {
        let specific_time = UNIX_EPOCH + Duration::from_secs(1234567890);
        let serializable = SerializableSystemTime::from(specific_time);
        let converted_back = SystemTime::from(serializable);
        
        assert_eq!(specific_time, converted_back);
        assert_eq!(serializable.as_secs(), 1234567890);
    }

    #[test]
    fn test_from_secs() {
        let secs = 1609459200; // 2021-01-01 00:00:00 UTC
        let serializable = SerializableSystemTime::from_secs(secs);
        let system_time = serializable.to_system_time();
        
        assert_eq!(serializable.as_secs(), secs);
        assert_eq!(system_time, UNIX_EPOCH + Duration::from_secs(secs));
    }
}