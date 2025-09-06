//! Conflict resolution logic for sync operations
//! 
//! This module provides conflict resolution strategies for handling
//! situations where files have been modified on both local and remote sides.

use volebarn_client::types::ConflictResolutionStrategy;

/// Conflict resolver for handling sync conflicts
#[derive(Debug, Clone)]
pub struct ConflictResolver {
    /// Default strategy to use for conflict resolution
    default_strategy: ConflictResolutionStrategy,
}

impl ConflictResolver {
    /// Create a new conflict resolver with the specified default strategy
    pub fn new(default_strategy: ConflictResolutionStrategy) -> Self {
        Self {
            default_strategy,
        }
    }
    
    /// Get the default conflict resolution strategy
    pub fn default_strategy(&self) -> ConflictResolutionStrategy {
        self.default_strategy.clone()
    }
    
    /// Set the default conflict resolution strategy
    pub fn set_default_strategy(&mut self, strategy: ConflictResolutionStrategy) {
        self.default_strategy = strategy;
    }
}