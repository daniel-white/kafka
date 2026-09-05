//! IsolationLevel: read isolation level for consumer fetches.
//!
//! Mirrors Java's `IsolationLevel` enum.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/IsolationLevel.java

use std::fmt;

/// Read isolation level for Kafka consumers.
///
/// Mirrors Java's `IsolationLevel` enum.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/IsolationLevel.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
}

impl IsolationLevel {
    /// Wire byte ID for this isolation level.
    ///
    /// Mirrors Java's `id()`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/IsolationLevel.java
    pub fn id(&self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }

    /// Parse from a wire byte ID.
    /// Panics for unrecognized values (matching Java's behavior).
    ///
    /// Mirrors Java's `forId(byte)`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/IsolationLevel.java
    pub fn for_id(id: i8) -> IsolationLevel {
        match id {
            0 => IsolationLevel::ReadUncommitted,
            1 => IsolationLevel::ReadCommitted,
            _ => panic!("Unknown isolation level {}", id),
        }
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IsolationLevel::ReadUncommitted => write!(f, "read_uncommitted"),
            IsolationLevel::ReadCommitted => write!(f, "read_committed"),
        }
    }
}
