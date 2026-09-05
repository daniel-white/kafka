//! Consumer group states for Kafka.
//!
//! Mirrors Java's `ConsumerGroupState` and `GroupState` enums.
//!
//! MIGRATION_SOURCE:
//! - clients/src/main/java/org/apache/kafka/common/ConsumerGroupState.java
//! - clients/src/main/java/org/apache/kafka/common/GroupState.java

use std::fmt;
use std::str::FromStr;

/// Consumer group state.
///
/// Mirrors Java's `ConsumerGroupState` enum (9 variants).
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/ConsumerGroupState.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ConsumerGroupState {
    #[default]
    Unknown,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
    Empty,
    Assigning,
    Reconciling,
    NotReady,
}

impl ConsumerGroupState {
    /// Get the string name (e.g. "Unknown", "Stable").
    ///
    /// Mirrors Java's `toString()`.
    ///
    /// MIGRATION_SOURCE: clients/.../ConsumerGroupState.java
    pub fn name(&self) -> &'static str {
        match self {
            ConsumerGroupState::Unknown => "Unknown",
            ConsumerGroupState::PreparingRebalance => "PreparingRebalance",
            ConsumerGroupState::CompletingRebalance => "CompletingRebalance",
            ConsumerGroupState::Stable => "Stable",
            ConsumerGroupState::Dead => "Dead",
            ConsumerGroupState::Empty => "Empty",
            ConsumerGroupState::Assigning => "Assigning",
            ConsumerGroupState::Reconciling => "Reconciling",
            ConsumerGroupState::NotReady => "NotReady",
        }
    }
}

impl FromStr for ConsumerGroupState {
    type Err = ();

    /// Parse case-insensitively, returning Unknown for unrecognized names.
    ///
    /// Mirrors Java's `ConsumerGroupState.parse(String)`.
    ///
    /// MIGRATION_SOURCE: clients/.../ConsumerGroupState.java
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "UNKNOWN" => Ok(ConsumerGroupState::Unknown),
            "PREPARINGREBALANCE" => Ok(ConsumerGroupState::PreparingRebalance),
            "COMPLETINGREBALANCE" => Ok(ConsumerGroupState::CompletingRebalance),
            "STABLE" => Ok(ConsumerGroupState::Stable),
            "DEAD" => Ok(ConsumerGroupState::Dead),
            "EMPTY" => Ok(ConsumerGroupState::Empty),
            "ASSIGNING" => Ok(ConsumerGroupState::Assigning),
            "RECONCILING" => Ok(ConsumerGroupState::Reconciling),
            "NOTREADY" => Ok(ConsumerGroupState::NotReady),
            _ => Ok(ConsumerGroupState::Unknown),
        }
    }
}

impl fmt::Display for ConsumerGroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
