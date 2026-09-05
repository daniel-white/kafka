//! BrokerState: lifecycle state of a Kafka broker.
//!
//! Mirrors Java's `BrokerState` enum.
//!
//! MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/BrokerState.java

/// Lifecycle state of a Kafka broker.
///
/// Each variant has a byte value used on the wire.
/// Unknown byte values map to `Unknown`.
///
/// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/BrokerState.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BrokerState {
    /// The broker is not running.
    NotRunning,
    /// The broker is catching up with cluster metadata.
    Starting,
    /// The broker has caught up but not yet unfenced by controller.
    Recovery,
    /// The broker has registered and is accepting client requests.
    Running,
    /// The broker is attempting a controlled shutdown.
    PendingControlledShutdown,
    /// The broker is shutting down.
    ShuttingDown,
    /// The broker state is unknown.
    #[default]
    Unknown,
}

impl BrokerState {
    /// Wire byte value for this state.
    ///
    /// Mirrors Java's `value()`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/BrokerState.java
    pub fn value(&self) -> i8 {
        match self {
            BrokerState::NotRunning => 0,
            BrokerState::Starting => 1,
            BrokerState::Recovery => 2,
            BrokerState::Running => 3,
            BrokerState::PendingControlledShutdown => 6,
            BrokerState::ShuttingDown => 7,
            BrokerState::Unknown => 127,
        }
    }

    /// Parse from a wire byte value.
    /// Returns `Unknown` for unrecognized values.
    ///
    /// Mirrors Java's `fromValue()`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/BrokerState.java
    pub fn from_value(value: i8) -> BrokerState {
        match value {
            0 => BrokerState::NotRunning,
            1 => BrokerState::Starting,
            2 => BrokerState::Recovery,
            3 => BrokerState::Running,
            6 => BrokerState::PendingControlledShutdown,
            7 => BrokerState::ShuttingDown,
            127 => BrokerState::Unknown,
            _ => BrokerState::Unknown,
        }
    }
}
