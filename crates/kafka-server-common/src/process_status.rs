//! ProcessStatus: lifecycle state of the Kafka server process.
//!
//! Mirrors Scala's `ProcessStatus` sealed trait from `core/.../Server.scala`.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/Server.scala:76

/// Lifecycle state of the Kafka server process.
///
/// Mirrors Scala's `ProcessStatus` sealed trait.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/Server.scala:76
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ProcessStatus {
    /// The process is not yet started.
    Shutdown,
    /// The process is starting up.
    Starting,
    /// The process has started.
    Started,
    /// The process is shutting down.
    ShuttingDown,
    /// Unknown state.
    #[default]
    Unknown,
}

impl ProcessStatus {
    /// Check if the process is running (started but not shutting down).
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/Server.scala:76
    pub fn is_running(&self) -> bool {
        *self == ProcessStatus::Started
    }

    /// Check if the process is shutting down or shut down.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/Server.scala:76
    pub fn is_stopped(&self) -> bool {
        *self == ProcessStatus::Shutdown || *self == ProcessStatus::ShuttingDown
    }
}
