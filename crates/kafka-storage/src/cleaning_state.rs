//! Log cleaning state: tracks the state of an in-progress log cleaner.
//!
//! Mirrors Java's `LogCleaningState` sealed interface.
//!
//! MIGRATION_SOURCE: storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaningState.java

/// State of a log cleaner for a partition.
///
/// This is a Rust enum replacing Java's sealed interface with three
/// final classes. The `Paused` variant carries the pause count.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaningState.java
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LogCleaningState {
    /// Cleaning is in progress.
    #[default]
    InProgress,
    /// Cleaning was aborted.
    Aborted,
    /// Cleaning is paused with a count of pause requests.
    Paused { paused_count: i32 },
}

/// Accessor for `paused_count` from the `Paused` variant.
/// Returns `None` if not `Paused`.
///
/// MIGRATION_SOURCE: storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaningState.java
impl LogCleaningState {
    /// Get the paused count if this state is `Paused`, else 0.
    pub fn paused_count(&self) -> i32 {
        match self {
            LogCleaningState::Paused { paused_count } => *paused_count,
            _ => 0,
        }
    }

    /// Create an `InProgress` state.
    ///
    /// Mirrors Java's `LogCleaningInProgress`.
    pub const fn in_progress() -> Self {
        LogCleaningState::InProgress
    }

    /// Create an `Aborted` state.
    ///
    /// Mirrors Java's `LogCleaningAborted`.
    pub const fn aborted() -> Self {
        LogCleaningState::Aborted
    }

    /// Create a `Paused` state with the given pause count.
    ///
    /// Mirrors Java's `logCleaningPaused(int)`.
    pub fn paused(paused_count: i32) -> Self {
        LogCleaningState::Paused { paused_count }
    }
}
