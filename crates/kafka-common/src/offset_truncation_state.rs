//! OffsetTruncationState: tracks the state of an offset truncation operation.
//!
//! Mirrors Java/Scala's `OffsetTruncationState` case class from
//! `core/src/main/scala/kafka/server/AbstractFetcherThread.scala`.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/AbstractFetcherThread.scala:989

use getset::CopyGetters;
use std::fmt;

/// State of an offset truncation operation.
///
/// When `truncation_completed` is true, the log has been truncated up to `offset`.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/AbstractFetcherThread.scala:989
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct OffsetTruncationState {
    #[get_copy = "pub"]
    offset: i64,
    #[get_copy = "pub"]
    truncation_completed: bool,
}

impl OffsetTruncationState {
    /// Create a new OffsetTruncationState with an explicit truncation flag.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/AbstractFetcherThread.scala:989
    pub const fn new(offset: i64, truncation_completed: bool) -> Self {
        OffsetTruncationState {
            offset,
            truncation_completed,
        }
    }

    /// Create an OffsetTruncationState with truncation completed.
    ///
    /// Mirrors Scala's `def this(offset: Long) = this(offset, true)`.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/AbstractFetcherThread.scala:989
    pub const fn with_offset(offset: i64) -> Self {
        OffsetTruncationState::new(offset, true)
    }
}

impl Default for OffsetTruncationState {
    fn default() -> Self {
        OffsetTruncationState::new(0, true)
    }
}

impl fmt::Display for OffsetTruncationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TruncationState(offset={}, completed={})",
            self.offset, self.truncation_completed
        )
    }
}
