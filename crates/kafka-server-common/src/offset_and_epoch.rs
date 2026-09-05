//! Offset and epoch pair — a position in a log partition.
//!
//! Mirrors Java's `OffsetAndEpoch` record.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/OffsetAndEpoch.java

use getset::CopyGetters;
use std::cmp::Ordering;

/// A position in a log: an offset and its associated epoch.
///
/// Fields are private; access via `getset` generated accessors.
/// Compares by epoch first, then by offset — matching Java's `compareTo`.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/OffsetAndEpoch.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct OffsetAndEpoch {
    #[get_copy = "pub"]
    offset: i64,
    #[get_copy = "pub"]
    epoch: i32,
}

impl OffsetAndEpoch {
    /// Create a new OffsetAndEpoch.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/OffsetAndEpoch.java
    pub const fn new(offset: i64, epoch: i32) -> Self {
        OffsetAndEpoch { offset, epoch }
    }
}

impl PartialOrd for OffsetAndEpoch {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OffsetAndEpoch {
    fn cmp(&self, other: &Self) -> Ordering {
        self.epoch
            .cmp(&other.epoch)
            .then_with(|| self.offset.cmp(&other.offset))
    }
}

impl Default for OffsetAndEpoch {
    fn default() -> Self {
        OffsetAndEpoch::new(0, 0)
    }
}
