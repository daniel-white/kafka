//! PersisterStateBatch: a batch of state entries for share group persistence.
//!
//! Mirrors Java's `PersisterStateBatch` class.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/share/persister/PersisterStateBatch.java

use getset::CopyGetters;
use std::cmp::Ordering;

/// A batch of delivery state for share group records in a persister.
///
/// Fields are private; access via `getset` generated accessors.
/// Implements `Ord` matching Java's `compareTo` (firstOffset, lastOffset,
/// deliveryCount, deliveryState).
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/share/persister/PersisterStateBatch.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct PersisterStateBatch {
    #[get_copy = "pub"]
    first_offset: i64,
    #[get_copy = "pub"]
    last_offset: i64,
    #[get_copy = "pub"]
    delivery_count: i16,
    #[get_copy = "pub"]
    delivery_state: i8,
}

impl PersisterStateBatch {
    /// Create a new PersisterStateBatch.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/share/persister/PersisterStateBatch.java
    pub const fn new(
        first_offset: i64,
        last_offset: i64,
        delivery_state: i8,
        delivery_count: i16,
    ) -> Self {
        PersisterStateBatch {
            first_offset,
            last_offset,
            delivery_count,
            delivery_state,
        }
    }
}

impl PartialOrd for PersisterStateBatch {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PersisterStateBatch {
    fn cmp(&self, other: &Self) -> Ordering {
        self.first_offset
            .cmp(&other.first_offset)
            .then_with(|| self.last_offset.cmp(&other.last_offset))
            .then_with(|| self.delivery_count.cmp(&other.delivery_count))
            .then_with(|| self.delivery_state.cmp(&other.delivery_state))
    }
}

impl Default for PersisterStateBatch {
    fn default() -> Self {
        PersisterStateBatch::new(0, 0, 0, 0)
    }
}
