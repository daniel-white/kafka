//! Producer ID block allocation for idempotent producers.
//!
//! Mirrors Java's `ProducerIdsBlock` class.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ProducerIdsBlock.java

use getset::CopyGetters;
use std::sync::atomic::{AtomicI64, Ordering};

/// Block of producer IDs assigned to a broker.
///
/// Each block is a range of `blockSize` IDs starting at `firstProducerId`.
/// The `producer_id_counter` atomically claims IDs from the block.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ProducerIdsBlock.java
#[derive(Debug, CopyGetters)]
pub struct ProducerIdsBlock {
    #[get_copy = "pub"]
    assigned_broker_id: i32,
    #[get_copy = "pub"]
    first_producer_id: i64,
    #[get_copy = "pub"]
    block_size: i32,
    producer_id_counter: AtomicI64,
}

/// Default block size.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ProducerIdsBlock.java
pub const PRODUCER_ID_BLOCK_SIZE: i32 = 1000;

impl ProducerIdsBlock {
    /// Sentinel: empty block (no producer IDs).
    ///
    /// Mirrors `ProducerIdsBlock.EMPTY`.
    ///
    /// Note: uses `const fn` — `AtomicI64` with `new()` is const-stable.
    /// Clippy warning suppressed because Java uses a `static final` constant
    /// with mutable state via `AtomicLong`.
    #[allow(clippy::declare_interior_mutable_const)]
    #[allow(dead_code)]
    pub const EMPTY: ProducerIdsBlock = ProducerIdsBlock {
        assigned_broker_id: -1,
        first_producer_id: 0,
        block_size: 0,
        producer_id_counter: AtomicI64::new(0),
    };

    /// Create a new block with the given broker ID, first producer ID, and block size.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ProducerIdsBlock.java
    pub fn new(assigned_broker_id: i32, first_producer_id: i64, block_size: i32) -> Self {
        ProducerIdsBlock {
            assigned_broker_id,
            first_producer_id,
            block_size,
            producer_id_counter: AtomicI64::new(first_producer_id),
        }
    }

    /// Last producer ID in this block (inclusive).
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ProducerIdsBlock.java
    pub fn last_producer_id(&self) -> i64 {
        self.first_producer_id + self.block_size as i64 - 1
    }

    /// Claim the next available producer ID from the block.
    /// Returns `None` if all IDs in the block have been claimed.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ProducerIdsBlock.java
    pub fn claim_next_id(&self) -> Option<i64> {
        self.producer_id_counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current > self.last_producer_id() {
                    return None;
                }
                Some(current + 1)
            })
            .ok()
    }
}

impl PartialEq for ProducerIdsBlock {
    fn eq(&self, other: &Self) -> bool {
        self.assigned_broker_id == other.assigned_broker_id
            && self.first_producer_id == other.first_producer_id
            && self.block_size == other.block_size
    }
}

impl Eq for ProducerIdsBlock {}

impl Default for ProducerIdsBlock {
    fn default() -> Self {
        ProducerIdsBlock::EMPTY
    }
}
