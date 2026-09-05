//! Result type for coordinator operations.
//!
//! Mirrors Java's `CoordinatorResult` — wraps records to be committed, an
//! optional response, and flags controlling replay/atomic persistence.
//!
//! Wire format: not serialized directly — records are written to the
//! coordinator's state log and replayed.
//!
//! MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorResult.java

use getset::{CopyGetters, Getters};
use std::fmt;

/// Result of a coordinator operation: contains records to persist and an
/// optional response to return to the caller.
///
/// During Phase A (JVM bytecode), async futures are not used. The `append_future`
/// concept from Java's `CompletableFuture<Void>` is omitted; async completion
/// is handled at a higher layer in Phase B.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorResult.java
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct CoordinatorResult<T, U> {
    #[get = "pub"]
    records: Vec<U>,
    #[get = "pub"]
    response: Option<T>,
    #[get_copy = "pub"]
    replay_records: bool,
    #[get_copy = "pub"]
    is_atomic: bool,
}

impl<T, U> CoordinatorResult<T, U> {
    /// Create a result with records only (no response).
    ///
    /// Defaults: replayRecords=true, isAtomic=true.
    ///
    /// MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorResult.java
    pub fn new(records: Vec<U>) -> Self {
        CoordinatorResult {
            records,
            response: None,
            replay_records: true,
            is_atomic: true,
        }
    }

    /// Create a result with records and a response.
    ///
    /// Defaults: replayRecords=true, isAtomic=true.
    ///
    /// MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorResult.java
    pub fn with_response(records: Vec<U>, response: T) -> Self {
        CoordinatorResult {
            records,
            response: Some(response),
            replay_records: true,
            is_atomic: true,
        }
    }

    /// Create a result with records and atomicity flag.
    ///
    /// MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorResult.java
    pub fn new_atomic(records: Vec<U>, is_atomic: bool) -> Self {
        CoordinatorResult {
            records,
            response: None,
            replay_records: true,
            is_atomic,
        }
    }

    /// Create a builder for fine-grained construction.
    ///
    /// MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorResult.java
    pub fn builder(records: Vec<U>) -> CoordinatorResultBuilder<T, U> {
        CoordinatorResultBuilder {
            records,
            response: None,
            replay_records: true,
            is_atomic: true,
        }
    }
}

impl<T: fmt::Debug, U: fmt::Debug> fmt::Display for CoordinatorResult<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CoordinatorResult(records={:?}, response={:?}, replayRecords={}, isAtomic={})",
            self.records, self.response, self.replay_records, self.is_atomic
        )
    }
}

impl<T: PartialEq, U: PartialEq> PartialEq for CoordinatorResult<T, U> {
    fn eq(&self, other: &Self) -> bool {
        self.records == other.records
            && self.response == other.response
            && self.replay_records == other.replay_records
            && self.is_atomic == other.is_atomic
    }
}

/// Builder for `CoordinatorResult` with fine-grained control.
///
/// Mirrors Java's multiple constructor overloads.
///
/// MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorResult.java
#[derive(Debug, Clone)]
pub struct CoordinatorResultBuilder<T, U> {
    records: Vec<U>,
    response: Option<T>,
    replay_records: bool,
    is_atomic: bool,
}

impl<T, U> CoordinatorResultBuilder<T, U> {
    /// Set whether records should be replayed after commit.
    pub fn replay_records(mut self, replay: bool) -> Self {
        self.replay_records = replay;
        self
    }

    /// Set whether the records must be persisted atomically.
    pub fn atomic(mut self, is_atomic: bool) -> Self {
        self.is_atomic = is_atomic;
        self
    }

    /// Build the final CoordinatorResult.
    pub fn build(self) -> CoordinatorResult<T, U> {
        CoordinatorResult {
            records: self.records,
            response: self.response,
            replay_records: self.replay_records,
            is_atomic: self.is_atomic,
        }
    }
}
