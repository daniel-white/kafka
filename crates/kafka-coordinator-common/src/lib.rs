//! Kafka coordinator common types: result wrappers, errors, and utilities.
//!
//! Migrates:
//! - `CoordinatorResult` from coordinator-common/runtime
//! - `UnknownRecordTypeException` / `UnknownRecordVersionException` from Deserializer
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod coordinator_result;
pub mod errors;

pub use coordinator_result::CoordinatorResult;
pub use errors::CoordinatorError;
