//! Kafka storage layer: log segment indexes, checkpoints, and log state.
//!
//! Mirrors `storage/src/main/java/org/apache/kafka/storage/internals/`.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod cleaning_state;
pub mod errors;
pub mod index;

pub use cleaning_state::LogCleaningState;
