//! Kafka storage layer: log segment indexes and checkpoints.
//!
//! Mirrors `storage/src/main/java/org/apache/kafka/storage/internals/`.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod errors;
pub mod index;
