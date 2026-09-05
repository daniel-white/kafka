//! Server-level common utility types for Kafka.
//!
//! Migrates types from `server-common/src/main/java/org/apache/kafka/server/common/` and related paths.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod offset_and_epoch;
pub mod topic_id_partition;

pub use offset_and_epoch::OffsetAndEpoch;
pub use topic_id_partition::TopicIdPartition;
