//! Server-level common utility types for Kafka.
//!
//! Migrates types from `server-common/src/main/java/org/apache/kafka/server/common/` and related paths.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod api_error;
pub mod feature_versions;
pub mod offset_and_epoch;
pub mod producer_ids_block;
pub mod topic_id_partition;

pub use api_error::ApiError;
pub use feature_versions::{
    GroupVersion, ShareVersion, TransactionVersion, TV_UNKNOWN,
};
pub use offset_and_epoch::OffsetAndEpoch;
pub use producer_ids_block::{ProducerIdsBlock, PRODUCER_ID_BLOCK_SIZE};
pub use topic_id_partition::TopicIdPartition;
