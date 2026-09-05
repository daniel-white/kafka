//! Kafka common types: Uuid, TopicPartition, and utility types.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/`.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod node;
pub mod offset_truncation_state;
pub mod partition_info;
pub mod topic_partition;
pub mod uuid;

pub use node::Node;
pub use offset_truncation_state::OffsetTruncationState;
pub use partition_info::PartitionInfo;
pub use topic_partition::TopicPartition;
pub use uuid::Uuid;
