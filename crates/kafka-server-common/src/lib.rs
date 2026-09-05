//! Server-level common utility types for Kafka.
//!
//! Migrates types from `server-common/src/main/java/org/apache/kafka/server/common/` and related paths.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod api_error;
pub mod broker_state;
pub mod directory_id;
pub mod feature_versions;
pub mod offset_and_epoch;
pub mod producer_ids_block;
pub mod replicas;
pub mod share_persister;
pub mod state_batch;
pub mod stop_partition;
pub mod topic_id_partition;

pub use api_error::ApiError;
pub use broker_state::BrokerState;
pub use directory_id::DirectoryId;
pub use feature_versions::{
    EligibleLeaderReplicasVersion, GroupVersion, ShareVersion, StreamsVersion,
    TransactionVersion, TV_UNKNOWN,
};
pub use offset_and_epoch::OffsetAndEpoch;
pub use producer_ids_block::{ProducerIdsBlock, PRODUCER_ID_BLOCK_SIZE};
pub use replicas::Replicas;
pub use share_persister::{PartitionData, PartitionDataBuilder, TopicData};
pub use state_batch::PersisterStateBatch;
pub use stop_partition::StopPartition;
pub use topic_id_partition::TopicIdPartition;
