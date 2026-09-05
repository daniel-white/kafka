//! StopPartition: request to stop (and optionally delete) a partition.
//!
//! Mirrors Java's `StopPartition` class.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/StopPartition.java

use crate::TopicIdPartition;
use getset::{CopyGetters, Getters};

/// Request to stop a partition, optionally deleting local and remote logs.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/StopPartition.java
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct StopPartition {
    #[get = "pub"]
    topic_partition: TopicIdPartition,
    #[get_copy = "pub"]
    delete_local_log: bool,
    #[get_copy = "pub"]
    delete_remote_log: bool,
    #[get_copy = "pub"]
    stop_remote_log_metadata_manager: bool,
}

impl StopPartition {
    /// Create a new StopPartition.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/StopPartition.java
    pub fn new(
        topic_partition: TopicIdPartition,
        delete_local_log: bool,
        delete_remote_log: bool,
        stop_remote_log_metadata_manager: bool,
    ) -> Self {
        StopPartition {
            topic_partition,
            delete_local_log,
            delete_remote_log,
            stop_remote_log_metadata_manager,
        }
    }
}

impl PartialEq for StopPartition {
    fn eq(&self, other: &Self) -> bool {
        self.delete_local_log == other.delete_local_log
            && self.delete_remote_log == other.delete_remote_log
            && self.stop_remote_log_metadata_manager == other.stop_remote_log_metadata_manager
            && self.topic_partition == other.topic_partition
    }
}

impl Eq for StopPartition {}

impl std::fmt::Display for StopPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StopPartition(topicPartition={}, deleteLocalLog={}, deleteRemoteLog={}, stopRemoteLogMetadataManager={})",
            self.topic_partition, self.delete_local_log, self.delete_remote_log, self.stop_remote_log_metadata_manager
        )
    }
}
