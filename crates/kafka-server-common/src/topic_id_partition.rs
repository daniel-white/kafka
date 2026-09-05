//! TopicIdPartition: a pair of topic ID (UUID) and partition index.
//!
//! Mirrors Java's `TopicIdPartition` record.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/TopicIdPartition.java

use getset::{CopyGetters, Getters};
use kafka_common::uuid::Uuid;

/// A topic ID and partition ID pair.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/TopicIdPartition.java
#[derive(Debug, Clone, PartialEq, Eq, CopyGetters, Getters)]
pub struct TopicIdPartition {
    #[get_copy = "pub"]
    topic_id: Uuid,
    #[get = "pub"]
    topic: String,
    #[get_copy = "pub"]
    partition_id: i32,
}

impl TopicIdPartition {
    /// Create a new TopicIdPartition.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/TopicIdPartition.java
    pub fn new(topic_id: Uuid, topic: String, partition_id: i32) -> Self {
        TopicIdPartition {
            topic_id,
            topic,
            partition_id,
        }
    }

    /// Convenience constructor without an explicit topic name.
    /// The topic name defaults to an empty string.
    pub fn new_without_name(topic_id: Uuid, partition_id: i32) -> Self {
        TopicIdPartition {
            topic_id,
            topic: String::new(),
            partition_id,
        }
    }
}

impl std::fmt::Display for TopicIdPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.topic_id, self.partition_id)
    }
}

impl Default for TopicIdPartition {
    fn default() -> Self {
        TopicIdPartition::new(
            Uuid::new(0, 0),
            String::new(),
            -1,
        )
    }
}
