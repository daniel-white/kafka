//! Broker-level state: topic metadata, partition state, and log manager.
//!
//! Mirrors `core/src/main/scala/kafka/server/KafkaServer.scala` and
//! `core/src/main/scala/kafka/server/KafkaApis.scala`.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaServer.scala

use kafka_common::Uuid;
use std::collections::HashMap;

/// Broker-level state: topic metadata and partition state.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaServer.scala
#[derive(Debug, Default)]
pub struct ServerState {
    /// All known topics, keyed by topic name.
    pub topics: HashMap<String, TopicMetadata>,
}

/// Topic metadata including state.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub uuid: Uuid,
    pub partitions: Vec<ServerPartition>,
    pub is_internal: bool,
}

impl Default for TopicMetadata {
    fn default() -> Self {
        TopicMetadata {
            name: String::new(),
            uuid: Uuid::ZERO_UUID,
            partitions: Vec::new(),
            is_internal: false,
        }
    }
}

/// A single partition's state in the broker.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
#[derive(Debug, Clone)]
pub struct ServerPartition {
    pub partition_index: i32,
}

impl ServerState {
    /// Create a new empty server state.
    pub fn new() -> Self {
        ServerState {
            topics: HashMap::new(),
        }
    }

    /// Register a topic with the given name and number of partitions.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala (createTopic)
    pub fn register_topic(&mut self, name: &str, num_partitions: i32) {
        let partitions: Vec<ServerPartition> = (0..num_partitions)
            .map(|i| ServerPartition {
                partition_index: i,
            })
            .collect();

        self.topics.insert(
            name.to_string(),
            TopicMetadata {
                name: name.to_string(),
                uuid: Uuid::random_uuid(),
                partitions,
                is_internal: name.starts_with("__"),
            },
        );
    }
}
