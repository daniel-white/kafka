//! Broker-level state: topic metadata, partition state, and log manager.
//!
//! Mirrors `core/src/main/scala/kafka/server/KafkaServer.scala` and
//! `core/src/main/scala/kafka/server/KafkaApis.scala`.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaServer.scala

use kafka_common::Uuid;
use kafka_protocol::api_key::ApiKey;
use std::collections::HashMap;

/// Registry of supported APIs and their version ranges.
///
/// This replaces the hardcoded API version list in `handle_api_versions`.
/// The registry is populated at startup with the APIs the broker actually
/// supports, and `ApiVersionsResponse` is generated from it.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
#[derive(Debug, Clone, Default)]
pub struct ApiRegistry {
    /// (api_key, min_version, max_version) for each supported API.
    entries: Vec<(i16, i16, i16)>,
}

impl ApiRegistry {
    pub fn new() -> Self {
        ApiRegistry {
            entries: Vec::new(),
        }
    }

    /// Register an API with its version range.
    pub fn register(&mut self, api_key: i16, min_version: i16, max_version: i16) {
        self.entries.push((api_key, min_version, max_version));
    }

    /// Add an API key using its enum's built-in min/max versions.
    pub fn register_api(&mut self, key: ApiKey) {
        self.register(key.id(), key.min_version(), key.max_version());
    }

    /// Get all registered entries.
    pub fn entries(&self) -> &[(i16, i16, i16)] {
        &self.entries
    }

    /// Build the default set of APIs supported by this minimal broker.
    ///
    /// Only includes APIs that have actual handlers in this broker.
    pub fn default_broker_apis() -> Self {
        let mut registry = ApiRegistry::new();
        registry.register(0, 3, 13); // Produce
        registry.register(1, 0, 16); // Fetch
        registry.register(2, 0, 5);  // ListOffsets
        registry.register(3, 0, 13); // Metadata
        registry.register(18, 0, 5); // ApiVersions
        registry
    }
}

/// Broker-level state: topic metadata, partition state, and log segments.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaServer.scala
#[derive(Debug, Clone)]
pub struct ServerState {
    /// All known topics, keyed by topic name.
    pub topics: HashMap<String, TopicMetadata>,
    /// Log storage: (topic_name, partition_index) → list of record batches.
    ///
    /// Each batch is stored as raw bytes for simple round-trip produce/fetch.
    pub logs: HashMap<(String, i32), Vec<Box<[u8]>>>,
    /// Offset tracker per topic-partition.
    pub offsets: HashMap<(String, i32), i64>,
    /// The broker's advertised endpoint (host, port) for Metadata responses.
    pub broker_endpoint: Option<(String, i32)>,
    /// Registry of supported APIs for ApiVersions responses.
    pub api_registry: ApiRegistry,
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
    pub fn new() -> Self {
        ServerState {
            topics: HashMap::new(),
            logs: HashMap::new(),
            offsets: HashMap::new(),
            broker_endpoint: None,
            api_registry: ApiRegistry::default_broker_apis(),
        }
    }

    /// Get the API registry for ApiVersions responses.
    pub fn api_registry(&self) -> &ApiRegistry {
        &self.api_registry
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

        for i in 0..num_partitions {
            self.logs.entry((name.to_string(), i)).or_default();
            self.offsets.entry((name.to_string(), i)).or_insert(0);
        }
    }

    /// Append a record batch to a topic-partition log.
    ///
    /// Returns the base offset assigned to this append.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/ReplicaManager.scala (appendRecords)
    pub fn append_records(&mut self, topic: &str, partition: i32, batch: Vec<u8>) -> i64 {
        let key = (topic.to_string(), partition);
        let logs = self.logs.entry(key.clone()).or_default();
        let offset = self.offsets.entry(key).or_insert(0);

        let base_offset = *offset;
        logs.push(batch.into_boxed_slice());

        // Simple offset tracking: count records in batch (we don't parse,
        // just assign sequential offsets)
        *offset = base_offset + 1;

        base_offset
    }

    /// Read records from a topic-partition log starting at the given offset.
    ///
    /// Returns the raw record batches.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/ReplicaManager.scala (readFromStart)
    pub fn read_records(&self, topic: &str, partition: i32, fetch_offset: i64) -> Vec<Box<[u8]>> {
        let key = (topic.to_string(), partition);
        match self.logs.get(&key) {
            Some(batches) => batches
                .iter()
                .enumerate()
                .skip(fetch_offset as usize)
                .map(|(_, b)| b.clone())
                .collect(),
            None => Vec::new(),
        }
    }

    /// Set the broker's advertised endpoint (host, port) for Metadata responses.
    pub fn set_broker_endpoint(&mut self, host: String, port: i32) {
        self.broker_endpoint = Some((host, port));
    }

    /// Get the broker's advertised endpoint, or default to localhost:9092.
    pub fn get_broker_endpoint(&self) -> (String, i32) {
        self.broker_endpoint.clone().unwrap_or(("localhost".to_string(), 9092))
    }
}
