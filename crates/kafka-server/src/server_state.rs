//! Broker-level state: topic metadata, partition state, and log manager.
//!
//! Mirrors `core/src/main/scala/kafka/server/KafkaServer.scala` and
//! `core/src/main/scala/kafka/server/KafkaApis.scala`.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaServer.scala

use kafka_common::Uuid;
use kafka_protocol::api_key::ApiKey;
use crate::handlers::{ApiHandlerResult, ApiRequest};
use std::collections::HashMap;

/// Metadata for a single API handler: the handler function.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
///   core/src/main/scala/kafka/server/KafkaApis.scala
#[derive(Clone, Copy)]
pub struct ApiHandlerInfo {
    pub api_key: ApiKey,
    pub handler: fn(ApiRequest) -> ApiHandlerResult,
}

impl std::fmt::Debug for ApiHandlerInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiHandlerInfo")
            .field("api_key", &self.api_key)
            .field("handler", &"<fn>")
            .finish()
    }
}

impl ApiHandlerInfo {
    /// Build an entry from an API key and its handler function.
    pub const fn new(api_key: ApiKey, handler: fn(ApiRequest) -> ApiHandlerResult) -> Self {
        ApiHandlerInfo { api_key, handler }
    }
}

/// Registry of supported APIs with their handlers.
///
/// This is the single source of truth: the same entries populate the
/// `ApiVersionsResponse` (via `ApiKey::min_version()` / `max_version()`)
/// and drive the `dispatch` lookup.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
#[derive(Debug, Clone, Default)]
pub struct ApiRegistry {
    entries: Vec<ApiHandlerInfo>,
}

impl ApiRegistry {
    pub fn new() -> Self {
        ApiRegistry {
            entries: Vec::new(),
        }
    }

    /// Register an API handler.
    pub fn register(&mut self, info: ApiHandlerInfo) {
        self.entries.push(info);
    }

    /// Get all registered entries.
    pub fn entries(&self) -> &[ApiHandlerInfo] {
        &self.entries
    }

    /// Find the handler for a given API key, if registered.
    pub fn find_handler(&self, key: ApiKey) -> Option<fn(ApiRequest) -> ApiHandlerResult> {
        self.entries.iter().find(|e| e.api_key == key).map(|e| e.handler)
    }

    /// Build the default set of APIs supported by this minimal broker.
    ///
    /// Only includes APIs that have actual handlers in this broker.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
    pub fn default_broker_apis() -> Self {
        let mut registry = ApiRegistry::new();
        registry.register(ApiHandlerInfo::new(ApiKey::Produce, crate::handlers::handle_produce));
        registry.register(ApiHandlerInfo::new(ApiKey::Fetch, crate::handlers::handle_fetch));
        registry.register(ApiHandlerInfo::new(ApiKey::ListOffsets, crate::handlers::handle_list_offsets));
        registry.register(ApiHandlerInfo::new(ApiKey::Metadata, crate::handlers::handle_metadata));
        registry.register(ApiHandlerInfo::new(ApiKey::DescribeTopicPartitions, crate::handlers::handle_describe_topics));
        registry.register(ApiHandlerInfo::new(ApiKey::ApiVersions, crate::handlers::handle_api_versions));
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

impl Default for ServerState {
    fn default() -> Self {
        Self::new()
    }
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
