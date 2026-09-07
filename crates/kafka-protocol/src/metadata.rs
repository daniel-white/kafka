//! Metadata request and response messages, implementing Writable and Readable traits.
//!
//! Wire format from MetadataResponse.json:
//!   v0-v8 (non-flexible): ThrottleTimeMs(v3+), Brokers[], ClusterId(v2+),
//!     ControllerId(v1+), Topics[], [ClusterAuthorizedOperations(v8-10)], [ErrorCode(v13+)]
//!   v9+ (flexible): same fields but with compact encoding + tagged_fields
//!
//! Each field is conditionally serialized based on api_version.
//!
//! The trait-based approach mirrors Java's `ApiMessage` interface where each
//! message struct knows how to serialize itself for any version, using a
//! `MessageContext` that carries version + flexibility info (similar to how
//! `Hash` passes itself through a `Hasher` context).
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/MetadataRequest.java
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java

use crate::errors::ProtocolError;
use crate::io::reader::Reader;
use crate::io::writer::{Writable, Writer};
use crate::MessageContext;
use getset::{CopyGetters, Getters};

/// Metadata request message.
///
/// For v0-v6 this request has no body (just the header).
/// For v7+ there's a body, but we handle it separately.
#[derive(Debug, Clone, Default)]
pub struct MetadataRequest;

impl Readable for MetadataRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(MetadataRequest)
    }
}

// ── Broker ───────────────────────────────────────────────────────────────

/// A single broker entry in the MetadataResponse.
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct MetadataResponseBroker {
    #[get_copy = "pub"]
    node_id: i32,
    #[get = "pub"]
    host: String,
    #[get_copy = "pub"]
    port: i32,
    #[get = "pub"]
    rack: Option<String>,
}

impl MetadataResponseBroker {
    pub fn new(node_id: i32, host: String, port: i32, rack: Option<String>) -> Self {
        MetadataResponseBroker {
            node_id,
            host,
            port,
            rack,
        }
    }
}

impl Writable for MetadataResponseBroker {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 9;
        // node_id: INT32 (always)
        w.write_int(self.node_id);
        // host: STRING/COMPACT_STRING
        if flexible {
            w.write_compact_string(&self.host);
        } else {
            w.write_string(&self.host);
        }
        // port: INT32 (always)
        w.write_int(self.port);
        // rack: NULLABLE_STRING/COMPACT_NULLABLE_STRING (v1+)
        if ctx.api_version() >= 1 {
            if flexible {
                w.write_compact_nullable_string(self.rack.as_deref());
            } else {
                w.write_nullable_string(self.rack.as_deref());
            }
        }
        // tagged_fields (flexible only)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for MetadataResponseBroker {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 9;
        // node_id: INT32
        let node_id = r.read_int()?;
        // host: STRING/COMPACT_STRING
        let host = if flexible {
            r.read_compact_string()?
        } else {
            r.read_string_prefixed()?
        };
        // port: INT32
        let port = r.read_int()?;
        // rack: NULLABLE_STRING/COMPACT_NULLABLE_STRING (v1+)
        let rack = if ctx.api_version() >= 1 {
            if flexible {
                r.read_compact_nullable_string()?
            } else {
                r.read_nullable_string()?
            }
        } else {
            None
        };
        // Skip tagged_fields (flexible)
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(MetadataResponseBroker {
            node_id,
            host,
            port,
            rack,
        })
    }
}

// ── Partition ────────────────────────────────────────────────────────────

/// A single partition entry in the MetadataResponse.
#[derive(Debug, Clone, CopyGetters)]
pub struct MetadataResponsePartition {
    #[get_copy = "pub"]
    partition_index: i32,
}

impl MetadataResponsePartition {
    pub fn new(partition_index: i32) -> Self {
        MetadataResponsePartition { partition_index }
    }
}

impl Writable for MetadataResponsePartition {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 9;
        // error_code: INT16
        w.write_short(0); // NO_ERROR
        // partition_index: INT32
        w.write_int(self.partition_index);
        // leader_id: INT32
        w.write_int(0); // leader_id = 0 (our broker)
        // leader_epoch: INT32 (v7+)
        if ctx.api_version() >= 7 {
            w.write_int(0);
        }
        // replica_nodes: ARRAY/COMPACT_ARRAY of INT32
        w.write_array_count(1, flexible); // 1 replica
        w.write_int(0); // node_id = 0
        // isr_nodes: ARRAY/COMPACT_ARRAY of INT32
        w.write_array_count(1, flexible); // 1 isr
        w.write_int(0); // node_id = 0
        // offline_replicas: ARRAY/COMPACT_ARRAY of INT32 (v5+)
        if ctx.api_version() >= 5 {
            w.write_array_count(0, flexible); // 0 offline
        }
        // tagged_fields (flexible)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for MetadataResponsePartition {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 9;
        // error_code: INT16
        let _error_code = r.read_short()?;
        // partition_index: INT32
        let partition_index = r.read_int()?;
        // leader_id: INT32
        let _leader_id = r.read_int()?;
        // leader_epoch: INT32 (v7+)
        if ctx.api_version() >= 7 {
            let _leader_epoch = r.read_int()?;
        }
        // replica_nodes
        let replica_count = r.read_array_count(flexible)?;
        for _ in 0..replica_count {
            let _ = r.read_int()?;
        }
        // isr_nodes
        let isr_count = r.read_array_count(flexible)?;
        for _ in 0..isr_count {
            let _ = r.read_int()?;
        }
        // offline_replicas (v5+)
        if ctx.api_version() >= 5 {
            let offline_count = r.read_array_count(flexible)?;
            for _ in 0..offline_count {
                let _ = r.read_int()?;
            }
        }
        // tagged_fields
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(MetadataResponsePartition { partition_index })
    }
}

// ── Topic ────────────────────────────────────────────────────────────────

/// A single topic entry in the MetadataResponse.
#[derive(Debug, Clone, Getters, CopyGetters)]
pub struct MetadataResponseTopic {
    #[get = "pub"]
    name: String,
    #[get_copy = "pub"]
    error_code: i16,
    #[get_copy = "pub"]
    is_internal: bool,
    #[get = "pub"]
    partitions: Vec<MetadataResponsePartition>,
}

impl MetadataResponseTopic {
    pub fn new(
        name: String,
        error_code: i16,
        is_internal: bool,
        partitions: Vec<MetadataResponsePartition>,
    ) -> Self {
        MetadataResponseTopic {
            name,
            error_code,
            is_internal,
            partitions,
        }
    }
}

impl Writable for MetadataResponseTopic {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 9;
        // error_code: INT16
        w.write_short(self.error_code);
        // name: STRING/COMPACT_STRING
        if flexible {
            w.write_compact_string(&self.name);
        } else {
            w.write_string(&self.name);
        }
        // topic_id: UUID (v10+)
        if ctx.api_version() >= 10 {
            // Write zero UUID
            w.write_long(0);
            w.write_long(0);
        }
        // is_internal: BOOLEAN (v1+)
        if ctx.api_version() >= 1 {
            w.write_byte(if self.is_internal { 1 } else { 0 });
        }
        // partitions: ARRAY/COMPACT_ARRAY
        w.write_array_count(self.partitions.len(), flexible);
        for partition in &self.partitions {
            partition.write(w, ctx);
        }
        // topic_authorized_operations: INT32 (v8+, ignorable)
        if ctx.api_version() >= 8 {
            w.write_int(0);
        }
        // tagged_fields (flexible)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for MetadataResponseTopic {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 9;
        // error_code: INT16
        let error_code = r.read_short()?;
        // name: STRING/COMPACT_STRING
        let name = if flexible {
            r.read_compact_string()?
        } else {
            r.read_string_prefixed()?
        };
        // topic_id: UUID (v10+)
        if ctx.api_version() >= 10 {
            let _ = r.read_uuid()?;
        }
        // is_internal: BOOLEAN (v1+)
        let is_internal = if ctx.api_version() >= 1 {
            r.read_byte()? != 0
        } else {
            false
        };
        // partitions
        let partition_count = r.read_array_count(flexible)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(MetadataResponsePartition::read(r, ctx)?);
        }
        // topic_authorized_operations (v8+)
        if ctx.api_version() >= 8 {
            let _ = r.read_int()?;
        }
        // tagged_fields
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(MetadataResponseTopic {
            name,
            error_code,
            is_internal,
            partitions,
        })
    }
}

// ── Top-level Response ──────────────────────────────────────────────────

/// Full MetadataResponse message.
#[derive(Debug, Clone, Default)]
pub struct MetadataResponse {
    throttle_time_ms: i32,
    brokers: Vec<MetadataResponseBroker>,
    cluster_id: Option<String>,
    controller_id: i32,
    topics: Vec<MetadataResponseTopic>,
    cluster_authorized_operations: i32,
    error_code: i16,
}

impl MetadataResponse {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        throttle_time_ms: i32,
        brokers: Vec<MetadataResponseBroker>,
        cluster_id: Option<String>,
        controller_id: i32,
        topics: Vec<MetadataResponseTopic>,
        cluster_authorized_operations: i32,
        error_code: i16,
    ) -> Self {
        MetadataResponse {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
            error_code,
        }
    }

    /// Build a response with a single broker (node 0) and the given topics.
    pub fn with_broker_and_topics(topics: Vec<MetadataResponseTopic>) -> Self {
        let broker = MetadataResponseBroker::new(0, "localhost".to_string(), 9092, None);
        MetadataResponse::new(0, vec![broker], None, 0, topics, 0, 0)
    }
}

impl Writable for MetadataResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 9;

        // ThrottleTimeMs: INT32 (v3+, ignorable)
        if ctx.api_version() >= 3 {
            w.write_int(self.throttle_time_ms);
        }
        // Brokers: ARRAY/COMPACT_ARRAY
        w.write_array_count(self.brokers.len(), flexible);
        for broker in &self.brokers {
            broker.write(w, ctx);
        }
        // ClusterId: STRING/COMPACT_NULLABLE_STRING (v2+, nullable)
        if ctx.api_version() >= 2 {
            if flexible {
                w.write_compact_nullable_string(self.cluster_id.as_deref());
            } else {
                w.write_nullable_string(self.cluster_id.as_deref());
            }
        }
        // ControllerId: INT32 (v1+, ignorable)
        if ctx.api_version() >= 1 {
            w.write_int(self.controller_id);
        }
        // Topics: ARRAY/COMPACT_ARRAY
        w.write_array_count(self.topics.len(), flexible);
        for topic in &self.topics {
            topic.write(w, ctx);
        }
        // ClusterAuthorizedOperations: INT32 (v8-10 only, NOT v11+)
        if (8..=10).contains(&ctx.api_version()) {
            w.write_int(self.cluster_authorized_operations);
        }
        // ErrorCode: INT16 (v13+, ignorable)
        if ctx.api_version() >= 13 {
            w.write_short(self.error_code);
        }
        // Top-level tagged_fields (flexible body only)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for MetadataResponse {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 9;

        // ThrottleTimeMs: INT32 (v3+)
        let throttle_time_ms = if ctx.api_version() >= 3 {
            r.read_int()?
        } else {
            0
        };
        // Brokers
        let broker_count = r.read_array_count(flexible)?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            brokers.push(MetadataResponseBroker::read(r, ctx)?);
        }
        // ClusterId (v2+)
        let cluster_id = if ctx.api_version() >= 2 {
            if flexible {
                r.read_compact_nullable_string()?
            } else {
                r.read_nullable_string()?
            }
        } else {
            None
        };
        // ControllerId (v1+)
        let controller_id = if ctx.api_version() >= 1 {
            r.read_int()?
        } else {
            -1
        };
        // Topics
        let topic_count = r.read_array_count(flexible)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            topics.push(MetadataResponseTopic::read(r, ctx)?);
        }
        // ClusterAuthorizedOperations (v8-10)
        let cluster_authorized_operations = if (8..=10).contains(&ctx.api_version()) {
            r.read_int()?
        } else {
            -2147483648
        };
        // ErrorCode (v13+)
        let error_code = if ctx.api_version() >= 13 {
            r.read_short()?
        } else {
            0
        };
        // tagged_fields (flexible body only)
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(MetadataResponse {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
            error_code,
        })
    }
}

// Re-export the sub-messages at the crate level
pub use MetadataResponseBroker as Broker;
pub use MetadataResponsePartition as Partition;
pub use MetadataResponseTopic as Topic;
use crate::io::reader::Readable;
