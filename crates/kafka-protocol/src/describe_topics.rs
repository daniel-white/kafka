//! DescribeTopics request and response messages, implementing Writable/Readable traits.
//!
//! Wire format from DescribeTopicsResponse.json:
//!   v0-v2 (non-flexible): Topics[] [TopicName, ErrorCode, IsInternal, Partitions[]]
//!   v3+ (flexible): throttle_time_ms + topics COMPACT_ARRAY + tagged_fields
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicsResponse.java

use crate::errors::ProtocolError;
use crate::io::reader::Readable;
use crate::io::reader::Reader;
use crate::io::writer::Writable;
use crate::{MessageContext, Writer};
use getset::Getters;

/// DescribeTopics request message.
///
/// For this broker implementation, the request body is not parsed
/// (the broker returns metadata for all known topics). This type
/// implements `Readable` so handlers can use the standard
/// `ctx.read_msg::<DescribeTopicsRequest>()` pattern.
#[derive(Debug, Clone, Default)]
pub struct DescribeTopicsRequest;

impl Readable for DescribeTopicsRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(DescribeTopicsRequest)
    }
}

/// DescribeTopics response message.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicsResponse.java
#[derive(Debug, Clone, Default, Getters)]
pub struct DescribeTopicsResponse {
    #[get]
    throttle_time_ms: i32,
    #[get]
    topics: Vec<DescribeTopicsTopic>,
}

impl DescribeTopicsResponse {
    pub fn new(throttle_time_ms: i32, topics: Vec<DescribeTopicsTopic>) -> Self {
        DescribeTopicsResponse { throttle_time_ms, topics }
    }
}

impl Writable for DescribeTopicsResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 3;

        // throttle_time_ms (v3+)
        if flexible {
            w.write_int(self.throttle_time_ms);
        }

        // Topics: COMPACT_ARRAY (flexible) or ARRAY (non-flexible)
        w.write_array_count(self.topics.len(), flexible);
        for topic in &self.topics {
            topic.write(w, ctx);
        }

        // Top-level tagged_fields (flexible only)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for DescribeTopicsResponse {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 3;
        let throttle_time_ms = if flexible { r.read_int()? } else { 0 };
        let count = r.read_array_count(flexible)?;
        let mut topics = Vec::with_capacity(count);
        for _ in 0..count {
            topics.push(DescribeTopicsTopic::read(r, ctx)?);
        }
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(DescribeTopicsResponse { throttle_time_ms, topics })
    }
}

/// A topic entry in the DescribeTopics response.
#[derive(Debug, Clone, Default, Getters)]
pub struct DescribeTopicsTopic {
    #[get]
    name: String,
    error_code: i16,
    is_internal: bool,
    #[get]
    partitions: Vec<DescribeTopicsPartition>,
}

impl DescribeTopicsTopic {
    pub fn error_code(&self) -> i16 { self.error_code }
    pub fn is_internal(&self) -> bool { self.is_internal }
}

impl DescribeTopicsTopic {
    pub fn new(name: String, error_code: i16, is_internal: bool, partitions: Vec<DescribeTopicsPartition>) -> Self {
        DescribeTopicsTopic { name, error_code, is_internal, partitions }
    }
}

impl Writable for DescribeTopicsTopic {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 3;

        if flexible {
            w.write_short(self.error_code);
            w.write_compact_string(&self.name);
            w.write_uuid(&kafka_common::uuid::Uuid::ZERO_UUID);
            w.write_boolean(self.is_internal);
            w.write_int(0); // authorized_operations
        } else {
            w.write_string(&self.name);
            w.write_boolean(self.is_internal);
        }

        // Partitions
        w.write_array_count(self.partitions.len(), flexible);
        for partition in &self.partitions {
            partition.write(w, ctx);
        }

        // Topic tagged_fields (flexible only)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for DescribeTopicsTopic {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 3;
        let error_code = if flexible { r.read_short()? } else { 0 };
        let name = if flexible { r.read_compact_string()? } else { r.read_string_prefixed()? };
        let is_internal = if flexible { r.read_boolean()? } else { false };
        let count = r.read_array_count(flexible)?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(DescribeTopicsPartition::read(r, ctx)?);
        }
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(DescribeTopicsTopic { name, error_code, is_internal, partitions })
    }
}

/// A partition entry in the DescribeTopics response.
#[derive(Debug, Clone, Default, Getters)]
pub struct DescribeTopicsPartition {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    #[get]
    replica_nodes: Vec<i32>,
    #[get]
    isr_nodes: Vec<i32>,
    #[get]
    adding_replicas: Vec<i32>,
    #[get]
    removing_replicas: Vec<i32>,
}

impl DescribeTopicsPartition {
    pub fn error_code(&self) -> i16 { self.error_code }
    pub fn partition_index(&self) -> i32 { self.partition_index }
    pub fn leader_id(&self) -> i32 { self.leader_id }
    pub fn leader_epoch(&self) -> i32 { self.leader_epoch }
}

impl DescribeTopicsPartition {
    pub fn new(
        error_code: i16,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32,
        replica_nodes: Vec<i32>,
        isr_nodes: Vec<i32>,
        adding_replicas: Vec<i32>,
        removing_replicas: Vec<i32>,
    ) -> Self {
        DescribeTopicsPartition {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            adding_replicas,
            removing_replicas,
        }
    }
}

impl Writable for DescribeTopicsPartition {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 3;

        if flexible {
            w.write_short(self.error_code);
            w.write_int(self.partition_index);
            w.write_int(self.leader_id);
            w.write_int(self.leader_epoch);
            // Compact arrays: count = len + 1
            w.write_unsigned_varint(self.replica_nodes.len() as u32 + 1);
            for &node in &self.replica_nodes {
                w.write_unsigned_varint(node as u32 + 1);
            }
            w.write_unsigned_varint(self.isr_nodes.len() as u32 + 1);
            for &node in &self.isr_nodes {
                w.write_unsigned_varint(node as u32 + 1);
            }
            w.write_unsigned_varint(self.adding_replicas.len() as u32 + 1);
            for &node in &self.adding_replicas {
                w.write_unsigned_varint(node as u32 + 1);
            }
            w.write_unsigned_varint(self.removing_replicas.len() as u32 + 1);
            for &node in &self.removing_replicas {
                w.write_unsigned_varint(node as u32 + 1);
            }
            w.write_unsigned_varint(0); // tagged_fields
        } else {
            w.write_short(self.error_code);
            w.write_int(self.partition_index);
            w.write_int(self.leader_id);
            w.write_int(self.leader_epoch);
            w.write_int(self.replica_nodes.len() as i32);
            for &node in &self.replica_nodes {
                w.write_int(node);
            }
            w.write_int(self.isr_nodes.len() as i32);
            for &node in &self.isr_nodes {
                w.write_int(node);
            }
        }
    }
}

impl Readable for DescribeTopicsPartition {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 3;
        let error_code = if flexible { r.read_short()? } else { 0 };
        let partition_index = r.read_int()?;
        let leader_id = r.read_int()?;
        let leader_epoch = r.read_int()?;

        let replica_count = if flexible {
            r.read_unsigned_varint()? as usize - 1
        } else {
            r.read_int()? as usize
        };
        let mut replica_nodes = Vec::with_capacity(replica_count);
        for _ in 0..replica_count {
            replica_nodes.push(if flexible { r.read_unsigned_varint()? as i32 } else { r.read_int()? });
        }

        let isr_count = if flexible {
            r.read_unsigned_varint()? as usize - 1
        } else {
            r.read_int()? as usize
        };
        let mut isr_nodes = Vec::with_capacity(isr_count);
        for _ in 0..isr_count {
            isr_nodes.push(if flexible { r.read_unsigned_varint()? as i32 } else { r.read_int()? });
        }

        if flexible {
            let adding_count = r.read_unsigned_varint()? as usize - 1;
            let mut adding_replicas = Vec::with_capacity(adding_count);
            for _ in 0..adding_count {
                adding_replicas.push(r.read_unsigned_varint()? as i32);
            }
            let removing_count = r.read_unsigned_varint()? as usize - 1;
            let mut removing_replicas = Vec::with_capacity(removing_count);
            for _ in 0..removing_count {
                removing_replicas.push(r.read_unsigned_varint()? as i32);
            }
            r.skip_tagged_fields()?;
            Ok(DescribeTopicsPartition {
                error_code,
                partition_index,
                leader_id,
                leader_epoch,
                replica_nodes,
                isr_nodes,
                adding_replicas,
                removing_replicas,
            })
        } else {
            Ok(DescribeTopicsPartition {
                error_code,
                partition_index,
                leader_id,
                leader_epoch,
                replica_nodes,
                isr_nodes,
                adding_replicas: vec![],
                removing_replicas: vec![],
            })
        }
    }
}
