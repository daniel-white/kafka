//! ListOffsets request and response messages, implementing Writable and Readable traits.
//!
//! Wire format from ListOffsetsResponse.json:
//!   v0-v4 (non-flexible): Responses[] (TopicName, Partitions[])
//!   v5+ (flexible): throttle_time_ms + responses COMPACT_ARRAY + tagged_fields
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsResponse.java

use crate::errors::ProtocolError;
use crate::io::reader::Readable;
use crate::io::reader::Reader;
use crate::io::writer::{Writable, Writer};
use crate::messages::tagged_fields::TaggedFields;
use crate::MessageContext;
use getset::Getters;

/// ListOffsets request message.
///
/// For this broker implementation, the request body is not parsed
/// (the broker returns minimal response with empty topic list). This type
/// implements `Readable` so handlers can use the standard
/// `ctx.read_msg::<ListOffsetsRequest>()` pattern.
#[derive(Debug, Clone, Default)]
pub struct ListOffsetsRequest;

impl Readable for ListOffsetsRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(ListOffsetsRequest)
    }
}

// ── ListOffsets Partition Response ────────────────────────────────────────

/// A single partition entry in the ListOffsetsResponse.
#[derive(Debug, Clone, Default, Getters)]
pub struct ListOffsetsPartitionResponse {
    #[get]
    partition_index: i32,
    #[get]
    error_code: i16,
    #[get]
    offsets: Vec<i64>,
    #[get]
    tagged_fields: TaggedFields,
}

impl ListOffsetsPartitionResponse {
    pub fn new(partition_index: i32, error_code: i16, offsets: Vec<i64>) -> Self {
        ListOffsetsPartitionResponse {
            partition_index,
            error_code,
            offsets,
            tagged_fields: TaggedFields::empty(),
        }
    }
}

impl Writable for ListOffsetsPartitionResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 5;
        // partition_index: INT32 (always)
        w.write_int(self.partition_index);
        // error_code: INT16 (always)
        w.write_short(self.error_code);
        // offsets: ARRAY / COMPACT_ARRAY
        w.write_array_count(self.offsets.len(), flexible);
        for &offset in &self.offsets {
            w.write_long(offset);
        }
        // tagged_fields (flexible)
        if flexible {
            self.tagged_fields.write(w, ctx);
        }
    }
}

// ── ListOffsets Topic Response ────────────────────────────────────────────

/// A single topic entry in the ListOffsetsResponse.
#[derive(Debug, Clone, Default, Getters)]
pub struct ListOffsetsTopicResponse {
    #[get]
    name: String,
    #[get]
    partitions: Vec<ListOffsetsPartitionResponse>,
    #[get]
    tagged_fields: TaggedFields,
}

impl ListOffsetsTopicResponse {
    pub fn new(name: String, partitions: Vec<ListOffsetsPartitionResponse>) -> Self {
        ListOffsetsTopicResponse {
            name,
            partitions,
            tagged_fields: TaggedFields::empty(),
        }
    }
}

impl Writable for ListOffsetsTopicResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 5;
        // name: STRING / COMPACT_STRING
        if flexible {
            w.write_compact_string(&self.name);
        } else {
            w.write_string(&self.name);
        }
        // partitions: ARRAY / COMPACT_ARRAY
        w.write_array_count(self.partitions.len(), flexible);
        for partition in &self.partitions {
            partition.write(w, ctx);
        }
        // tagged_fields (flexible)
        if flexible {
            self.tagged_fields.write(w, ctx);
        }
    }
}

// ── Top-level ListOffsetsResponse ─────────────────────────────────────────

/// Full ListOffsetsResponse message.
#[derive(Debug, Clone, Default, Getters)]
pub struct ListOffsetsResponse {
    #[get]
    throttle_time_ms: i32,
    #[get]
    responses: Vec<ListOffsetsTopicResponse>,
    #[get]
    error_code: i16,
    #[get]
    tagged_fields: TaggedFields,
}

impl ListOffsetsResponse {
    pub fn new(
        throttle_time_ms: i32,
        responses: Vec<ListOffsetsTopicResponse>,
        error_code: i16,
    ) -> Self {
        ListOffsetsResponse {
            throttle_time_ms,
            responses,
            error_code,
            tagged_fields: TaggedFields::empty(),
        }
    }
}

impl Writable for ListOffsetsResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 5;
        // throttle_time_ms: INT32 (v5+, ignorable)
        if ctx.api_version() >= 5 {
            w.write_int(self.throttle_time_ms);
        }
        // responses: ARRAY / COMPACT_ARRAY
        w.write_array_count(self.responses.len(), flexible);
        for topic in &self.responses {
            topic.write(w, ctx);
        }
        // error_code: INT16 (v1+, ignorable)
        if ctx.api_version() >= 1 {
            w.write_short(self.error_code);
        }
        // tagged_fields (flexible body)
        if flexible {
            self.tagged_fields.write(w, ctx);
        }
    }
}