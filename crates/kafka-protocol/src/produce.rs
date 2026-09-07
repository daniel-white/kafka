//! Produce request and response messages, implementing Writable trait.
//!
//! Wire format from ProduceResponse.json:
//!   v3-v8 (non-flexible): Responses[], [ThrottleTimeMs(v6+)], [ErrorCode(v13+)]
//!   v9+ (flexible): same fields but with compact encoding + tagged_fields
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java

use crate::errors::ProtocolError;
use crate::io::reader::Reader;
use crate::io::writer::{Writable, Writer};
use crate::MessageContext;

/// Produce request message.
///
/// This request has a body but for this broker implementation we read it
/// using the standard pattern then parse manually.
#[derive(Debug, Clone, Default)]
pub struct ProduceRequest;

impl Readable for ProduceRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(ProduceRequest)
    }
}

// ── Partition Produce Response ─────────────────────────────────────────────

/// A single partition entry in the ProduceResponse.
#[derive(Debug, Clone)]
pub struct PartitionProduceResponse {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
    pub record_errors: Vec<BatchIndexAndErrorMessage>,
    pub error_message: Option<String>,
    pub current_leader: Option<LeaderIdAndEpoch>,
}

/// Batch index and error message for dropped records (v8+).
#[derive(Debug, Clone)]
pub struct BatchIndexAndErrorMessage {
    pub batch_index: i32,
    pub batch_index_error_message: Option<String>,
}

/// Leader ID and epoch for the partition leader (v10+, tagged field).
#[derive(Debug, Clone)]
pub struct LeaderIdAndEpoch {
    pub leader_id: i32,
    pub leader_epoch: i32,
}

impl PartitionProduceResponse {
    pub fn new(index: i32, error_code: i16, base_offset: i64, log_append_time_ms: i64, log_start_offset: i64) -> Self {
        PartitionProduceResponse {
            index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: None,
        }
    }
}

impl Writable for PartitionProduceResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 9;
        // Index: INT32 (always)
        w.write_int(self.index);
        // ErrorCode: INT16 (always)
        w.write_short(self.error_code);
        // BaseOffset: INT64 (always)
        w.write_long(self.base_offset);
        // LogAppendTimeMs: INT64 (v2+)
        if ctx.api_version() >= 2 {
            w.write_long(self.log_append_time_ms);
        }
        // LogStartOffset: INT64 (v5+)
        if ctx.api_version() >= 5 {
            w.write_long(self.log_start_offset);
        }
        // RecordErrors: COMPACT_ARRAY (v8+)
        if ctx.api_version() >= 8 {
            w.write_array_count(self.record_errors.len(), flexible);
            for err in &self.record_errors {
                w.write_int(err.batch_index);
                if flexible {
                    w.write_compact_nullable_string(err.batch_index_error_message.as_deref());
                } else {
                    w.write_nullable_string(err.batch_index_error_message.as_deref());
                }
                if flexible {
                    w.write_empty_tagged_fields();
                }
            }
        }
        // ErrorMessage: STRING/COMPACT_NULLABLE_STRING (v8+)
        if ctx.api_version() >= 8 {
            if flexible {
                w.write_compact_nullable_string(self.error_message.as_deref());
            } else {
                w.write_nullable_string(self.error_message.as_deref());
            }
        }
        // tagged_fields (flexible)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for PartitionProduceResponse {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 9;
        let index = r.read_int()?;
        let error_code = r.read_short()?;
        let base_offset = r.read_long()?;
        let log_append_time_ms = if ctx.api_version() >= 2 { r.read_long()? } else { -1 };
        let log_start_offset = if ctx.api_version() >= 5 { r.read_long()? } else { -1 };
        
        let record_errors = if ctx.api_version() >= 8 {
            let count = r.read_array_count(flexible)?;
            let mut errors = Vec::with_capacity(count);
            for _ in 0..count {
                let batch_index = r.read_int()?;
                let batch_index_error_message = if flexible {
                    r.read_compact_nullable_string()?
                } else {
                    r.read_nullable_string()?
                };
                if flexible {
                    r.skip_tagged_fields()?;
                }
                errors.push(BatchIndexAndErrorMessage {
                    batch_index,
                    batch_index_error_message,
                });
            }
            errors
        } else {
            Vec::new()
        };
        
        let error_message = if ctx.api_version() >= 8 {
            if flexible {
                r.read_compact_nullable_string()?
            } else {
                r.read_nullable_string()?
            }
        } else {
            None
        };
        
        let current_leader = if ctx.api_version() >= 10 {
            // Will be read from tagged_fields, but we don't need it for parsing
            None
        } else {
            None
        };
        
        if flexible {
            r.skip_tagged_fields()?;
        }
        
        Ok(PartitionProduceResponse {
            index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            current_leader,
        })
    }
}

// ── Topic Produce Response ─────────────────────────────────────────────────

/// A single topic entry in the ProduceResponse.
#[derive(Debug, Clone)]
pub struct TopicProduceResponse {
    pub name: String,
    pub partitions: Vec<PartitionProduceResponse>,
}

impl TopicProduceResponse {
    pub fn new(name: String, partitions: Vec<PartitionProduceResponse>) -> Self {
        TopicProduceResponse { name, partitions }
    }
}

impl Writable for TopicProduceResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 9;
        // Name: STRING/COMPACT_STRING (v0-12 only, v13+ uses TopicId)
        if ctx.api_version() < 13 {
            if flexible {
                w.write_compact_string(&self.name);
            } else {
                w.write_string(&self.name);
            }
        }
        // TopicId: UUID (v13+ only)
        if ctx.api_version() >= 13 {
            w.write_long(0); // high bits of UUID
            w.write_long(0); // low bits of UUID
        }
        // PartitionResponses: ARRAY/COMPACT_ARRAY
        w.write_array_count(self.partitions.len(), flexible);
        for partition in &self.partitions {
            partition.write(w, ctx);
        }
        // tagged_fields (flexible)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

// ── Top-level ProduceResponse ──────────────────────────────────────────────

/// Full ProduceResponse message.
#[derive(Debug, Clone, Default)]
pub struct ProduceResponse {
    pub responses: Vec<TopicProduceResponse>,
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

impl ProduceResponse {
    pub fn new(responses: Vec<TopicProduceResponse>, throttle_time_ms: i32, error_code: i16) -> Self {
        ProduceResponse {
            responses,
            throttle_time_ms,
            error_code,
        }
    }
}

impl Writable for ProduceResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 9;
        // Responses: ARRAY/COMPACT_ARRAY
        w.write_array_count(self.responses.len(), flexible);
        for topic in &self.responses {
            topic.write(w, ctx);
        }
        // ThrottleTimeMs: INT32 (v6+, ignorable)
        if ctx.api_version() >= 6 {
            w.write_int(self.throttle_time_ms);
        }
        // ErrorCode: INT16 (v13+, ignorable)
        if ctx.api_version() >= 13 {
            w.write_short(self.error_code);
        }
        // tagged_fields (flexible body)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for ProduceResponse {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 9;
        let count = r.read_array_count(flexible)?;
        let mut responses = Vec::with_capacity(count);
        for _ in 0..count {
            let name = if ctx.api_version() < 13 {
                if flexible { r.read_compact_string()? } else { r.read_string_prefixed()? }
            } else {
                let _ = r.read_uuid()?;
                String::new()
            };
            let partition_count = r.read_array_count(flexible)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                partitions.push(PartitionProduceResponse::read(r, ctx)?);
            }
            if flexible { r.skip_tagged_fields()?; }
            responses.push(TopicProduceResponse { name, partitions });
        }
        let throttle_time_ms = if ctx.api_version() >= 6 { r.read_int()? } else { 0 };
        let error_code = if ctx.api_version() >= 13 { r.read_short()? } else { 0 };
        if flexible { r.skip_tagged_fields()?; }
        Ok(ProduceResponse {
            responses,
            throttle_time_ms,
            error_code,
        })
    }
}

// Re-export for convenience
pub use PartitionProduceResponse as Partition;
pub use TopicProduceResponse as Topic;
use crate::io::reader::Readable;
