//! Fetch request and response messages, implementing Writable and Readable traits.
//!
//! Wire format from FetchResponse.json:
//!   v0-v11 (non-flexible): Responses[], [ThrottleTimeMs(v4+)], [ErrorCode(v7+)]
//!   v12+ (flexible): throttle_time_ms + responses COMPACT_ARRAY + tagged_fields
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/FetchResponse.java

use crate::errors::ProtocolError;
use crate::io::reader::Readable;
use crate::io::reader::Reader;
use crate::io::writer::{Writable, Writer};
use crate::messages::tagged_fields::TaggedFields;
use crate::MessageContext;
use getset::Getters;

/// Fetch request message.
///
/// Parses the request body according to the FetchRequest JSON spec.
/// The broker extracts topic/partition/fetch_offset info to look up stored records.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java
#[derive(Debug, Clone, Default)]
pub struct FetchRequest {
    pub topics: Vec<FetchTopic>,
    pub forgotten_topics: Vec<ForgottenTopic>,
}

/// A topic in a Fetch request.
#[derive(Debug, Clone, Default)]
pub struct FetchTopic {
    pub name: String,
    pub partitions: Vec<FetchPartition>,
}

impl FetchTopic {
    pub fn new(name: String, partitions: Vec<FetchPartition>) -> Self {
        FetchTopic { name, partitions }
    }
}

/// A partition in a Fetch request.
#[derive(Debug, Clone, Default)]
pub struct FetchPartition {
    pub partition: i32,
    pub fetch_offset: i64,
}

impl FetchPartition {
    pub fn new(partition: i32, fetch_offset: i64) -> Self {
        FetchPartition { partition, fetch_offset }
    }
}

/// A forgotten topic in an incremental fetch request (v7+).
#[derive(Debug, Clone, Default)]
pub struct ForgottenTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl ForgottenTopic {
    pub fn new(name: String, partitions: Vec<i32>) -> Self {
        ForgottenTopic { name, partitions }
    }
}

impl Readable for FetchRequest {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 12;

        // ClusterId (v12+, tagged v12+, ignorable)
        if ctx.api_version() >= 12 {
            let _ = r.read_compact_nullable_string()?;
        }

        // ReplicaId (v0-14, default -1)
        let _replica_id = if ctx.api_version() <= 14 {
            r.read_int()?
        } else {
            -1
        };

        // ReplicaState (v15+, tagged v15+)
        if ctx.api_version() >= 15 {
            let tag_count = r.read_unsigned_varint()?;
            for _ in 0..tag_count {
                let _tag = r.read_unsigned_varint()?;
                let size = r.read_unsigned_varint()? as usize;
                let _ = r.read_bytes_vec(size)?;
            }
        }

        // MaxWaitMs (v0+)
        let _max_wait_ms = r.read_int()?;

        // MinBytes (v0+)
        let _min_bytes = r.read_int()?;

        // MaxBytes (v3+, default 0x7fffffff, ignorable)
        if ctx.api_version() >= 3 {
            let _max_bytes = r.read_int()?;
        }

        // IsolationLevel (v4+, default 0, ignorable)
        if ctx.api_version() >= 4 {
            let _isolation_level = r.read_byte()?;
        }

        // SessionId (v7+, default 0, ignorable)
        if ctx.api_version() >= 7 {
            let _session_id = r.read_int()?;
        }

        // SessionEpoch (v7+, default -1, ignorable)
        if ctx.api_version() >= 7 {
            let _session_epoch = r.read_int()?;
        }

        // Topics (v0+)
        let topic_count = r.read_array_count(flexible)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = if ctx.api_version() < 13 {
                if flexible {
                    r.read_compact_string()?
                } else {
                    r.read_string_prefixed()?
                }
            } else {
                let _uuid = r.read_uuid()?;
                String::new()
            };

            let partition_count = r.read_array_count(flexible)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition = r.read_int()?;

                // CurrentLeaderEpoch (v9+, default -1, ignorable)
                if ctx.api_version() >= 9 {
                    let _ = r.read_int()?;
                }

                let fetch_offset = r.read_long()?;

                // LastFetchedEpoch (v12+, default -1)
                if ctx.api_version() >= 12 {
                    let _ = r.read_int()?;
                }

                // LogStartOffset (v5+, default -1, ignorable)
                if ctx.api_version() >= 5 {
                    let _ = r.read_long()?;
                }

                let _partition_max_bytes = r.read_int()?;

                // ReplicaDirectoryId (v17+, tagged v17+, ignorable)
                if ctx.api_version() >= 17 {
                    let tag_count = r.read_unsigned_varint()?;
                    for _ in 0..tag_count {
                        let _tag = r.read_unsigned_varint()?;
                        let size = r.read_unsigned_varint()? as usize;
                        let _ = r.read_bytes_vec(size)?;
                    }
                }

                // HighWatermark (v18+, default 9223372036854775807, tagged v18+, ignorable)
                if ctx.api_version() >= 18 {
                    let tag_count = r.read_unsigned_varint()?;
                    for _ in 0..tag_count {
                        let _tag = r.read_unsigned_varint()?;
                        let size = r.read_unsigned_varint()? as usize;
                        let _ = r.read_bytes_vec(size)?;
                    }
                }

                if flexible {
                    r.skip_tagged_fields()?;
                }

                partitions.push(FetchPartition::new(partition, fetch_offset));
            }

            if flexible {
                r.skip_tagged_fields()?;
            }

            topics.push(FetchTopic::new(name, partitions));
        }

        // ForgottenTopicsData (v7+, ignorable)
        let mut forgotten_topics = Vec::new();
        if ctx.api_version() >= 7 {
            let forgotten_count = r.read_array_count(flexible)?;
            for _ in 0..forgotten_count {
                let name = if ctx.api_version() < 13 {
                    if flexible {
                        r.read_compact_string()?
                    } else {
                        r.read_string_prefixed()?
                    }
                } else {
                    let _uuid = r.read_uuid()?;
                    String::new()
                };

                let partition_count = r.read_array_count(flexible)?;
                let mut partitions = Vec::with_capacity(partition_count);
                for _ in 0..partition_count {
                    partitions.push(r.read_int()?);
                }

                if flexible {
                    r.skip_tagged_fields()?;
                }

                forgotten_topics.push(ForgottenTopic::new(name, partitions));
            }
        }

        // RackId (v11+, default "", ignorable)
        if ctx.api_version() >= 11 {
            if flexible {
                let _ = r.read_compact_string()?;
            } else {
                let _ = r.read_string_prefixed()?;
            }
        }

        Ok(FetchRequest {
            topics,
            forgotten_topics,
        })
    }
}

// ── Fetch Partition Response ──────────────────────────────────────────────

/// A single partition entry in the FetchResponse.
#[derive(Debug, Clone, Default, Getters)]
pub struct FetchPartitionResponse {
    #[get]
    partition_index: i32,
    #[get]
    error_code: i16,
    #[get]
    high_watermark: i64,
    #[get]
    last_stable_offset: i64,
    #[get]
    log_start_offset: i64,
    #[get]
    log_end_offset: i64,
    #[get]
    records: Vec<u8>,
    #[get]
    tagged_fields: TaggedFields,
}

impl FetchPartitionResponse {
    pub fn new(
        partition_index: i32,
        error_code: i16,
        high_watermark: i64,
        last_stable_offset: i64,
        log_start_offset: i64,
        log_end_offset: i64,
        records: Vec<u8>,
    ) -> Self {
        FetchPartitionResponse {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            log_end_offset,
            records,
            tagged_fields: TaggedFields::empty(),
        }
    }
}

impl Writable for FetchPartitionResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 12;
        // partition_index: INT32 (always)
        w.write_int(self.partition_index);
        // error_code: INT16 (always)
        w.write_short(self.error_code);
        // high_watermark: INT64 (always)
        w.write_long(self.high_watermark);
        // last_stable_offset: INT64 (v8+)
        if ctx.api_version() >= 8 {
            w.write_long(self.last_stable_offset);
        }
        // log_start_offset: INT64 (v11+)
        if ctx.api_version() >= 11 {
            w.write_long(self.log_start_offset);
        }
        // log_end_offset: INT64 (v12+)
        if ctx.api_version() >= 12 {
            w.write_long(self.log_end_offset);
        }
        // records: NULLABLE_BYTES / COMPACT_NULLABLE_BYTES
        if flexible {
            if self.records.is_empty() {
                w.write_unsigned_varint(0); // null
            } else {
                w.write_unsigned_varint(self.records.len() as u32 + 1);
                w.write_bytes(&self.records);
            }
        } else {
            if self.records.is_empty() {
                w.write_int(-1); // null
            } else {
                w.write_int(self.records.len() as i32);
                w.write_bytes(&self.records);
            }
        }
        // tagged_fields (flexible)
        if flexible {
            self.tagged_fields.write(w, ctx);
        }
    }
}

// ── Fetch Topic Response ──────────────────────────────────────────────────

/// A single topic entry in the FetchResponse.
#[derive(Debug, Clone, Default, Getters)]
pub struct FetchTopicResponse {
    #[get]
    name: String,
    #[get]
    partitions: Vec<FetchPartitionResponse>,
    #[get]
    tagged_fields: TaggedFields,
}

impl FetchTopicResponse {
    pub fn new(name: String, partitions: Vec<FetchPartitionResponse>) -> Self {
        FetchTopicResponse {
            name,
            partitions,
            tagged_fields: TaggedFields::empty(),
        }
    }
}

impl Writable for FetchTopicResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 12;
        // name: STRING / COMPACT_STRING (v0-v11, v12+ uses topic_id)
        if ctx.api_version() < 12 {
            if flexible {
                w.write_compact_string(&self.name);
            } else {
                w.write_string(&self.name);
            }
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

// ── Top-level FetchResponse ───────────────────────────────────────────────

/// Full FetchResponse message.
#[derive(Debug, Clone, Default, Getters)]
pub struct FetchResponse {
    #[get]
    throttle_time_ms: i32,
    #[get]
    responses: Vec<FetchTopicResponse>,
    #[get]
    error_code: i16,
    #[get]
    tagged_fields: TaggedFields,
}

impl FetchResponse {
    pub fn new(
        throttle_time_ms: i32,
        responses: Vec<FetchTopicResponse>,
        error_code: i16,
    ) -> Self {
        FetchResponse {
            throttle_time_ms,
            responses,
            error_code,
            tagged_fields: TaggedFields::empty(),
        }
    }
}

impl Writable for FetchResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 12;
        // throttle_time_ms: INT32 (v4+, ignorable)
        if ctx.api_version() >= 4 {
            w.write_int(self.throttle_time_ms);
        }
        // responses: ARRAY / COMPACT_ARRAY
        w.write_array_count(self.responses.len(), flexible);
        for topic in &self.responses {
            topic.write(w, ctx);
        }
        // error_code: INT16 (v7+)
        if ctx.api_version() >= 7 {
            w.write_short(self.error_code);
        }
        // tagged_fields (flexible body)
        if flexible {
            self.tagged_fields.write(w, ctx);
        }
    }
}