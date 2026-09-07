//! Fetch request handler: parse request body, retrieve stored records, build response.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/FetchResponse.java
//!   core/src/main/scala/kafka/server/KafkaApis.scala (handleFetch)

use crate::handlers::{build_response_frame, RequestContext};
use crate::server_state::ServerState;
use kafka_protocol::byte_utils;
use kafka_protocol::byte_utils::read_unsigned_varint_from_slice;

/// Parsed Fetch request details for one topic-partition.
///
/// MIGRATION_SOURCE: clients/.../FetchRequest.java
#[derive(Debug)]
pub struct FetchTopicPartition {
    pub topic_name: String,
    pub partition_index: i32,
    pub fetch_offset: i64,
}

/// Parse a non-flexible string (2-byte length prefix + bytes) from buf.
fn read_string(buf: &mut &[u8]) -> Option<String> {
    if buf.len() < 2 {
        return None;
    }
    let len = i16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
    *buf = &buf[2..];
    if buf.len() < len {
        return None;
    }
    let s = String::from_utf8_lossy(&buf[..len]).into_owned();
    *buf = &buf[len..];
    Some(s)
}

/// Parse a compact string (varint length prefix + bytes) from buf.
fn read_compact_string(buf: &mut &[u8]) -> Option<String> {
    let (raw_len, consumed) = read_unsigned_varint_from_slice(buf).ok()?;
    *buf = &buf[consumed..];
    let actual_len = raw_len.saturating_sub(1) as usize;
    if buf.len() < actual_len {
        return None;
    }
    let s = String::from_utf8_lossy(&buf[..actual_len]).into_owned();
    *buf = &buf[actual_len..];
    Some(s)
}

/// Read array count from buf, handling both flexible (varint) and non-flexible (i32).
fn read_array_count(buf: &mut &[u8], is_flexible: bool) -> Option<usize> {
    if is_flexible {
        let (raw, consumed) = read_unsigned_varint_from_slice(buf).ok()?;
        *buf = &buf[consumed..];
        Some(raw.saturating_sub(1) as usize)
    } else {
        if buf.len() < 4 {
            return None;
        }
        let count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        *buf = &buf[4..];
        if count < 0 {
            return None;
        }
        Some(count as usize)
    }
}

/// Parse a Fetch request body to extract topic-partition fetch specs.
///
/// MIGRATION_SOURCE: clients/.../FetchRequest.java
fn parse_fetch_partitions(body: &[u8], is_flexible: bool) -> Vec<FetchTopicPartition> {
    let mut buf = body;
    let mut result = Vec::new();

    if is_flexible {
        // v2+: replica_id (varint)
        match read_unsigned_varint_from_slice(buf) {
            Ok((_, consumed)) => buf = &buf[consumed..],
            Err(_) => return result,
        }
    } else {
        // v0-v1: replica_id (int32)
        if buf.len() < 4 {
            return result;
        }
        buf = &buf[4..];
    }

    // Skip max_wait_ms (INT32)
    if buf.len() < 4 {
        return result;
    }
    buf = &buf[4..];

    // Skip min_bytes (INT32)
    if buf.len() < 4 {
        return result;
    }
    buf = &buf[4..];

    // Skip max_bytes (INT32, v3+)
    if is_flexible && buf.len() >= 4 {
        buf = &buf[4..];
    }

    // Skip isolation_level (INT8, v1+)
    if is_flexible {
        buf = &buf[1..];
    }

    // Read topics array
    let topic_count = match read_array_count(&mut buf, is_flexible) {
        Some(n) => n,
        None => return result,
    };

    for _ in 0..topic_count {
        let topic_name = if is_flexible {
            read_compact_string(&mut buf)
        } else {
            read_string(&mut buf)
        };

        let Some(topic_name) = topic_name else {
            break;
        };

        let partition_count = match read_array_count(&mut buf, is_flexible) {
            Some(n) => n,
            None => break,
        };

        if partition_count > 0 {
            if buf.len() < 12 {
                break;
            }
            let partition_index = i32::from_be_bytes(buf[0..4].try_into().unwrap());
            let fetch_offset = i64::from_be_bytes(buf[4..12].try_into().unwrap());
            buf = &buf[12..];
            // Skip max_bytes (INT32) if present
            if buf.len() >= 4 {
                buf = &buf[4..];
            }
            result.push(FetchTopicPartition {
                topic_name: topic_name.clone(),
                partition_index,
                fetch_offset,
            });
        }
    }

    result
}

/// Handle a Fetch request: extract topic/partition, retrieve stored records,
/// and return a Fetch response.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala (handleFetch)
pub fn handle_fetch(ctx: RequestContext) -> Vec<u8> {
    let is_flexible = ctx.is_flexible;
    let api_version = ctx.api_version;
    let body_is_flexible = api_version >= 12;
    let parts = parse_fetch_partitions(&ctx.body, body_is_flexible);

    let state = ctx.state.read_atomic();
    build_fetch_response(
        ctx.correlation_id,
        is_flexible,
        api_version,
        &parts,
        &state,
    )
}

/// Build a Fetch response with stored record batches.
///
/// v0-v11 (non-flexible):
///   responses ARRAY [
///     topic_name STRING
///     partitions ARRAY [
///       partition_index INT32
///       error_code INT16
///       high_watermark INT64
///       records: NULLABLE_BYTES
///     ]
///   ]
///   error_code INT16
///
/// v12+ (flexible): throttle_time_ms + responses COMPACT_ARRAY + tagged_fields
///
/// MIGRATION_SOURCE: clients/.../FetchResponse.java
fn build_fetch_response(
    correlation_id: i32,
    is_flexible: bool,
    api_version: i16,
    parts: &[FetchTopicPartition],
    state: &ServerState,
) -> Vec<u8> {
    let mut body = Vec::new();
    let body_is_flexible = api_version >= 12;

    // throttle_time_ms (v3+)
    if api_version >= 3 {
        body.extend_from_slice(&0i32.to_be_bytes());
    }

    // responses ARRAY / COMPACT_ARRAY
    if body_is_flexible {
        byte_utils::write_unsigned_varint((parts.len() + 1) as u32, &mut body).unwrap();
    } else {
        body.extend_from_slice(&(parts.len() as i32).to_be_bytes());
    }

    for part in parts {
        let batches = state.read_records(&part.topic_name, part.partition_index, part.fetch_offset);

        if body_is_flexible {
            // topic_name: COMPACT_STRING
            byte_utils::write_unsigned_varint((part.topic_name.len() + 1) as u32, &mut body).unwrap();
            body.extend_from_slice(part.topic_name.as_bytes());
            // partitions: COMPACT_ARRAY (1)
            byte_utils::write_unsigned_varint(2, &mut body).unwrap();
            body.extend_from_slice(&part.partition_index.to_be_bytes()); // partition_index
            body.extend_from_slice(&0i16.to_be_bytes()); // error_code
            body.extend_from_slice(&0i64.to_be_bytes()); // last_stable_offset
            if api_version >= 8 { body.extend_from_slice(&0i64.to_be_bytes()); } // log_start_offset
            body.extend_from_slice(&0i64.to_be_bytes()); // log_end_offset
            // records: COMPACT_BYTES (varint length + bytes, or 0 for null)
            let total: usize = batches.iter().map(|b| b.len()).sum();
            if total > 0 {
                byte_utils::write_unsigned_varint((total + 1) as u32, &mut body).unwrap();
                for batch in &batches { body.extend_from_slice(batch); }
            } else {
                byte_utils::write_unsigned_varint(0, &mut body).unwrap();
            }
            byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // partition tagged_fields
            byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // topic tagged_fields
        } else {
            // v0-v11 non-flexible
            body.extend_from_slice(&(part.topic_name.len() as i16).to_be_bytes());
            body.extend_from_slice(part.topic_name.as_bytes());
            body.extend_from_slice(&1i32.to_be_bytes()); // partitions ARRAY count = 1
            body.extend_from_slice(&part.partition_index.to_be_bytes()); // partition_index
            body.extend_from_slice(&0i16.to_be_bytes()); // error_code
            body.extend_from_slice(&0i64.to_be_bytes()); // high_watermark
            if api_version >= 8 {
                body.extend_from_slice(&0i64.to_be_bytes()); // last_stable_offset
                body.extend_from_slice(&0i64.to_be_bytes()); // log_start_offset
                body.extend_from_slice(&0i64.to_be_bytes()); // log_end_offset
            }
            // records: NULLABLE_BYTES (INT32 length + bytes, or -1 for null)
            let total: usize = batches.iter().map(|b| b.len()).sum();
            if total > 0 {
                body.extend_from_slice(&(total as i32).to_be_bytes());
                for batch in &batches { body.extend_from_slice(batch); }
            } else {
                body.extend_from_slice(&(-1i32).to_be_bytes()); // null records
            }
        }
    }

    // Top-level: error_code (non-flexible) or tagged_fields (flexible)
    if body_is_flexible {
        byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // tagged_fields
    } else {
        body.extend_from_slice(&0i16.to_be_bytes()); // error_code = NO_ERROR
    }

    build_response_frame(correlation_id, is_flexible, body)
}
