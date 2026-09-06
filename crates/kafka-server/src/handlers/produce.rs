//! Produce request handler: parse request body, store records, build response.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java
//!   core/src/main/scala/kafka/server/KafkaApis.scala (handleProduce)

use crate::handlers::build_response_frame;
use crate::server_state::ServerState;
use kafka_net::kafka_request::KafkaRequest;
use kafka_protocol::byte_utils;
use kafka_protocol::byte_utils::read_unsigned_varint_from_slice;

/// Parsed Produce request details for one topic-partition.
///
/// MIGRATION_SOURCE: clients/.../ProduceRequest.java
#[derive(Debug)]
pub struct ProduceTopicPartition {
    pub topic_name: String,
    pub partition_index: i32,
    pub record_batch: Vec<u8>,
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

/// Parse a Produce request body to extract topic-partition entries.
///
/// Handles both non-flexible (v0/v1) and flexible (v2+) formats.
///
/// MIGRATION_SOURCE: clients/.../ProduceRequest.java
fn parse_produce_partitions(body: &[u8], is_flexible: bool) -> Vec<ProduceTopicPartition> {
    let mut buf = body;
    let mut result = Vec::new();

    // Skip transactional_id
    if is_flexible {
        let _ = read_compact_string(&mut buf);
    } else {
        let _ = read_string(&mut buf);
    }

    // Skip acks (INT16)
    if buf.len() < 2 {
        return result;
    }
    buf = &buf[2..];

    // Skip timeout_ms (INT32)
    if buf.len() < 4 {
        return result;
    }
    buf = &buf[4..];

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
            if buf.len() < 4 {
                break;
            }
            let partition_index = i32::from_be_bytes(buf[0..4].try_into().unwrap());
            buf = &buf[4..];

            // record_set: INT32 length + raw record batch bytes
            if buf.len() < 4 {
                break;
            }
            let batch_len = i32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
            buf = &buf[4..];
            if buf.len() >= batch_len {
                let record_batch = buf[..batch_len].to_vec();
                result.push(ProduceTopicPartition {
                    topic_name: topic_name.clone(),
                    partition_index,
                    record_batch,
                });
            }
        }
    }

    result
}

/// Handle a Produce request: extract topic/partition info, append records
/// to the log, and return a Produce response with the assigned base_offset.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala (handleProduce)
pub fn handle_produce(request: &KafkaRequest, state: &mut ServerState) -> Vec<u8> {
    let is_flexible = request.header.flexible;
    let api_version = request.header.api_version;
    let body_is_flexible = api_version >= 9;
    let parts = parse_produce_partitions(&request.body, body_is_flexible);

    let mut base_offsets: Vec<i64> = Vec::new();
    for part in &parts {
        if !state.topics.contains_key(&part.topic_name) {
            state.register_topic(&part.topic_name, 1);
        }
        let base_offset = state.append_records(
            &part.topic_name,
            part.partition_index,
            part.record_batch.clone(),
        );
        base_offsets.push(base_offset);
    }

    build_produce_response(
        request.header.correlation_id,
        is_flexible,
        api_version,
        &parts,
        &base_offsets,
    )
}

/// Build a Produce response echoing back topics/partitions from the request,
/// with actual base_offset values from the log append.
///
/// MIGRATION_SOURCE: clients/.../ProduceResponse.java
fn build_produce_response(
    correlation_id: i32,
    is_flexible: bool,
    api_version: i16,
    parts: &[ProduceTopicPartition],
    base_offsets: &[i64],
) -> Vec<u8> {
    let mut body = Vec::new();
    let body_is_flexible = api_version >= 9;

    // responses ARRAY / COMPACT_ARRAY
    if body_is_flexible {
        byte_utils::write_unsigned_varint((parts.len() + 1) as u32, &mut body).unwrap();
    } else {
        body.extend_from_slice(&(parts.len() as i32).to_be_bytes());
    }

    for (i, part) in parts.iter().enumerate() {
        let base_offset = base_offsets.get(i).copied().unwrap_or(0);

        if body_is_flexible {
            // topic_name: COMPACT_STRING
            byte_utils::write_unsigned_varint((part.topic_name.len() + 1) as u32, &mut body).unwrap();
            body.extend_from_slice(part.topic_name.as_bytes());
            // partitions: COMPACT_ARRAY (1 partition)
            byte_utils::write_unsigned_varint(2, &mut body).unwrap();
            body.extend_from_slice(&part.partition_index.to_be_bytes()); // partition_index
            body.extend_from_slice(&0i16.to_be_bytes()); // error_code = NO_ERROR
            body.extend_from_slice(&base_offset.to_be_bytes()); // base_offset
            if api_version >= 6 {
                body.extend_from_slice(&0i64.to_be_bytes()); // log_append_time_ms = 0 (v6+)
            }
            byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // tagged_fields
            byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // topic tagged_fields
        } else {
            // topic_name: STRING
            body.extend_from_slice(&(part.topic_name.len() as i16).to_be_bytes());
            body.extend_from_slice(part.topic_name.as_bytes());
            // partitions: ARRAY (1 partition)
            body.extend_from_slice(&1i32.to_be_bytes());
            body.extend_from_slice(&part.partition_index.to_be_bytes()); // partition_index
            body.extend_from_slice(&0i16.to_be_bytes()); // error_code = NO_ERROR
            body.extend_from_slice(&base_offset.to_be_bytes()); // base_offset
            if api_version >= 1 {
                body.extend_from_slice(&0i64.to_be_bytes()); // log_append_time_ms = 0
            }
        }
    }

    // v6+ has throttle_time_ms
    if api_version >= 6 {
        body.extend_from_slice(&0i32.to_be_bytes());
    }
    // tagged_fields at the end (flexible body only)
    if body_is_flexible {
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
    }

    build_response_frame(correlation_id, is_flexible, body)
}
