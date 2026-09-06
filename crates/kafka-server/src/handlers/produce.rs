//! Produce request handler: parse request body, store records, build response.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java
//!   core/src/main/scala/kafka/server/KafkaApis.scala (handleProduce)

use crate::handlers::build_response_frame;
use crate::server_state::ServerState;
use kafka_net::kafka_request::KafkaRequest;
use kafka_protocol::byte_utils::read_unsigned_varint_from_slice;
use kafka_protocol::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::{MessageContext, Writable};

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

            // record_set: size prefix + raw record batch bytes
            // For flexible versions (v9+), librdkafka writes the size as a uvarint
            // (via rd_kafka_buf_finalize_arraycnt). For non-flexible, it's INT32.
            let batch_len: usize = if is_flexible {
                let (raw, consumed) = match read_unsigned_varint_from_slice(buf) {
                    Ok(v) => v,
                    Err(_) => break,
                };
                buf = &buf[consumed..];
                raw.saturating_sub(1) as usize  // uvarint count has +1 base
            } else {
                if buf.len() < 4 {
                    break;
                }
                let len = i32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
                buf = &buf[4..];
                len
            };
            if buf.len() >= batch_len {
                let record_batch = buf[..batch_len].to_vec();
                result.push(ProduceTopicPartition {
                    topic_name: topic_name.clone(),
                    partition_index,
                    record_batch,
                });
                buf = &buf[batch_len..];
            }
        }
    }

    result
}

/// Handle a Produce request: extract topic-partition info, append records
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

/// Build a Produce response using the trait-based ProduceResponse struct.
///
/// MIGRATION_SOURCE: clients/.../ProduceResponse.java
fn build_produce_response(
    correlation_id: i32,
    is_flexible: bool,
    api_version: i16,
    parts: &[ProduceTopicPartition],
    base_offsets: &[i64],
) -> Vec<u8> {
    let mut topics = Vec::new();

    for (i, part) in parts.iter().enumerate() {
        let base_offset = base_offsets.get(i).copied().unwrap_or(0);

        let partition = PartitionProduceResponse {
            index: part.partition_index,
            error_code: 0,
            base_offset,
            log_append_time_ms: 0,
            log_start_offset: 0,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: None,
        };

        topics.push(TopicProduceResponse::new(
            part.topic_name.clone(),
            vec![partition],
        ));
    }

    let response = ProduceResponse::new(topics, 0, 0);
    let ctx = MessageContext::new(api_version, is_flexible);
    let mut body = Vec::new();
    response.write(&mut body, &ctx);

    build_response_frame(correlation_id, is_flexible, body)
}
