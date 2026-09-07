//! DescribeTopics request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicsResponse.java

use crate::handlers::{build_response_frame, RequestContext};
use kafka_protocol::byte_utils;

/// Handle a DescribeTopics/DescribeTopicPartitions request: return metadata for all known topics.
///
/// v3+ (flexible): throttle_time_ms + topics COMPACT_ARRAY + tagged_fields
/// v0-v2 (non-flexible): topics ARRAY [error_code, topic_name, is_internal, partitions]
///
/// Response header format is determined by `is_flexible` (whether the request used a flexible header).
///
/// MIGRATION SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicsResponse.java
pub fn handle_describe_topics(ctx: &RequestContext) -> Vec<u8> {
    let state = ctx.state.read_atomic();
    let mut body = Vec::new();
    let body_is_flexible = ctx.api_version >= 3;

    if body_is_flexible {
        body.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms = 0
    }

    // topics array
    if body_is_flexible {
        byte_utils::write_unsigned_varint((state.topics.len() + 1) as u32, &mut body).unwrap();
    } else {
        body.extend_from_slice(&(state.topics.len() as i32).to_be_bytes());
    }

    for topic in state.topics.values() {
        if body_is_flexible {
            body.extend_from_slice(&0i16.to_be_bytes()); // error_code = NO_ERROR (per-topic)
            // topic_name: COMPACT_STRING
            byte_utils::write_unsigned_varint((topic.name.len() + 1) as u32, &mut body).unwrap();
            body.extend_from_slice(topic.name.as_bytes());
            // topic_id: UUID
            let msb = topic.uuid.get_most_significant_bits().to_be_bytes();
            let lsb = topic.uuid.get_least_significant_bits().to_be_bytes();
            body.extend_from_slice(&msb);
            body.extend_from_slice(&lsb);
            body.push(0); // is_internal = false
            body.extend_from_slice(&0i32.to_be_bytes()); // authorized_operations
        } else {
            // v0-v2 non-flexible
            body.extend_from_slice(&(topic.name.len() as i16).to_be_bytes());
            body.extend_from_slice(topic.name.as_bytes());
            body.push(0); // is_internal = false
        }

        // partitions array
        if topic.partitions.is_empty() {
            if body_is_flexible {
                byte_utils::write_unsigned_varint(1, &mut body).unwrap();
            } else {
                body.extend_from_slice(&0i32.to_be_bytes());
            }
        } else {
            if body_is_flexible {
                byte_utils::write_unsigned_varint((topic.partitions.len() + 1) as u32, &mut body).unwrap();
            } else {
                body.extend_from_slice(&(topic.partitions.len() as i32).to_be_bytes());
            }
            for partition in &topic.partitions {
                if body_is_flexible {
                    body.extend_from_slice(&0i16.to_be_bytes()); // error_code
                    body.extend_from_slice(&partition.partition_index.to_be_bytes());
                    body.extend_from_slice(&0i32.to_be_bytes()); // leader_id = 0 (our broker node id)
                    body.extend_from_slice(&0i32.to_be_bytes()); // leader_epoch
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap(); // replica_nodes
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap(); // isr_nodes
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap(); // adding_replicas
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap(); // removing_replicas
                    byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // tagged_fields
                } else {
                    body.extend_from_slice(&0i16.to_be_bytes()); // error_code
                    body.extend_from_slice(&partition.partition_index.to_be_bytes());
                    body.extend_from_slice(&0i32.to_be_bytes()); // leader_id = 0 (our broker node id)
                    body.extend_from_slice(&0i32.to_be_bytes()); // leader_epoch
                    body.extend_from_slice(&0i32.to_be_bytes()); // replica_nodes count = 0
                    body.extend_from_slice(&0i32.to_be_bytes()); // isr_nodes count = 0
                }
            }
        }

        if body_is_flexible {
            byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // topic tagged_fields
        }
    }

    if body_is_flexible {
        byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // end tagged_fields
    } else {
        body.extend_from_slice(&0i16.to_be_bytes()); // top-level error_code = 0
    }

    build_response_frame(ctx.correlation_id, ctx.is_flexible, body)
}
