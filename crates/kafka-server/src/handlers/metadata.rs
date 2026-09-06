//! Metadata request handler: return broker and topic metadata.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java

use crate::handlers::build_response_frame;
use crate::server_state::ServerState;
use kafka_protocol::byte_utils;

/// Handle a Metadata request: return brokers list and topic metadata.
///
/// Field order from JSON spec (MetadataResponse.json):
///   ThrottleTimeMs (v3+), Brokers (v0+), ClusterId (v2+),
///   ControllerId (v1+), Topics (v0+), [ClusterAuthorizedOperations (v8-10)],
///   ErrorCode (v13+), tagged_fields
///
/// v0-v8: non-flexible (STRING/ARRAY)
/// v9+ (flexible): COMPACT_STRING/COMPACT_ARRAY + tagged_fields
///
/// Response header format is determined by `is_flexible` (whether the request
/// used a flexible header version).
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java
pub fn handle_metadata(
    correlation_id: i32,
    state: &ServerState,
    api_version: i16,
    is_flexible: bool,
) -> Vec<u8> {
    let mut body = Vec::new();
    let body_is_flexible = api_version >= 9;

    // ThrottleTimeMs: INT32 (v3+, ignorable)
    if api_version >= 3 {
        body.extend_from_slice(&0i32.to_be_bytes());
    }

    // Brokers array — 1 broker: node_id=0, host="localhost", port=9092, rack=null
    let broker_count = 1i32;
    if body_is_flexible {
        byte_utils::write_unsigned_varint((broker_count + 1) as u32, &mut body).unwrap();
        // Broker: node_id INT32
        body.extend_from_slice(&0i32.to_be_bytes()); // node_id = 0
        // Host: COMPACT_STRING
        let host = "localhost";
        byte_utils::write_unsigned_varint((host.len() + 1) as u32, &mut body).unwrap();
        body.extend_from_slice(host.as_bytes());
        // Port: INT32
        body.extend_from_slice(&9092i32.to_be_bytes());
        // Rack: NULLABLE_COMPACT_STRING (null → 0)
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
        // tagged_fields
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
    } else {
        body.extend_from_slice(&broker_count.to_be_bytes());
        body.extend_from_slice(&0i32.to_be_bytes()); // node_id = 0
        // Host: STRING
        let host = "localhost";
        body.extend_from_slice(&(host.len() as i16).to_be_bytes());
        body.extend_from_slice(host.as_bytes());
        body.extend_from_slice(&9092i32.to_be_bytes()); // port
        if api_version >= 1 {
            // Rack: STRING (nullable → -1)
            body.extend_from_slice(&(-1i16).to_be_bytes());
        }
    }

    // ClusterId: STRING/COMPACT_STRING (v2+, nullable)
    if body_is_flexible {
        byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // null compact_string
    } else if api_version >= 2 {
        body.extend_from_slice(&(-1i16).to_be_bytes()); // null string
    }

    // ControllerId: INT32 (v1+, ignorable)
    if api_version >= 1 {
        body.extend_from_slice(&0i32.to_be_bytes()); // controller_id = 0
    }

    // Topics array
    let topic_count = state.topics.len() as i32;
    if body_is_flexible {
        byte_utils::write_unsigned_varint((topic_count + 1) as u32, &mut body).unwrap();
    } else {
        body.extend_from_slice(&topic_count.to_be_bytes());
    }

    for topic in state.topics.values() {
        if body_is_flexible {
            // error_code: INT16
            body.extend_from_slice(&0i16.to_be_bytes()); // NO_ERROR
            // name: COMPACT_STRING (nullable in v12+, but populated for existing topics)
            byte_utils::write_unsigned_varint((topic.name.len() + 1) as u32, &mut body).unwrap();
            body.extend_from_slice(topic.name.as_bytes());
            // topic_id: UUID (v10+)
            if api_version >= 10 {
                let msb = topic.uuid.get_most_significant_bits().to_be_bytes();
                let lsb = topic.uuid.get_least_significant_bits().to_be_bytes();
                body.extend_from_slice(&msb);
                body.extend_from_slice(&lsb);
            }
            // is_internal: BOOLEAN (v1+)
            if api_version >= 1 {
                body.push(0); // is_internal = false
            }
            // Partitions: COMPACT_ARRAY
            let partition_count = topic.partitions.len() as i32;
            byte_utils::write_unsigned_varint((partition_count + 1) as u32, &mut body).unwrap();
            for partition in &topic.partitions {
                // error_code: INT16
                body.extend_from_slice(&0i16.to_be_bytes()); // NO_ERROR
                // partition_index: INT32
                body.extend_from_slice(&partition.partition_index.to_be_bytes());
                // leader_id: INT32
                body.extend_from_slice(&0i32.to_be_bytes()); // leader_id = 0
                // leader_epoch: INT32 (v7+)
                if api_version >= 7 {
                    body.extend_from_slice(&0i32.to_be_bytes());
                }
                // replica_nodes: COMPACT_ARRAY of INT32
                byte_utils::write_unsigned_varint(2, &mut body).unwrap(); // 1 replica
                body.extend_from_slice(&0i32.to_be_bytes()); // node_id = 0
                // isr_nodes: COMPACT_ARRAY of INT32
                byte_utils::write_unsigned_varint(2, &mut body).unwrap(); // 1 isr
                body.extend_from_slice(&0i32.to_be_bytes()); // node_id = 0
                // offline_replicas: COMPACT_ARRAY of INT32 (v5+)
                if api_version >= 5 {
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap(); // 0 offline
                }
                // tagged_fields
                byte_utils::write_unsigned_varint(0, &mut body).unwrap();
            }
            // topic_authorized_operations: INT32 (v8+, ignorable)
            if api_version >= 8 {
                body.extend_from_slice(&0i32.to_be_bytes());
            }
            // tagged_fields
            byte_utils::write_unsigned_varint(0, &mut body).unwrap();
        } else {
            // v0-v8 non-flexible topic entry
            body.extend_from_slice(&(topic.name.len() as i16).to_be_bytes());
            body.extend_from_slice(topic.name.as_bytes());
            // IsInternal: BOOLEAN (v1+)
            if api_version >= 1 {
                body.push(0); // is_internal = false
            }
            let partition_count = topic.partitions.len() as i32;
            body.extend_from_slice(&partition_count.to_be_bytes());
            for partition in &topic.partitions {
                body.extend_from_slice(&0i16.to_be_bytes()); // error_code
                body.extend_from_slice(&partition.partition_index.to_be_bytes());
                body.extend_from_slice(&0i32.to_be_bytes()); // leader_id = 0
                // LeaderEpoch: INT32 (v7+)
                if api_version >= 7 {
                    body.extend_from_slice(&0i32.to_be_bytes()); // leader_epoch = 0
                }
                // ReplicaNodes: ARRAY of INT32
                body.extend_from_slice(&1i32.to_be_bytes()); // 1 replica
                body.extend_from_slice(&0i32.to_be_bytes()); // node_id = 0
                // IsrNodes: ARRAY of INT32
                body.extend_from_slice(&1i32.to_be_bytes()); // 1 isr
                body.extend_from_slice(&0i32.to_be_bytes()); // node_id = 0
                // OfflineReplicas: ARRAY of INT32 (v5+)
                if api_version >= 5 {
                    body.extend_from_slice(&0i32.to_be_bytes()); // 0 offline replicas
                }
            }
            // TopicAuthorizedOperations: INT32 (v8+)
            if api_version >= 8 {
                body.extend_from_slice(&0i32.to_be_bytes());
            }
        }
    }

    // ClusterAuthorizedOperations: INT32 (v8-10 only, NOT v11+)
    // Skipped for v13 — not in the version range

    // ErrorCode: INT16 (v13+, ignorable)
    if api_version >= 13 {
        body.extend_from_slice(&0i16.to_be_bytes()); // top-level error_code = NO_ERROR
    }

    // Top-level tagged_fields (flexible only)
    if body_is_flexible {
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
    } else {
        // Non-flexible: no top-level tagged_fields
        // (no additional error_code for v0-v12)
    }

    build_response_frame(correlation_id, is_flexible, body)
}
