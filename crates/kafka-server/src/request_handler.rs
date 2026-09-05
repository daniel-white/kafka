//! Request dispatcher: routes incoming Kafka requests by api_key to handlers.
//!
//! Mirrors `core/src/main/scala/kafka/server/KafkaApis.scala` request routing.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala

use crate::server_state::ServerState;
use kafka_protocol::byte_utils;
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::response_header::ResponseHeader;
use kafka_protocol::ApiKey;

/// Dispatch a parsed request to the appropriate handler.
///
/// Returns the serialized response bytes (frame: [4-byte size][response_header][body]).
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
pub fn dispatch(
    request: &KafkaRequest,
    state: &ServerState,
) -> Vec<u8> {
    let api_key = ApiKey::from_id(request.header.api_key);
    let version = request.header.api_version;

    match api_key {
        ApiKey::ApiVersions => build_api_versions_response(request.header.correlation_id, version),
        ApiKey::DescribeTopics => build_describe_topics_response(request.header.correlation_id, state, version),
        ApiKey::Produce => build_produce_response(request.header.correlation_id, version),
        ApiKey::Fetch => build_fetch_response(request.header.correlation_id, version),
        ApiKey::ListOffsets => build_list_offsets_response(request.header.correlation_id, version),
        _ => build_empty_response(request.header.correlation_id),
    }
}

/// Build an ApiVersions response.
///
/// v0/v1/v2 (non-flexible): throttle_time_ms, api_versions ARRAY, (error_code for v0)
/// v3+ (flexible): throttle_time_ms, api_versions COMPACT_ARRAY, supported_usable_versions STRING, tagged_fields
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java
fn build_api_versions_response(correlation_id: i32, version: i16) -> Vec<u8> {
    let api_entries: &[(i16, i16, i16)] = &[
        (0, 0, 13),    // Produce
        (1, 0, 16),    // Fetch
        (2, 0, 5),     // ListOffsets
        (3, 0, 5),     // Heartbeat
        (4, 0, 3),     // Leave
        (5, 0, 5),     // JoinGroup
        (6, 0, 3),     // ElectLeaders
        (7, 0, 2),     // AlterConfigs
        (8, 0, 2),     // DescribeConfigs
        (9, 0, 1),     // UpdateFeatures
        (11, 0, 2),    // ListGroups
        (12, 0, 2),    // DeleteGroups
        (13, 0, 3),    // AddPartitionsToTxn
        (14, 0, 2),    // RemoveFromTxn
        (15, 0, 5),    // ApiVersions
        (16, 0, 8),    // CreateTopics
        (17, 0, 8),    // DeleteTopics
        (18, 0, 2),    // DescribeTopics
        (19, 0, 2),    // AlterReplicaLogDirs
        (20, 0, 2),    // DescribeAcls
        (21, 0, 2),    // DescribeUsers
        (22, 0, 2),    // AlterUserScramCredentials
        (23, 0, 2),    // DeleteRecords
        (24, 0, 1),    // AddQuotas
        (25, 0, 1),    // RemoveQuotas
        (26, 0, 1),    // AlterPartition
        (27, 0, 1),    // UnassignReplica
        (28, 0, 1),    // AlterAcls
        (29, 0, 1),    // DescribeQuotas
    ];

    let mut body = Vec::new();

    // throttle_time_ms = 0
    body.extend_from_slice(&0i32.to_be_bytes());

    // error_code = 0 (only for v0, non-flexible)
    if version < 3 {
        body.extend_from_slice(&0i16.to_be_bytes());
    }

    if version >= 3 {
        // v3+ flexible: compact array
        byte_utils::write_unsigned_varint((api_entries.len() + 1) as u32, &mut body).unwrap();
        for (api_key, min_version, max_version) in api_entries {
            body.extend_from_slice(&api_key.to_be_bytes());
            body.extend_from_slice(&min_version.to_be_bytes());
            body.extend_from_slice(&max_version.to_be_bytes());
            // tagged_fields per entry: 0 = no tagged fields
            byte_utils::write_unsigned_varint(0, &mut body).unwrap();
        }
        // supported_usable_versions: STRING (2-byte length prefix + UTF-8)
        let usable_versions = "0..29";
        body.extend_from_slice(&(usable_versions.len() as i16).to_be_bytes());
        body.extend_from_slice(usable_versions.as_bytes());
        // tagged_fields
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
    } else {
        // v0/v1/v2 non-flexible: regular array (4-byte count)
        body.extend_from_slice(&(api_entries.len() as i32).to_be_bytes());
        for (api_key, min_version, max_version) in api_entries {
            body.extend_from_slice(&api_key.to_be_bytes());
            body.extend_from_slice(&min_version.to_be_bytes());
            body.extend_from_slice(&max_version.to_be_bytes());
        }
    }

    let header = ResponseHeader::new(correlation_id);
    let header_size = header.size();
    let response_size = (header_size + body.len()) as i32;
    let mut frame = Vec::with_capacity(4 + header_size + body.len());
    frame.extend_from_slice(&response_size.to_be_bytes());
    header.write(&mut frame);
    frame.extend_from_slice(&body);
    frame
}

/// Build a DescribeTopics v3 response.
///
/// Wire format (v3, flexible):
///   throttle_time_ms  int32
///   topics            compact_array (empty = 1 varint byte)
///   tagged_fields     tagged_fields
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicsResponse.java
fn build_describe_topics_response(correlation_id: i32, state: &ServerState, _version: i16) -> Vec<u8> {
    let mut body = Vec::new();

    // throttle_time_ms = 0
    body.extend_from_slice(&0i32.to_be_bytes());

    // topics compact array
    if state.topics.is_empty() {
        // Empty array: count = 1 (0 entries + 1)
        byte_utils::write_unsigned_varint(1, &mut body).unwrap();
    } else {
        byte_utils::write_unsigned_varint((state.topics.len() + 1) as u32, &mut body).unwrap();
        for topic in state.topics.values() {
            // error_code = 0 (no error)
            body.extend_from_slice(&0i16.to_be_bytes());
            // topic_name: compact string
            byte_utils::write_unsigned_varint((topic.name.len() + 1) as u32, &mut body).unwrap();
            body.extend_from_slice(topic.name.as_bytes());
            // topic_id: UUID (16 bytes: MSB + LSB in big-endian)
            let msb = topic.uuid.get_most_significant_bits().to_be_bytes();
            let lsb = topic.uuid.get_least_significant_bits().to_be_bytes();
            body.extend_from_slice(&msb);
            body.extend_from_slice(&lsb);
            // is_internal: bool (false = 0)
            body.push(0);
            // topic_authorized_operations: int32 (0 = no operations)
            body.extend_from_slice(&0i32.to_be_bytes());
            // partitions compact array
            if topic.partitions.is_empty() {
                byte_utils::write_unsigned_varint(1, &mut body).unwrap();
            } else {
                byte_utils::write_unsigned_varint((topic.partitions.len() + 1) as u32, &mut body).unwrap();
                for partition in &topic.partitions {
                    body.extend_from_slice(&0i16.to_be_bytes());  // error_code
                    body.extend_from_slice(&partition.partition_index.to_be_bytes());
                    body.extend_from_slice(&(-1i32).to_be_bytes());  // leader_id
                    body.extend_from_slice(&0i32.to_be_bytes());  // leader_epoch
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap();  // replica_nodes
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap();  // isr_nodes
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap();  // adding_replicas
                    byte_utils::write_unsigned_varint(1, &mut body).unwrap();  // removing_replicas
                    byte_utils::write_unsigned_varint(0, &mut body).unwrap();  // tagged_fields
                }
            }
            // topic_state tagged_fields
            byte_utils::write_unsigned_varint(0, &mut body).unwrap();
        }
    }

    // tagged_fields at end
    byte_utils::write_unsigned_varint(0, &mut body).unwrap();

    let header = ResponseHeader::new(correlation_id);
    let header_size = header.size();
    let response_size = (header_size + body.len()) as i32;
    let mut frame = Vec::with_capacity(4 + header_size + body.len());
    frame.extend_from_slice(&response_size.to_be_bytes());
    header.write(&mut frame);
    frame.extend_from_slice(&body);
    frame
}

/// Build a minimal Produce v0 response with an empty topic list.
///
/// Wire format (v0, non-flexible):
///   responses ARRAY (count=0)
///   throttle_time_ms (v6+ only)
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java
fn build_produce_response(correlation_id: i32, version: i16) -> Vec<u8> {
    let mut body = Vec::new();

    // responses ARRAY: 4-byte count = 0 (empty)
    body.extend_from_slice(&0i32.to_be_bytes());

    // v6+ has throttle_time_ms
    if version >= 6 {
        body.extend_from_slice(&0i32.to_be_bytes());
    }

    // v3+ has tagged_fields
    if version >= 3 {
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
    }

    let header = ResponseHeader::new(correlation_id);
    let header_size = header.size();
    let response_size = (header_size + body.len()) as i32;
    let mut frame = Vec::with_capacity(4 + header_size + body.len());
    frame.extend_from_slice(&response_size.to_be_bytes());
    header.write(&mut frame);
    frame.extend_from_slice(&body);
    frame
}

/// Build a minimal Fetch v0 response with empty topic list.
///
/// Wire format (v0, non-flexible):
///   throttle_time_ms: int32
///   responses ARRAY (count=0)
///   error_code: int16
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/FetchResponse.java
fn build_fetch_response(correlation_id: i32, _version: i16) -> Vec<u8> {
    let mut body = Vec::new();

    // throttle_time_ms = 0
    body.extend_from_slice(&0i32.to_be_bytes());

    // responses ARRAY: 4-byte count = 0 (empty)
    body.extend_from_slice(&0i32.to_be_bytes());

    // error_code = 0 (NO_ERROR)
    body.extend_from_slice(&0i16.to_be_bytes());

    let header = ResponseHeader::new(correlation_id);
    let header_size = header.size();
    let response_size = (header_size + body.len()) as i32;
    let mut frame = Vec::with_capacity(4 + header_size + body.len());
    frame.extend_from_slice(&response_size.to_be_bytes());
    header.write(&mut frame);
    frame.extend_from_slice(&body);
    frame
}

/// Build a minimal ListOffsets v0 response with empty topic list.
///
/// Wire format (v0, non-flexible):
///   responses ARRAY (count=0)
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsResponse.java
fn build_list_offsets_response(correlation_id: i32, _version: i16) -> Vec<u8> {
    let mut body = Vec::new();

    // responses ARRAY: 4-byte count = 0 (empty)
    body.extend_from_slice(&0i32.to_be_bytes());

    let header = ResponseHeader::new(correlation_id);
    let header_size = header.size();
    let response_size = (header_size + body.len()) as i32;
    let mut frame = Vec::with_capacity(4 + header_size + body.len());
    frame.extend_from_slice(&response_size.to_be_bytes());
    header.write(&mut frame);
    frame.extend_from_slice(&body);
    frame
}

/// Build a minimal error response with just the header (no error body).
///
/// Response format: [4-byte body_size][correlation_id]
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/AbstractResponse.java
fn build_empty_response(correlation_id: i32) -> Vec<u8> {
    let header = ResponseHeader::new(correlation_id);
    let body_size = header.size() as i32;
    let mut frame = Vec::with_capacity(4 + header.size());
    frame.extend_from_slice(&body_size.to_be_bytes());
    header.write(&mut frame);
    frame
}

// ServerState, TopicMetadata, ServerPartition are defined in server_state.rs
