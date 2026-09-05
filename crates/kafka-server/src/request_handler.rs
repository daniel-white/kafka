//! Request dispatcher: routes incoming Kafka requests by api_key to handlers.
//!
//! Mirrors `core/src/main/scala/kafka/server/KafkaApis.scala` request routing.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala

use kafka_net::kafka_request::KafkaRequest;
use kafka_net::response_header::ResponseHeader;
use kafka_protocol::ApiKey;

/// Dispatch a parsed request to the appropriate handler.
///
/// Currently handles: ApiVersions (api_key=15) with a full response listing
/// all known API keys and their version ranges.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
pub fn dispatch(request: &KafkaRequest) -> Vec<u8> {
    let api_key = ApiKey::from_id(request.header.api_key);
    let header = &request.header;

    match api_key {
        ApiKey::ApiVersions | ApiKey::Unknown(15) => {
            build_api_versions_response(header.correlation_id)
        }
        _ => {
            // Echo back with empty body for unimplemented APIs
            build_empty_response(header.correlation_id)
        }
    }
}

/// Build a complete ApiVersions v0 response.
///
/// Wire format (v0):
///   correlation_id  int32  (from header)
///   throttle_time_ms int32 (0)
///   error_code      int16 (0 = no error)
///   api_versions    array of (api_key: int16, min_version: int16, max_version: int16)
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java
fn build_api_versions_response(correlation_id: i32) -> Vec<u8> {
    // Known API keys and their version ranges
    let api_entries: &[(i16, i16)] = &[
        (0, 13),    // Produce
        (1, 16),    // Fetch
        (2, 5),     // ListOffsets
        (3, 5),     // Heartbeat
        (4, 3),     // Leave
        (5, 5),     // JoinGroup
        (6, 3),     // ElectLeaders
        (7, 2),     // AlterConfigs
        (8, 2),     // DescribeConfigs
        (9, 1),     // UpdateFeatures
        (11, 2),    // ListGroups
        (12, 2),    // DeleteGroups
        (13, 3),    // AddPartitionsToTxn
        (14, 2),    // RemoveFromTxn
        (15, 5),    // ApiVersions
        (16, 8),    // CreateTopics
        (17, 8),    // DeleteTopics
        (18, 2),    // DescribeTopics
        (19, 2),    // AlterReplicaLogDirs
        (20, 2),    // DescribeAcls
        (21, 2),    // DescribeUsers
        (22, 2),    // AlterUserScramCredentials
        (23, 2),    // DeleteRecords
        (24, 1),    // AddQuotas
        (25, 1),    // RemoveQuotas
        (26, 1),    // AlterPartition
        (27, 1),    // UnassignReplica
        (28, 1),    // AlterAcls
        (29, 1),    // DescribeQuotas
    ];

    let mut body = Vec::new();

    // throttle_time_ms = 0
    body.extend_from_slice(&0i32.to_be_bytes());

    // error_code = 0 (no error)
    body.extend_from_slice(&0i16.to_be_bytes());

    // api_versions array: 4-byte count (not compact array, so i32)
    body.extend_from_slice(&(api_entries.len() as i32).to_be_bytes());
    for (api_key, max_version) in api_entries {
        body.extend_from_slice(&api_key.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes()); // min_version
        body.extend_from_slice(&max_version.to_be_bytes());
    }

    // Build full frame: [4-byte body_size][response_header][body]
    let header = ResponseHeader::new(correlation_id);
    let response_size = (header.size() + body.len()) as i32;
    let mut frame = Vec::with_capacity(4 + header.size() + body.len());
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
