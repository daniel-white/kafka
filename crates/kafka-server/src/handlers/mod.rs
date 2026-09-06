//! API request handlers, organized by Kafka API key.
//!
//! Each module handles request parsing, state updates, and response serialization
//! for a single API key. The dispatcher in [`crate::request_handler`] routes
//! requests to the appropriate handler.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala

pub mod api_versions;
pub mod describe_topics;
pub mod fetch;
pub mod list_offsets;
pub mod metadata;
pub mod produce;

pub use api_versions::handle_api_versions;
pub use describe_topics::handle_describe_topics;
pub use fetch::handle_fetch;
pub use list_offsets::handle_list_offsets;
pub use metadata::handle_metadata;
pub use produce::handle_produce;

use crate::server_state::ServerState;
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::response_header::ResponseHeader;
use kafka_protocol::byte_utils;
use kafka_protocol::ApiKey;
use std::sync::RwLock;

/// Shared helper: wrap body bytes in a Kafka response frame.
///
/// Frame: [4-byte size][response_header][body]
/// For flexible responses (is_flexible=true), response_header includes a trailing tagged_fields varint.
/// For non-flexible responses, response_header is just correlation_id.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/AbstractResponse.java
pub fn build_response_frame(correlation_id: i32, is_flexible: bool, body: Vec<u8>) -> Vec<u8> {
    let header_size = if is_flexible { 5 } else { 4 };
    let response_size = (header_size + body.len()) as i32;
    let mut frame = Vec::with_capacity(4 + header_size + body.len());
    frame.extend_from_slice(&response_size.to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    if is_flexible {
        byte_utils::write_unsigned_varint(0, &mut frame).unwrap(); // tagged_fields
    }
    frame.extend_from_slice(&body);
    frame
}

/// Shared helper: build minimal error response (header only, no body).
///
/// MIGRATION_SOURCE: clients/.../AbstractResponse.java
pub fn build_empty_response(correlation_id: i32) -> Vec<u8> {
    let header = ResponseHeader::new(correlation_id);
    let body_size = header.size() as i32;
    let mut frame = Vec::with_capacity(4 + header.size());
    frame.extend_from_slice(&body_size.to_be_bytes());
    header.write(&mut frame);
    frame
}

/// Dispatch a parsed request to the appropriate handler.
///
/// State is read-only for most handlers; Produce needs mutable state to
/// append records.
///
/// Returns the serialized response bytes (frame: [4-byte size][response_header][body]).
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
pub fn dispatch(
    request: &KafkaRequest,
    state: &RwLock<ServerState>,
) -> Vec<u8> {
    let api_key = ApiKey::from_id(request.header.api_key);
    let api_version = request.header.api_version;
    let is_flexible = request.header.flexible;

    eprintln!(
        "dispatch: api_key={:?} (id={}), api_version={}, is_flexible={}, client_id={:?}, body_len={}",
        api_key, request.header.api_key, api_version, is_flexible,
        request.header.client_id, request.body.len()
    );

    match api_key {
        ApiKey::ApiVersions => handle_api_versions(request.header.correlation_id, api_version, is_flexible),
        ApiKey::DescribeTopicPartitions => {
            let state = state.read().unwrap();
            handle_describe_topics(request.header.correlation_id, &state, api_version, is_flexible)
        }
        ApiKey::Metadata => {
            let state = state.read().unwrap();
            handle_metadata(request.header.correlation_id, &state, api_version, is_flexible)
        }
        ApiKey::Produce => {
            let mut state = state.write().unwrap();
            handle_produce(request, &mut state)
        }
        ApiKey::Fetch => {
            let state = state.read().unwrap();
            handle_fetch(request, &state)
        }
        ApiKey::ListOffsets => handle_list_offsets(request.header.correlation_id, api_version, is_flexible),
        _ => build_empty_response(request.header.correlation_id),
    }
}
