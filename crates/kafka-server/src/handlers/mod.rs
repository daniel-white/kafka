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

/// Type alias for a parsed Kafka protocol request received by the broker.
/// Renamed from KafkaRequest for clarity at the server layer.
pub type BrokerRequest = KafkaRequest;

use crate::server_state::ServerState;
use fast_stm::TVar;
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::response_header::ResponseHeader;
use kafka_protocol::byte_utils;
use kafka_protocol::ApiKey;

/// Common context passed to all request handlers.
///
/// Uses `TVar` for lock-free transactional state access. Read handlers call
/// `state.read_atomic()` for atomic, lock-free reads. Write handlers use
/// `state.write_atomic(new_state)` for atomic replacement.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
#[derive(Debug)]
pub struct RequestContext {
    pub correlation_id: i32,
    pub api_version: i16,
    pub is_flexible: bool,
    pub body: Vec<u8>,
    pub state: TVar<ServerState>,
}

impl RequestContext {
    /// Build a RequestContext from a parsed BrokerRequest.
    pub fn from_request(
        request: &BrokerRequest,
        state: TVar<ServerState>,
    ) -> Self {
        RequestContext {
            correlation_id: request.header.correlation_id,
            api_version: request.header.api_version,
            is_flexible: request.header.flexible,
            body: request.body.to_vec(),
            state,
        }
    }
}

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

/// Generic `build_response_frame` that takes a `Writable` by value and writes
/// it directly, pre-sizing the frame buffer (single allocation, no intermediate
/// Vec for body). The `override_flexible` parameter allows handlers to control
/// the response header format (e.g., ApiVersions always uses non-flexible
/// per KIP-511, regardless of the request's flexibility).
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/AbstractResponse.java
pub fn build_response_frame_with<W: kafka_protocol::Writable>(
    correlation_id: i32,
    is_flexible: bool,
    api_version: i16,
    msg: W,
) -> Vec<u8> {
    let header_size = if is_flexible { 5 } else { 4 };
    let msg_ctx = kafka_protocol::MessageContext::new(api_version, is_flexible);
    let body_size = kafka_protocol::message_body_size(&msg, &msg_ctx);
    let response_size = (header_size + body_size) as i32;
    let mut frame = Vec::with_capacity(4 + header_size + body_size);
    frame.extend_from_slice(&response_size.to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    if is_flexible {
        byte_utils::write_unsigned_varint(0, &mut frame).unwrap(); // tagged_fields
    }
    msg.write(&mut frame, &msg_ctx);
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
    request: BrokerRequest,
    state: TVar<ServerState>,
) -> Vec<u8> {
    let api_key = ApiKey::from_id(request.header.api_key);
    let ctx = RequestContext::from_request(&request, state);

    eprintln!(
        "dispatch: api_key={:?} (id={}), api_version={}, is_flexible={}, client_id={:?}, body_len={}",
        api_key, request.header.api_key, ctx.api_version, ctx.is_flexible,
        request.header.client_id, request.body.len()
    );

    match api_key {
        ApiKey::ApiVersions => {
            handle_api_versions(ctx)
        }
        ApiKey::DescribeTopicPartitions => {
            handle_describe_topics(&ctx)
        }
        ApiKey::Metadata => {
            handle_metadata(ctx)
        }
        ApiKey::Produce => {
            handle_produce(ctx)
        }
        ApiKey::Fetch => {
            handle_fetch(ctx)
        }
        ApiKey::ListOffsets => handle_list_offsets(ctx.correlation_id, ctx.api_version, ctx.is_flexible),
        _ => build_empty_response(ctx.correlation_id),
    }
}
