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
pub type BrokerResponseFrame = Vec<u8>;

use crate::server_state::ServerState;
use fast_stm::TVar;
use getset::{CloneGetters, CopyGetters, Getters};
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::response_header::ResponseHeader;
use kafka_protocol::byte_utils;
use kafka_protocol::errors::{Errors, ProtocolError};
use kafka_protocol::io::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::io::reader::Readable;
use kafka_protocol::io::writer::Writable;
use kafka_protocol::message_context::MessageContext;
use kafka_protocol::ApiKey;

/// Common context passed to all request handlers.
///
/// Uses `TVar` for lock-free transactional state access. Read handlers call
/// `state.read_atomic()` for atomic, lock-free reads. Write handlers use
/// `state.write_atomic(new_state)` for atomic replacement.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
#[derive(Debug, Getters, CopyGetters, CloneGetters)]
pub struct ApiRequest {
    #[getset(get_copy = "pub")]
    correlation_id: i32,
    #[getset(get_copy = "pub")]
    api_key: ApiKey,
    #[getset(get_copy = "pub")]
    api_version: i16,
    #[getset(get_copy = "pub")]
    is_flexible: bool,
    body: Box<[u8]>,
    #[getset(get_clone = "pub")]
    state: TVar<ServerState>,
}


impl ApiRequest {
    /// Build a RequestContext from a parsed BrokerRequest.
    pub fn from_request(
        request: BrokerRequest,
        state: TVar<ServerState>,
    ) -> Self {
        ApiRequest {
            correlation_id: request.header.correlation_id,
            api_key: ApiKey::from_id(request.header.api_key),
            api_version: request.header.api_version,
            is_flexible: request.header.flexible,
            body: request.body,
            state,
        }
    }
    
    /// Read a request message of type R from the request body.
    ///
    /// This is the standard way to deserialize request bodies in handlers.
    /// It wraps the raw bytes in a `ByteBufferAccessor`, creates a `Reader`,
    /// and delegates to the `Readable::read` implementation.
    ///
    /// # Errors
    ///
    /// Returns a `ProtocolError` if deserialization fails (e.g., malformed data,
    /// version mismatch, or unexpected EOF).
    pub fn read_msg<R: Readable>(&self) -> Result<R, ProtocolError> {
        let mut buf = ByteBufferAccessor::from_slice(&self.body);
        let ctx = MessageContext::new(self.api_version, self.is_flexible);
        R::read(&mut buf, &ctx)
    }
    
    pub fn send_msg<W: Writable>(self, msg: W) -> ApiResponse<W> {
        ApiResponse {
            correlation_id: self.correlation_id,
            api_key: self.api_key,
            api_version: self.api_version,
            is_flexible: self.is_flexible,
            msg,
            state: self.state
        }
    }
}

#[derive(Debug, Getters, CopyGetters, CloneGetters)]
pub struct ApiResponse<W: Writable> {
    #[getset(get_copy = "pub")]
    correlation_id: i32,
    #[getset(get_copy = "pub")]
    api_key: ApiKey,
    #[getset(get_copy = "pub")]
    api_version: i16,
    #[getset(get_copy = "pub")]
    is_flexible: bool,
    msg: W,
    #[getset(get_clone = "pub")]
    state: TVar<ServerState>,
}

impl <W: Writable> ApiResponse<W> {
    pub fn write_frame(self) -> Result<Vec<u8>, ProtocolError> {
        let header_size = if self.is_flexible { 5 } else { 4 };
        let msg_ctx = MessageContext::new(self.api_version, self.is_flexible);
        let body_size = kafka_protocol::compute_size(&self.msg, &msg_ctx);
        let response_size = (header_size + body_size) as i32;
        let mut frame = Vec::with_capacity(4 + header_size + body_size);
        frame.extend_from_slice(&response_size.to_be_bytes());
        frame.extend_from_slice(&self.correlation_id.to_be_bytes());
        if self.is_flexible {
            byte_utils::write_unsigned_varint(0, &mut frame)?; // tagged_fields
        }
        self.msg.write(&mut frame, &msg_ctx);
        Ok(frame)
    }
}

pub type ApiHandlerResult<W: Writable> = Result<ApiResponse<W>, ProtocolError>;


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

/// Shared helper: build response frame from raw body bytes.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
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

/// Shared helper: build response frame from a Writable message.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
pub fn build_response_frame_with<W: Writable>(
    correlation_id: i32,
    is_flexible: bool,
    api_version: i16,
    msg: W,
) -> Vec<u8> {
    let header_size = if is_flexible { 5 } else { 4 };
    let msg_ctx = MessageContext::new(api_version, is_flexible);
    let body_size = kafka_protocol::compute_size(&msg, &msg_ctx);
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

/// Build an error response frame for the given API key and protocol error.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
fn build_error_response(api_key: ApiKey, correlation_id: i32, is_flexible: bool, api_version: i16, error: ProtocolError) -> Vec<u8> {
    let error_code = match error {
        ProtocolError::UnsupportedVersion => Errors::UnsupportedVersion.code(),
        ProtocolError::Io(_) => Errors::UnknownServerError.code(),
    };
    let header = ResponseHeader::new(correlation_id);
    let body_size = header.size() as i32;
    let mut frame = Vec::with_capacity(4 + header.size());
    frame.extend_from_slice(&body_size.to_be_bytes());
    header.write(&mut frame);
    // Include error_code in the response body
    let mut body = Vec::new();
    body.extend_from_slice(&error_code.to_be_bytes());
    frame.extend_from_slice(&body);
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
) -> BrokerResponseFrame {
    let api_key = ApiKey::from_id(request.header.api_key);
    let correlation_id = request.header.correlation_id;
    let is_flexible = request.header.flexible;
    let api_version = request.header.api_version;
    let req = ApiRequest::from_request(request, state);

    match api_key {
        ApiKey::ApiVersions => {
            match handle_api_versions(req) {
                Ok(res) => res.write_frame().unwrap_or_else(|_| build_empty_response(correlation_id)),
                Err(e) => build_error_response(api_key, correlation_id, is_flexible, api_version, e),
            }
        }
        ApiKey::DescribeTopicPartitions => {
            handle_describe_topics(req)
        }
        ApiKey::Metadata => {
            match handle_metadata(req) {
                Ok(res) => res.write_frame().unwrap_or_else(|_| build_empty_response(correlation_id)),
                Err(e) => build_error_response(api_key, correlation_id, is_flexible, api_version, e),
            }
        }
        ApiKey::Produce => {
            match handle_produce(req) {
                Ok(res) => res.write_frame().unwrap_or_else(|_| build_empty_response(correlation_id)),
                Err(e) => build_error_response(api_key, correlation_id, is_flexible, api_version, e),
            }
        }
        ApiKey::Fetch => {
            handle_fetch(req)
        }
        ApiKey::ListOffsets => handle_list_offsets(req),
        _ => build_empty_response(correlation_id),
    }
}
