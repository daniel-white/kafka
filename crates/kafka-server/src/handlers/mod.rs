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
use kafka_protocol::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::errors::ProtocolError;
use kafka_protocol::message_context::MessageContext;
use kafka_protocol::{Readable, Writable};
use getset::{CloneGetters, CopyGetters, Getters};
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
        let body_size = kafka_protocol::message_body_size(&self.msg, &msg_ctx);
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
    let req = ApiRequest::from_request(request, state);

    // TODO handle error
    match api_key {
        ApiKey::ApiVersions => {
            let res = handle_api_versions(req).unwrap(); 
            res.write_frame().unwrap()
        }
        ApiKey::DescribeTopicPartitions => {
            handle_describe_topics(req)
        }
        ApiKey::Metadata => {
            let res = handle_metadata(req).unwrap();
            res.write_frame().unwrap()
        }
        ApiKey::Produce => {
            let res =  handle_produce(req).unwrap();
            res.write_frame().unwrap()
        }
        ApiKey::Fetch => {
            handle_fetch(req)
        }
        ApiKey::ListOffsets => handle_list_offsets(req),
        _ => build_empty_response(req.correlation_id),
    }
}
