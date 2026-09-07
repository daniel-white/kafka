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
use kafka_protocol::kafka_request::KafkaRequest;
use kafka_protocol::response_header::ResponseHeader;
use kafka_protocol::errors::ProtocolError;
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
    
    /// Serialize a response message and wrap it in an [`ApiResponse`].
    ///
    /// The message body is serialized using the request's API version and
    /// flexibility context. The response header flexibility is determined
    /// automatically: ApiVersions always uses a non-flexible header (KIP-511),
    /// while all other APIs follow the request's flexibility.
    pub fn respond_with<W: Writable>(self, msg: W) -> Result<ApiResponse, ProtocolError> {
        // ApiVersions (key 18) must always use a non-flexible response header
        // per KIP-511, regardless of the request's flexibility.
        let is_flexible = self.api_key != ApiKey::ApiVersions && self.is_flexible;
        let msg_ctx = MessageContext::new(self.api_version, is_flexible);
        let mut body = Vec::new();
        msg.write(&mut body, &msg_ctx);
        Ok(ApiResponse {
            correlation_id: self.correlation_id,
            api_key: self.api_key,
            api_version: self.api_version,
            is_flexible,
            body,
        })
    }
}

/// Structured response container: holds the serialized response body
/// along with the metadata needed to build the wire frame.
///
/// Non-generic so all handlers share the same return type,
/// enabling a uniform dispatch table.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
#[derive(Debug, Getters, CopyGetters, CloneGetters)]
pub struct ApiResponse {
    #[getset(get_copy = "pub")]
    correlation_id: i32,
    #[getset(get_copy = "pub")]
    api_key: ApiKey,
    #[getset(get_copy = "pub")]
    api_version: i16,
    #[getset(get_copy = "pub")]
    is_flexible: bool,
    #[getset(get_clone = "pub")]
    body: Vec<u8>,
}

impl ApiResponse {
    /// Build the full wire frame: [4-byte size][response_header][body].
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
    pub fn write_frame(self) -> Vec<u8> {
        let header_size = if self.is_flexible { 5 } else { 4 };
        let response_size = (header_size + self.body.len()) as i32;
        let mut frame = Vec::with_capacity(4 + header_size + self.body.len());
        frame.extend_from_slice(&response_size.to_be_bytes());
        frame.extend_from_slice(&self.correlation_id.to_be_bytes());
        if self.is_flexible {
            frame.push(0x00); // tagged_fields count = 0
        }
        frame.extend_from_slice(&self.body);
        frame
    }
}

pub type ApiHandlerResult = Result<ApiResponse, ProtocolError>;


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

/// Convert a handler result into a wire frame, falling back to an empty
/// response on error.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala
fn to_frame(r: ApiHandlerResult, correlation_id: i32) -> BrokerResponseFrame {
    r.map(|a| a.write_frame()).unwrap_or_else(|_| build_empty_response(correlation_id))
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
    let req = ApiRequest::from_request(request, state);

    let state = req.state.read_atomic();
    let handler = state.api_registry.find_handler(api_key);
    drop(state);

    match handler {
        Some(h) => to_frame(h(req), correlation_id),
        None => build_empty_response(correlation_id),
    }
}
