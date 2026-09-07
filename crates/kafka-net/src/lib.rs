//! kafka-net — re-export shim.
//!
//! All protocol-level types (RequestHeader, ResponseHeader, KafkaRequest,
//! KafkaResponse, NetworkReceive, ByteBufferSend, ChannelState, Connection)
//! live in `kafka-protocol`. This crate exists only for backwards compatibility
//! with code that imports via `kafka_net::`.
//!
//! MIGRATION_SOURCE: (shim)

pub use kafka_protocol::{
    byte_buffer_send::ByteBufferSend,
    byte_buffer_send::SIZE_HEADER_SIZE,
    channel_state::ChannelState,
    connection::{ClientRequest, ClientResponse, InFlightRequest, InFlightRequests, RequestCompletionHandler},
    errors::NetworkError,
    kafka_request::KafkaRequest,
    kafka_response::KafkaResponse,
    network_receive::NetworkReceive,
    request_header::RequestHeader,
    response_header::ResponseHeader,
};

// Re-export under old submodule paths for backwards compatibility.
pub mod byte_buffer_send {
    pub use kafka_protocol::byte_buffer_send::*;
}
pub mod channel_state {
    pub use kafka_protocol::channel_state::*;
}
pub mod connection {
    pub use kafka_protocol::connection::*;
}
pub mod errors {
    pub use kafka_protocol::errors::NetworkError;
}
pub mod kafka_request {
    pub use kafka_protocol::kafka_request::*;
}
pub mod kafka_response {
    pub use kafka_protocol::kafka_response::*;
}
pub mod network_receive {
    pub use kafka_protocol::network_receive::*;
}
pub mod request_header {
    pub use kafka_protocol::request_header::*;
}
pub mod response_header {
    pub use kafka_protocol::response_header::*;
}