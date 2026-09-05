//! Kafka network layer: request/response framing and channel state.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/network/` and
//! `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`.

pub mod errors;
pub mod channel_state;
pub mod network_receive;
pub mod byte_buffer_send;
pub mod request_header;
pub mod response_header;
pub mod kafka_request;
pub mod kafka_response;
pub mod connection;
