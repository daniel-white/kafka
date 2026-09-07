//! Kafka protocol layer: Writable/Readable message traits, Writer/Reader byte I/O traits,
//! MessageContext, ByteBufferAccessor, Errors, Uuid, and request/response headers.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

#![deny(clippy::rc_buffer)]
#![deny(clippy::ptr_arg)]

pub mod api_key;
pub mod byte_utils;
pub mod errors;
pub mod io;
pub mod messages;
pub mod message_context;
pub mod request_header;
pub mod response_header;
pub mod kafka_request;
pub mod kafka_response;
pub mod network_receive;
pub mod byte_buffer_send;
pub mod channel_state;
pub mod connection;

pub use api_key::ApiKey;
pub use byte_utils::*;
pub use messages::api_versions::{ApiVersionsRequest, ApiVersionsResponse, ApiVersionEntry};
pub use messages::describe_topics::{DescribeTopicsRequest, DescribeTopicsResponse, DescribeTopicsTopic, DescribeTopicsPartition};
pub use messages::fetch::FetchRequest;
pub use io::sizing::compute_size;
pub use messages::list_offsets::ListOffsetsRequest;
pub use message_context::{HeaderContext, MessageContext};
pub use messages::metadata::{MetadataRequest, MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
pub use messages::produce::{ProduceRequest, PartitionProduceResponse, ProduceResponse, TopicProduceResponse};
pub use io::reader::Reader;
pub use kafka_common::uuid;
pub use messages::tagged_fields::RawTaggedField;
pub use io::writer::Writer;
pub use io::byte_buffer_accessor::ByteBufferAccessor;
pub use request_header::RequestHeader;
pub use response_header::ResponseHeader;
pub use kafka_request::KafkaRequest;
pub use kafka_response::KafkaResponse;
pub use network_receive::{NetworkReceive, SIZE_HEADER_SIZE};
pub use byte_buffer_send::ByteBufferSend;
pub use channel_state::ChannelState;
pub use connection::{ClientRequest, ClientResponse, InFlightRequest, InFlightRequests, RequestCompletionHandler};