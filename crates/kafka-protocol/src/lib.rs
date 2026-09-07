//! Kafka protocol layer: Writable/Readable message traits, Writer/Reader byte I/O traits,
//! MessageContext, ByteBufferAccessor, Errors, Uuid.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

#![deny(clippy::rc_buffer)]
#![deny(clippy::ptr_arg)]

pub mod api_key;
pub mod api_versions_response;
pub mod byte_buffer_accessor;
pub mod byte_utils;
pub mod errors;
pub mod message;
pub mod message_context;
pub mod metadata_response;
pub mod produce_response;
pub mod raw_tagged_field;
pub mod reader;
pub mod request_types;
pub mod tagged_fields;
pub mod types;
pub mod writer;

pub use api_key::ApiKey;
pub use api_versions_response::{ApiVersionsRequest, ApiVersionsResponse, ApiVersionEntry};
pub use message::{message_body_size, Readable, Writable};
pub use message_context::{HeaderContext, MessageContext};
pub use metadata_response::{MetadataRequest, MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
pub use produce_response::{ProduceRequest, PartitionProduceResponse, ProduceResponse, TopicProduceResponse};
pub use reader::Reader;
pub use kafka_common::uuid;
pub use raw_tagged_field::RawTaggedField;
pub use request_types::{DescribeTopicsRequest, FetchRequest, ListOffsetsRequest};
pub use writer::{SizeCounter, Writer};