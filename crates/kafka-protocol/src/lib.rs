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
pub mod tagged_fields;
pub mod types;
pub mod writer;

pub use api_key::ApiKey;
pub use message::{message_body_size, Readable, Writable};
pub use message_context::{HeaderContext, MessageContext};
pub use metadata_response::{MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
pub use produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse};
pub use reader::Reader;
pub use kafka_common::uuid;
pub use raw_tagged_field::RawTaggedField;
pub use writer::{SizeCounter, Writer};
pub use api_versions_response::{ApiVersionsResponse, ApiVersionEntry};