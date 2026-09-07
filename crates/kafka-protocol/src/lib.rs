//! Kafka protocol layer: Writable/Readable message traits, Writer/Reader byte I/O traits,
//! MessageContext, ByteBufferAccessor, Errors, Uuid.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

#![deny(clippy::rc_buffer)]
#![deny(clippy::ptr_arg)]

pub mod api_key;
pub mod byte_utils;
pub mod describe_topics;
pub mod errors;
pub mod fetch;
pub mod list_offsets;
pub mod message_context;
pub mod metadata;
pub mod produce;
pub mod types;
pub mod io;
pub mod messages;

pub use api_key::ApiKey;
pub use messages::api_versions::{ApiVersionsRequest, ApiVersionsResponse, ApiVersionEntry};
pub use describe_topics::{DescribeTopicsRequest, DescribeTopicsResponse, DescribeTopicsTopic, DescribeTopicsPartition};
pub use fetch::FetchRequest;
pub use io::sizing::compute_size;
pub use list_offsets::ListOffsetsRequest;
pub use message_context::{HeaderContext, MessageContext};
pub use metadata::{MetadataRequest, MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
pub use produce::{ProduceRequest, PartitionProduceResponse, ProduceResponse, TopicProduceResponse};
pub use io::reader::Reader;
pub use kafka_common::uuid;
pub use messages::tagged_fields::RawTaggedField;
pub use io::writer::Writer;
pub use io::byte_buffer_accessor::ByteBufferAccessor;