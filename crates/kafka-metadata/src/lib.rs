//! KRaft metadata layer: record types, serde, and versioned message framing.
//!
//! Migrates:
//! - `MetadataRecordSerde` / `AbstractApiMessageSerde` (server/common)
//! - `ApiMessageAndVersion` (server/common)
//! - `RecordSerde<T>` (server/common)
//! - `MetadataRecordType` (message-generated)

pub mod api_message;
pub mod errors;
pub mod record_serde;
pub mod types;

pub use api_message::ApiMessageAndVersion;
pub use errors::MetadataError;
pub use record_serde::{MetadataRecordSerde, RecordSerde};
pub use types::MetadataRecordType;

pub use kafka_protocol::{byte_utils, readable::Readable, writable::Writable};
