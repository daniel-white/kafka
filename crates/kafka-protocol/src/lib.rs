//! Kafka protocol layer: Readable/Writable traits, ByteBufferAccessor, Errors, Uuid.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

#![deny(clippy::rc_buffer)]
#![deny(clippy::ptr_arg)]

pub mod byte_buffer_accessor;
pub mod byte_utils;
pub mod errors;
pub mod raw_tagged_field;
pub mod readable;
pub mod tagged_fields;
pub mod types;
pub mod writable;

pub use kafka_common::uuid;
pub use raw_tagged_field::RawTaggedField;
