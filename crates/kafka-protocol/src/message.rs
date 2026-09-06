//! Writable and Readable traits for protocol messages.
//!
//! Each Kafka message struct implements these traits to serialize/deserialize
//! itself according to the version-specific wire format defined in the JSON specs.
//!
//! Uses generic parameter dispatch (no `dyn Trait`) per the no-virtual-dispatch rule.
//!
//! The `MessageContext` carries the API version and header flexibility, allowing
//! each field to be conditionally serialized based on its `versions` annotation.
//!
//! To compute the size of a message body, use `SizeCounter` (which implements `Writer`
//! but doesn't store data): create one, call `write()` on it, then read `size()`.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ApiMessage.java

use crate::reader::Reader;
use crate::writer::{SizeCounter, Writer};
use crate::MessageContext;

/// Compute the serialized body size of a `Writable` message by writing to a
/// `SizeCounter` (which counts bytes without storing them).
///
/// This is the standard way to get a message's body size without duplicating
/// the field-counting logic.
pub fn message_body_size(msg: &impl Writable, ctx: &MessageContext) -> usize {
    let mut counter = SizeCounter::new();
    msg.write(&mut counter, ctx);
    counter.size()
}

/// Trait for writing a protocol message to a byte sink.
///
/// Implementations must respect the version annotations from the JSON specs:
/// only write fields whose `versions` range includes `ctx.api_version()`.
///
/// Body flexibility (compact strings, varint array counts) is determined within
/// `write` by checking `ctx.api_version()` against the message's
/// `flexibleVersions` threshold from the JSON spec.
///
/// `ctx.is_flexible()` only controls the response *header* format (tagged fields
/// after the correlation_id).
pub trait Writable {
    /// Write the message body to `w`.
    ///
    /// `ctx.api_version()` determines which fields are present and whether
    /// flexible (compact) encoding is used.
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext);
}

/// Trait for reading a protocol message from a byte source.
///
/// Implementations parse the body according to the version-specific wire format.
///
/// The `MessageContext` carries the API version, allowing each field to be
/// conditionally read based on its `versions` annotation.
pub trait Readable: Sized {
    /// Read the message body from `r`.
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, crate::errors::ProtocolError>;
}
