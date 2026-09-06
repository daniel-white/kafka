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
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ApiMessage.java

use crate::reader::Reader;
use crate::writer::Writer;
use crate::MessageContext;

/// Trait for writing a protocol message to a byte sink.
///
/// Implementations must respect the version annotations from the JSON specs:
/// only write fields whose `versions` range includes `ctx.api_version()`.
///
/// Body flexibility (compact strings, varint array counts) is determined by
/// `self.is_flexible_body(ctx)`, which should match the message's own
/// `flexibleVersions` from the JSON spec.
///
/// `ctx.is_flexible()` only controls the response *header* format (tagged fields
/// after the correlation_id).
pub trait Writable {
    /// Write the message body to `w`.
    ///
    /// `ctx.api_version()` determines which fields are present.
    /// `self.is_flexible_body(ctx)` determines whether to use compact encoding.
    fn write_body<W: Writer>(&self, w: &mut W, ctx: &MessageContext);

    /// Compute the encoded size of the message body.
    fn body_size(&self, ctx: &MessageContext) -> usize;

    /// Whether the body uses flexible (compact) encoding for this version.
    ///
    /// This should match the message's `flexibleVersions` from the JSON spec.
    /// For example, MetadataResponse has `flexibleVersions: "9+"`, so this
    /// returns true when `api_version >= 9`.
    fn is_flexible_body(&self, ctx: &MessageContext) -> bool {
        // Default: check a version threshold. Override if more complex logic is needed.
        ctx.api_version() >= self.flexible_body_start_version()
    }

    /// The first API version that uses flexible body encoding.
    /// Override this to return the actual flexible version start.
    fn flexible_body_start_version(&self) -> i16 {
        i16::MAX
    }

    /// Write the top-level tagged_fields for the end of a flexible body.
    ///
    /// Most messages just write a single uvarint(0) for "no tagged fields".
    /// Override if the message has custom top-level tagged fields.
    fn write_top_level_tags<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        if self.is_flexible_body(ctx) {
            w.write_unsigned_varint(0);
        }
    }
}

/// Trait for reading a protocol message from a byte source.
///
/// Implementations parse the body according to the version-specific wire format.
///
/// The `MessageContext` carries the API version, allowing each field to be
/// conditionally read based on its `versions` annotation.
pub trait Readable: Sized {
    /// Read the message body from `r`.
    fn read_body<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, crate::errors::ProtocolError>;
}
