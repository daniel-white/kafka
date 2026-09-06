//! Serde trait for reading/writing versioned API messages.
//!
//! Mirrors `RecordSerde<T>` from `server-common`.
//!
//! MIGRATION_SOURCE:
//! - server-common/src/main/java/org/apache/kafka/server/common/serialization/RecordSerde.java
//! - server-common/src/main/java/org/apache/kafka/server/common/serialization/AbstractApiMessageSerde.java

use crate::api_message::ApiMessageAndVersion;
use kafka_protocol::{byte_utils, reader::Reader, writer::Writer};

/// Frame version written before the API key.
/// Mirrors `DEFAULT_FRAME_VERSION = 1` in `AbstractApiMessageSerde`.
const DEFAULT_FRAME_VERSION: u32 = 1;

/// Trait for reading/writing versioned API messages to/from byte buffers.
///
/// Uses generic parameter dispatch (no `dyn Trait`) per the no-virtual-dispatch rule.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/serialization/RecordSerde.java
pub trait RecordSerde<T> {
    /// Deserialize a message from the readable buffer.
    ///
    /// `remaining` is the number of bytes left in the enclosing record.
    fn read<R: Reader>(
        &self,
        readable: &mut R,
        remaining: usize,
    ) -> Result<T, crate::MetadataError>;

    /// Compute the size of the framed message in bytes.
    fn record_size(&self, data: &T) -> usize;

    /// Write the framed message to the writable sink.
    ///
    /// Wire format:
    ///   frame_version  unsigned varint (always 1)
    ///   api_key          unsigned varint
    ///   api_version      unsigned varint
    ///   body              message body bytes
    fn write<W: Writer>(&self, data: &T, writable: &mut W);
}

/// Concrete serde for framing `ApiMessageAndVersion` records.
///
/// Wire format:
///   frame_version  unsigned varint = 1
///   api_key          unsigned varint
///   api_version      unsigned varint
///   body              message body bytes
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/serialization/AbstractApiMessageSerde.java
#[derive(Debug, Clone, Default)]
pub struct MetadataRecordSerde;

impl RecordSerde<ApiMessageAndVersion> for MetadataRecordSerde {
    fn read<R: Reader>(
        &self,
        readable: &mut R,
        remaining: usize,
    ) -> Result<ApiMessageAndVersion, crate::MetadataError> {
        let frame_version = readable.read_unsigned_varint()?;
        if frame_version != DEFAULT_FRAME_VERSION {
            return Err(crate::MetadataError::InvalidFrameVersion(frame_version));
        }

        let api_key_unsigned = readable.read_unsigned_varint()?;
        if api_key_unsigned > i16::MAX as u32 {
            return Err(crate::MetadataError::InvalidApiKey(api_key_unsigned as i16));
        }
        let api_key = api_key_unsigned as i16;

        let version_unsigned = readable.read_unsigned_varint()?;
        if version_unsigned > i16::MAX as u32 {
            return Err(crate::MetadataError::Protocol(format!(
                "version {} is too large",
                version_unsigned
            )));
        }
        let version = version_unsigned as i16;

        // Body is the remaining bytes after the frame header.
        let body_len = std::cmp::min(remaining, readable.remaining());
        let data = readable.read_array(body_len)?;

        Ok(ApiMessageAndVersion::new(data, api_key, version))
    }

    fn record_size(&self, data: &ApiMessageAndVersion) -> usize {
        // frame_version (varint) + api_key (varint) + version (varint) + body
        byte_utils::size_of_unsigned_varint(DEFAULT_FRAME_VERSION)
            + byte_utils::size_of_unsigned_varint(data.api_key() as u32)
            + byte_utils::size_of_unsigned_varint(data.version() as u32)
            + data.data().len()
    }

    fn write<W: Writer>(&self, data: &ApiMessageAndVersion, writable: &mut W) {
        writable.write_unsigned_varint(DEFAULT_FRAME_VERSION);
        writable.write_unsigned_varint(data.api_key() as u32);
        writable.write_unsigned_varint(data.version() as u32);
        writable.write_byte_buffer(data.data());
    }
}
