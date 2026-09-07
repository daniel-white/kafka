//! DefaultRecord: on-disk record format for magic 2+.
//!
//! Record =>
//!   Length => Varint
//!   Attributes => Int8
//!   TimestampDelta => Varlong
//!   OffsetDelta => Varint
//!   KeyLength => Varint
//!   Key => Bytes
//!   ValueLength => Varint
//!   Value => Bytes
//!   HeadersCount => Varint
//!   Headers => [HeaderKey HeaderValue]
//!     HeaderKeyLength => Varint
//!     HeaderKey => String
//!     HeaderValueLength => Varint
//!     HeaderValue => Bytes
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecord.java`.

use crate::errors::RecordError;
use crate::header::Header;
use crate::record_batch::{
    MAGIC_VALUE_V2, MAX_RECORD_OVERHEAD, NO_SEQUENCE, NULL_VARINT_SIZE_BYTES,
};
use kafka_errors::KafkaError;
use kafka_protocol::byte_utils::{size_of_varint, size_of_varlong};
use kafka_protocol::io::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::io::reader::Reader;
use kafka_protocol::io::writer::Writer;

/// A single record in the magic v2+ format.
///
/// In Java each field is `private final` and accessed via explicit getters;
/// here fields are private and accessed via `getset`-generated methods.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecord.java
pub struct DefaultRecord {
    size_in_bytes: i32,
    attributes: u8,
    offset: i64,
    timestamp: i64,
    sequence: i32,
    key: Option<Box<[u8]>>,
    value: Option<Box<[u8]>>,
    headers: Vec<Header>,
}

impl DefaultRecord {
    /// Maximum overhead excluding key, value and headers.
    /// Mirrors `DefaultRecord.MAX_RECORD_OVERHEAD`.
    pub const MAX_RECORD_OVERHEAD: i32 = MAX_RECORD_OVERHEAD;

    /// Create a record with the given fields and pre-computed `size_in_bytes`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        size_in_bytes: i32,
        attributes: u8,
        offset: i64,
        timestamp: i64,
        sequence: i32,
        key: Option<Box<[u8]>>,
        value: Option<Box<[u8]>>,
        headers: Vec<Header>,
    ) -> Self {
        DefaultRecord {
            size_in_bytes,
            attributes,
            offset,
            timestamp,
            sequence,
            key,
            value,
            headers,
        }
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn sequence(&self) -> i32 {
        self.sequence
    }

    pub fn size_in_bytes(&self) -> i32 {
        self.size_in_bytes
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn attributes(&self) -> u8 {
        self.attributes
    }

    pub fn key_size(&self) -> i32 {
        match &self.key {
            Some(k) => k.len() as i32,
            None => -1,
        }
    }

    pub fn value_size(&self) -> i32 {
        match &self.value {
            Some(v) => v.len() as i32,
            None => -1,
        }
    }

    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    pub fn ensure_valid(&self) {}

    pub fn has_magic(&self, magic: i8) -> bool {
        magic >= MAGIC_VALUE_V2
    }

    pub fn is_compressed(&self) -> bool {
        false
    }

    pub fn has_timestamp_type(
        &self,
        _timestamp_type: crate::timestamp_type::TimestampType,
    ) -> bool {
        false
    }

    /// Read a record from `readable`.
    ///
    /// Mirrors `DefaultRecord.readFrom(ByteBuffer, baseOffset, baseTimestamp, baseSequence, logAppendTime)`.
    pub fn read_from<R: Reader>(
        readable: &mut R,
        base_offset: i64,
        base_timestamp: i64,
        base_sequence: i32,
        log_append_time: Option<i64>,
    ) -> Result<Self, RecordError> {
        let size_of_body = readable.read_varint()?;

        if size_of_body < 0 {
            return Err(RecordError::InvalidRecordSize {
                expected: 0,
                actual: size_of_body as usize,
            });
        }

        let body = readable.read_bytes_array(size_of_body as usize)?;

        let mut accessor = ByteBufferAccessor::from_bytes(body);

        let record_start = accessor.position();
        let attributes = accessor.read_byte()?;
        let timestamp_delta = accessor.read_varlong()?;
        let timestamp = base_timestamp + timestamp_delta;
        let timestamp = log_append_time.unwrap_or(timestamp);

        let offset_delta = accessor.read_varint()?;
        let offset = base_offset + offset_delta as i64;
        let sequence = if base_sequence >= 0 {
            Self::increment_sequence(base_sequence, offset_delta)
        } else {
            NO_SEQUENCE
        };

        // read key
        let key_size = accessor.read_varint()?;
        let key = if key_size < 0 {
            None
        } else {
            Some(accessor.read_bytes_array(key_size as usize)?.into_boxed_slice())
        };

        // read value
        let value_size = accessor.read_varint()?;
        let value = if value_size < 0 {
            None
        } else {
            Some(accessor.read_bytes_array(value_size as usize)?.into_boxed_slice())
        };

        // read headers
        let num_headers = accessor.read_varint()?;
        if num_headers < 0 {
            return Err(RecordError::InvalidHeaderCount(num_headers));
        }

        let headers = Self::read_headers(&mut accessor, num_headers)?;

        // validate we read all body bytes
        if accessor.position() - record_start != size_of_body as usize {
            return Err(RecordError::Kafka(KafkaError::InvalidRecord {
                message: format!(
                    "Invalid record size: expected to read {} bytes, but read {}",
                    size_of_body,
                    accessor.position() - record_start
                ),
            }));
        }

        let total_size = size_of_varint(size_of_body) + size_of_body as usize;

        Ok(DefaultRecord::new(
            total_size as i32,
            attributes,
            offset,
            timestamp,
            sequence,
            key,
            value,
            headers,
        ))
    }

    fn read_headers<R: Reader>(
        readable: &mut R,
        num_headers: i32,
    ) -> Result<Vec<Header>, RecordError> {
        let mut headers = Vec::with_capacity(num_headers as usize);
        for _ in 0..num_headers {
            let header_key_size = readable.read_varint()?;
            if header_key_size < 0 {
                return Err(RecordError::InvalidHeaderKeySize(header_key_size));
            }
            let key_bytes = readable.read_bytes_array(header_key_size as usize)?;
            let key = String::from_utf8(key_bytes).map_err(|e| {
                RecordError::Kafka(KafkaError::InvalidRecord {
                    message: format!("Invalid UTF-8 in header key: {}", e),
                })
            })?;

            let header_value_size = readable.read_varint()?;
            let value = if header_value_size < 0 {
                None
            } else {
                Some(readable.read_bytes_array(header_value_size as usize)?)
            };

            headers.push(Header::new(key, value));
        }
        Ok(headers)
    }

    /// Write the record to `writable` and return the total size in bytes written.
    ///
    /// Mirrors `DefaultRecord.writeTo(DataOutputStream, ...)`.
    pub fn write_to<W: Writer>(
        writable: &mut W,
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header],
    ) -> i32 {
        let body_size = Self::size_of_body_in_bytes(offset_delta, timestamp_delta, key, value, headers);

        // Write varint size prefix
        writable.write_varint(body_size);

        // Attributes (always 0 for magic 2+)
        writable.write_byte(0);

        // Timestamp delta
        writable.write_varlong(timestamp_delta);

        // Offset delta
        writable.write_varint(offset_delta);

        // Key
        match key {
            None => writable.write_varint(-1),
            Some(k) => {
                writable.write_varint(k.len() as i32);
                writable.write_bytes(k);
            }
        }

        // Value
        match value {
            None => writable.write_varint(-1),
            Some(v) => {
                writable.write_varint(v.len() as i32);
                writable.write_bytes(v);
            }
        }

        // Headers
        writable.write_varint(headers.len() as i32);
        for header in headers {
            let key_bytes = header.key().as_bytes();
            writable.write_varint(key_bytes.len() as i32);
            writable.write_bytes(key_bytes);

            match header.value() {
                None => writable.write_varint(-1),
                Some(v) => {
                    writable.write_varint(v.len() as i32);
                    writable.write_bytes(v);
                }
            }
        }

        size_of_varint(body_size) as i32 + body_size
    }

    /// Compute the total size in bytes of a record with the given fields.
    ///
    /// Mirrors `DefaultRecord.sizeInBytes(...)`.
    pub fn size_in_bytes_for(
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header],
    ) -> i32 {
        let body_size = Self::size_of_body_in_bytes(offset_delta, timestamp_delta, key, value, headers);
        (size_of_varint(body_size) + body_size as usize) as i32
    }

    fn size_of_body_in_bytes(
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header],
    ) -> i32 {
        let key_size = key.map(|k| k.len() as i32).unwrap_or(-1);
        let value_size = value.map(|v| v.len() as i32).unwrap_or(-1);
        Self::size_of_body_in_bytes_with_sizes(offset_delta, timestamp_delta, key_size, value_size, headers)
    }

    fn size_of_body_in_bytes_with_sizes(
        offset_delta: i32,
        timestamp_delta: i64,
        key_size: i32,
        value_size: i32,
        headers: &[Header],
    ) -> i32 {
        let mut size = 0i32;
        size += 1; // attributes byte
        size += size_of_varint(offset_delta) as i32;
        size += size_of_varlong(timestamp_delta) as i32;
        size += Self::size_of(key_size, value_size, headers);
        size
    }

    fn size_of(key_size: i32, value_size: i32, headers: &[Header]) -> i32 {
        let null_varint = NULL_VARINT_SIZE_BYTES as i32;
        let mut size = 0i32;

        if key_size < 0 {
            size += null_varint;
        } else {
            size += size_of_varint(key_size) as i32 + key_size;
        }

        if value_size < 0 {
            size += null_varint;
        } else {
            size += size_of_varint(value_size) as i32 + value_size;
        }

        size += size_of_varint(headers.len() as i32) as i32;
        for header in headers {
            let key_bytes = header.key().as_bytes();
            let key_len = key_bytes.len() as i32;
            size += size_of_varint(key_len) as i32 + key_len;

            let header_value_size = header.value().map(|v| v.len() as i32).unwrap_or(-1);
            if header_value_size < 0 {
                size += null_varint;
            } else {
                size += size_of_varint(header_value_size) as i32 + header_value_size;
            }
        }

        size
    }

    pub fn record_size_upper_bound(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[Header]) -> i32 {
        let key_size = key.map(|k| k.len() as i32).unwrap_or(-1);
        let value_size = value.map(|v| v.len() as i32).unwrap_or(-1);
        Self::MAX_RECORD_OVERHEAD + Self::size_of(key_size, value_size, headers)
    }

    pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
        if sequence == NO_SEQUENCE {
            return NO_SEQUENCE;
        }
        sequence.wrapping_add(increment)
    }

    pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
        if sequence == NO_SEQUENCE {
            return NO_SEQUENCE;
        }
        sequence.wrapping_sub(decrement)
    }
}
