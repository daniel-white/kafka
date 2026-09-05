//! Request header serialization.
//!
//! Mirrors `org.apache.kafka.common.requests.RequestHeader` / `RequestHeaderData`.
//!
//! Wire format (v0/v1, non-flexible):
//!   api_key       int16
//!   api_version   int16
//!   correlation_id int32
//!   client_id     string  (2-byte length prefix + UTF-8 bytes)
//!
//! Wire format (v2+, flexible):
//!   api_key       int16
//!   api_version   int16
//!   correlation_id int32
//!   client_id     compact_string  (unsigned varint length prefix + UTF-8 bytes)
//!   tagged_fields tagged_fields

use crate::errors::NetworkError;

/// Write an unsigned varint to a byte buffer.
fn write_unsigned_varint_to_buf(buf: &mut Vec<u8>, value: u32) {
    let mut v = value;
    while (v & !0x7F) != 0 {
        buf.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Size in bytes of an unsigned varint encoding.
fn varint_size(value: u32) -> usize {
    let mut v = value;
    let mut size = 1;
    while v >= 0x80 {
        v >>= 7;
        size += 1;
    }
    size
}

/// Read an unsigned varint from a byte slice, returning the value and bytes consumed.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
fn read_unsigned_varint(buf: &[u8]) -> Result<(u32, usize), NetworkError> {
    let mut result = 0u32;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i >= 5 {
            return Err(NetworkError::Eof);
        }
        result |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(NetworkError::Eof)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: impl Into<String>) -> Self {
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id: client_id.into(),
        }
    }

    pub fn size(&self) -> usize {
        if self.api_version >= 2 {
            // Flexible: 2+2+4 + compact_string + tagged_fields
            2 + 2 + 4 + Self::compact_string_size(&self.client_id) + 1 // 1 byte for empty tagged_fields
        } else {
            // Non-flexible: 2+2+4 + 2-byte string
            2 + 2 + 4 + 2 + self.client_id.len()
        }
    }

    /// Write the header in the appropriate format (flexible for v2+, non-flexible otherwise).
    ///
    /// MIGRATION_SOURCE:
    ///   clients/src/main/java/org/apache/kafka/common/protocol/Protocol.java
    pub fn write(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.api_key.to_be_bytes());
        buf.extend_from_slice(&self.api_version.to_be_bytes());
        buf.extend_from_slice(&self.correlation_id.to_be_bytes());
        if self.api_version >= 2 {
            // Flexible: compact string for client_id + tagged_fields
            Self::write_compact_string(buf, &self.client_id);
            // Empty tagged fields
            buf.push(0);
        } else {
            // Non-flexible: 2-byte length-prefixed string
            buf.extend_from_slice(&(self.client_id.len() as i16).to_be_bytes());
            buf.extend_from_slice(self.client_id.as_bytes());
        }
    }

    /// Write a compact string (varint length prefix + bytes).
    pub(crate) fn write_compact_string(buf: &mut Vec<u8>, s: &str) {
        write_unsigned_varint_to_buf(buf, (s.len() + 1) as u32);
        buf.extend_from_slice(s.as_bytes());
    }

    /// Size in bytes of a compact string.
    pub(crate) fn compact_string_size(s: &str) -> usize {
        varint_size((s.len() + 1) as u32) + s.len()
    }

    /// Read a RequestHeader, handling both non-flexible (v0/v1) and flexible (v2+) headers.
    ///
    /// The header format depends on `api_version`:
    /// - v0/v1 (non-flexible): client_id is a 2-byte length-prefixed STRING
    /// - v2+ (flexible): client_id is a compact_string (varint prefix) + tagged_fields
    ///
    /// MIGRATION_SOURCE:
    ///   clients/src/main/java/org/apache/kafka/common/protocol/Protocol.java
    pub fn read(buf: &mut &[u8]) -> Result<Self, NetworkError> {
        if buf.len() < 8 {
            return Err(NetworkError::Eof);
        }
        let api_key = i16::from_be_bytes(buf[0..2].try_into().unwrap());
        let api_version = i16::from_be_bytes(buf[2..4].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(buf[4..8].try_into().unwrap());
        *buf = &buf[8..];

        if api_version >= 2 {
            // Flexible header: compact_string + tagged_fields
            let (raw_len, consumed) = read_unsigned_varint(buf)?;
            *buf = &buf[consumed..];
            let actual_len = raw_len.saturating_sub(1) as usize;
            if buf.len() < actual_len {
                return Err(NetworkError::Eof);
            }
            let client_id = String::from_utf8_lossy(&buf[..actual_len]).into_owned();
            *buf = &buf[actual_len..];
            // Skip tagged_fields
            let (tag_count, tag_consumed) = read_unsigned_varint(buf)?;
            *buf = &buf[tag_consumed..];
            for _ in 0..tag_count {
                let (_, tag_bytes) = read_unsigned_varint(buf)?;
                *buf = &buf[tag_bytes..];
                let (field_size, size_bytes) = read_unsigned_varint(buf)?;
                *buf = &buf[size_bytes..];
                if buf.len() < field_size as usize {
                    return Err(NetworkError::Eof);
                }
                *buf = &buf[field_size as usize..];
            }
            Ok(RequestHeader {
                api_key,
                api_version,
                correlation_id,
                client_id,
            })
        } else {
            // Non-flexible header: 2-byte length-prefixed STRING
            if buf.len() < 2 {
                return Err(NetworkError::Eof);
            }
            let client_id_len = i16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
            *buf = &buf[2..];
            if buf.len() < client_id_len {
                return Err(NetworkError::Eof);
            }
            let client_id = String::from_utf8_lossy(&buf[..client_id_len]).into_owned();
            *buf = &buf[client_id_len..];
            Ok(RequestHeader {
                api_key,
                api_version,
                correlation_id,
                client_id,
            })
        }
    }
}
