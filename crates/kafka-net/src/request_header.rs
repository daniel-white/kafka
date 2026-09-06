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
use kafka_protocol::byte_utils::read_unsigned_varint_from_slice;

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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    /// Whether the request used the flexible (v2+) header format.
    /// This determines the response header format — responses must
    /// use the same flexibility as the request.
    pub flexible: bool,
}

impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: impl Into<String>) -> Self {
        let flexible = api_version >= 2;
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id: client_id.into(),
            flexible,
        }
    }

    pub fn size(&self) -> usize {
        if self.flexible {
            2 + 2 + 4 + Self::compact_string_size(&self.client_id) + 1
        } else {
            2 + 2 + 4 + 2 + self.client_id.len()
        }
    }

    pub fn write(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.api_key.to_be_bytes());
        buf.extend_from_slice(&self.api_version.to_be_bytes());
        buf.extend_from_slice(&self.correlation_id.to_be_bytes());
        if self.flexible {
            Self::write_compact_string(buf, &self.client_id);
            buf.push(0); // Empty tagged fields
        } else {
            buf.extend_from_slice(&(self.client_id.len() as i16).to_be_bytes());
            buf.extend_from_slice(self.client_id.as_bytes());
        }
    }

    pub(crate) fn write_compact_string(buf: &mut Vec<u8>, s: &str) {
        write_unsigned_varint_to_buf(buf, (s.len() + 1) as u32);
        buf.extend_from_slice(s.as_bytes());
    }

    pub(crate) fn compact_string_size(s: &str) -> usize {
        varint_size((s.len() + 1) as u32) + s.len()
    }

    /// Read a request header, handling both non-flexible and flexible formats.
    ///
    /// Header flexibility is NOT determined by api_version alone — some clients (like rdkafka)
    /// send flexible headers even when the client_id is written as a STRING (non-compact).
    /// We detect flexibility by examining the raw bytes after the 8-byte fixed header.
    ///
    /// Detection logic after the fixed header:
    /// - Flexible client_id is a COMPACT_STRING with unsigned varint length prefix.
    ///   The varint for actual_len+1 is always >= 1, so the first byte is >= 0x01.
    /// - If the first byte is 0x00, it could be:
    ///   a) Non-flexible: MSB of i16(0..127) string length
    ///   b) Flexible with null compact_string (uvarint 0)
    /// - For case (a), after the STRING we check for a trailing tagged_fields
    ///   byte (uvarint 0). If present AND the api_version supports flexible headers,
    ///   this is actually a librdkafka-style flexible header that writes client_id as STRING.
    ///
    /// MIGRATION_SOURCE:
    ///   clients/src/main/java/org/apache/kafka/common/protocol/Protocol.java
    ///   clients/src/main/java/org/apache/kafka/common/requests/RequestHeader.java
    pub fn read(buf: &[u8]) -> Result<(Self, &[u8]), NetworkError> {
        if buf.len() < 8 {
            return Err(NetworkError::Eof);
        }
        let api_key = i16::from_be_bytes(buf[0..2].try_into().unwrap());
        let api_version = i16::from_be_bytes(buf[2..4].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(buf[4..8].try_into().unwrap());

        let after_fixed = &buf[8..];

        if after_fixed.is_empty() {
            // No client_id data at all — treat as non-flexible with empty client_id
            return Ok((
                RequestHeader {
                    api_key,
                    api_version,
                    correlation_id,
                    client_id: String::new(),
                    flexible: false,
                },
                &[],
            ));
        }

        let is_flexible = after_fixed[0] != 0x00;

        if is_flexible {
            // Flexible header: client_id is compact_string + tagged_fields
            let (header, rest) = Self::read_flexible(after_fixed, api_key, api_version, correlation_id)?;
            Ok((header, rest))
        } else {
            // Could be non-flexible (client_id as STRING, no header tags)
            // or a librdkafka-style flexible header (client_id as STRING + trailing header tags byte)
            let (header, rest) = Self::read_non_flexible(after_fixed, api_key, api_version, correlation_id)?;
            // Check if there are remaining bytes that look like empty tagged_fields (uvarint 0)
            // Only do this check if the api_version supports flexible headers (>= 2 for most APIs,
            // >= 9 for Metadata/Fetch/Produce). Without this check, we'd misinterpret
            // a body byte of 0x00 as a header tags byte.
            let supports_flexible = api_version >= 2;
            if supports_flexible {
                if let Some(byte) = rest.first() {
                    if *byte == 0x00 {
                        // This looks like a trailing tagged_fields byte (empty tags)
                        // librdkafka writes client_id as STRING even in flexible headers
                        return Ok((
                            RequestHeader {
                                api_key,
                                api_version,
                                correlation_id,
                                client_id: header.client_id,
                                flexible: true,
                            },
                            &rest[1..],
                        ));
                    }
                }
            }
            Ok((header, rest))
        }
    }

    /// Read a non-flexible request header (v0/v1): client_id as 2-byte STRING.
    fn read_non_flexible(
        buf: &[u8],
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
    ) -> Result<(Self, &[u8]), NetworkError> {
        if buf.len() < 2 {
            return Err(NetworkError::Eof);
        }
        let client_id_len = i16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
        let rest = &buf[2..];
        if rest.len() < client_id_len {
            return Err(NetworkError::Eof);
        }
        let client_id = String::from_utf8_lossy(&rest[..client_id_len]).into_owned();
        let rest = &rest[client_id_len..];
        Ok((
            RequestHeader {
                api_key,
                api_version,
                correlation_id,
                client_id,
                flexible: false,
            },
            rest,
        ))
    }

    /// Read a flexible request header (v2+): compact_string + tagged_fields.
    fn read_flexible(
        buf: &[u8],
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
    ) -> Result<(Self, &[u8]), NetworkError> {
        let (raw_len, consumed) = read_unsigned_varint_from_slice(buf)?;
        let rest = &buf[consumed..];
        let actual_len = raw_len.saturating_sub(1) as usize;
        if rest.len() < actual_len {
            return Err(NetworkError::Eof);
        }
        let client_id = String::from_utf8_lossy(&rest[..actual_len]).into_owned();
        let rest = &rest[actual_len..];
        // Skip tagged_fields
        let (tag_count, tag_consumed) = read_unsigned_varint_from_slice(rest)?;
        let mut rest = &rest[tag_consumed..];
        for _ in 0..tag_count {
            let (_, tag_bytes) = read_unsigned_varint_from_slice(rest)?;
            rest = &rest[tag_bytes..];
            let (field_size, size_bytes) = read_unsigned_varint_from_slice(rest)?;
            rest = &rest[size_bytes..];
            if rest.len() < field_size as usize {
                return Err(NetworkError::Eof);
            }
            rest = &rest[field_size as usize..];
        }
        Ok((
            RequestHeader {
                api_key,
                api_version,
                correlation_id,
                client_id,
                flexible: true,
            },
            rest,
        ))
    }
}
