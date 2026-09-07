//! Writer trait mirroring Java's binary I/O write interface.
//!
//! Includes default methods for Kafka protocol types: STRING, COMPACT_STRING,
//! NULLABLE_STRING, COMPACT_NULLABLE_STRING, and array counts.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java

use crate::MessageContext;
use kafka_common::uuid::Uuid;


/// Trait for writing primitive types to a byte sink, mirroring Java's `Writer`.
///
/// Uses generic parameter dispatch (no `dyn Trait`) per the no-virtual-dispatch rule.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
pub trait Writer {
    fn write_byte(&mut self, val: u8);
    fn write_short(&mut self, val: i16);
    fn write_int(&mut self, val: i32);
    fn write_long(&mut self, val: i64);
    fn write_bytes(&mut self, arr: &[u8]);

    /// Write a string as UTF-8 bytes.
    fn write_byte_array_string(&mut self, s: &str) {
        self.write_bytes(s.as_bytes());
    }

    /// Write a STRING: int16 length prefix + UTF-8 bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_string(&mut self, s: &str) {
        self.write_short(s.len() as i16);
        self.write_bytes(s.as_bytes());
    }

    /// Write a COMPACT_STRING: unsigned varint length prefix (len+1) + UTF-8 bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_compact_string(&mut self, s: &str) {
        self.write_unsigned_varint(s.len() as u32 + 1);
        self.write_bytes(s.as_bytes());
    }

    /// Write a NULLABLE_STRING: int16 length prefix (-1 for null).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.write_string(s),
            None => self.write_short(-1),
        }
    }

    /// Write a NULLABLE_BYTES: int32 length prefix (-1 for null).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_nullable_bytes(&mut self, b: Option<&[u8]>) {
        match b {
            Some(b) => {
                self.write_int(b.len() as i32);
                self.write_bytes(b);
            }
            None => self.write_int(-1),
        }
    }

    /// Write a COMPACT_NULLABLE_STRING: unsigned varint (0 for null).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_compact_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.write_compact_string(s),
            None => self.write_unsigned_varint(0),
        }
    }

    /// Write a COMPACT_NULLABLE_BYTES: unsigned varint (0 for null).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_compact_nullable_bytes(&mut self, b: Option<&[u8]>) {
        match b {
            Some(b) => {
                self.write_unsigned_varint(b.len() as u32 + 1);
                self.write_bytes(b);
            }
            None => self.write_unsigned_varint(0),
        }
    }

    /// Write an ARRAY count (int32) or COMPACT_ARRAY count (varint, count+1).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_array_count(&mut self, count: usize, flexible: bool) {
        if flexible {
            self.write_unsigned_varint(count as u32 + 1);
        } else {
            self.write_int(count as i32);
        }
    }

    /// Write a UUID (most significant bits first).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_uuid(&mut self, uuid: &Uuid) {
        self.write_long(uuid.get_most_significant_bits());
        self.write_long(uuid.get_least_significant_bits());
    }

    /// Write a boolean value as a single byte (0 for false, 1 for true).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_boolean(&mut self, val: bool) {
        self.write_byte(if val { 1 } else { 0 });
    }

    /// Write an unsigned short (2 bytes, big-endian).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_unsigned_short(&mut self, val: u16) {
        self.write_short(val as i16);
    }

    /// Write an unsigned int (4 bytes, big-endian).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_unsigned_int(&mut self, val: u32) {
        self.write_int(val as i32);
    }

    /// Write a double (IEEE 754 big-endian) to a byte stream.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
    fn write_double(&mut self, value: f64) {
        self.write_bytes(&value.to_be_bytes());
    }

    /// Write an unsigned int in variable-length (protobuf) format.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
    fn write_unsigned_varint(&mut self, value: u32) {
        let mut v = value;
        while (v & !0x7F) != 0 {
            self.write_byte((v as u8 & 0x7F) | 0x80);
            v >>= 7;
        }
        self.write_byte(v as u8);
    }

    /// Write a zig-zag encoded signed int.
    fn write_varint(&mut self, value: i32) {
        let zigzag = ((value << 1) ^ (value >> 31)) as u32;
        self.write_unsigned_varint(zigzag);
    }

    /// Write an unsigned long in variable-length format.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
    fn write_unsigned_varlong(&mut self, value: u64) {
        let mut v = value;
        while (v & !0x7F) != 0 {
            self.write_byte((v as u8 & 0x7F) | 0x80);
            v >>= 7;
        }
        self.write_byte(v as u8);
    }

    /// Write a zig-zag encoded signed long.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
    fn write_varlong(&mut self, value: i64) {
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        self.write_unsigned_varlong(zigzag);
    }
}

/// Implement `Writer` for `Vec<u8>` so messages can write directly into a
/// growable buffer (used by `ApiRequest::respond_with`).
///
/// MIGRATION_SOURCE: (new)
impl Writer for Vec<u8> {
    fn write_byte(&mut self, val: u8) {
        self.push(val);
    }

    fn write_short(&mut self, val: i16) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    fn write_int(&mut self, val: i32) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    fn write_long(&mut self, val: i64) {
        self.extend_from_slice(&val.to_be_bytes());
    }

    fn write_bytes(&mut self, arr: &[u8]) {
        self.extend_from_slice(arr);
    }
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