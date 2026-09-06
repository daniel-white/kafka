//! Writer trait mirroring Java's binary I/O write interface.
//!
//! Includes default methods for Kafka protocol types: STRING, COMPACT_STRING,
//! NULLABLE_STRING, COMPACT_NULLABLE_STRING, and array counts.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java

use crate::byte_utils;
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
    fn write_double(&mut self, val: f64);
    fn write_byte_array(&mut self, arr: &[u8]);
    fn write_unsigned_varint(&mut self, val: u32);
    fn write_byte_buffer(&mut self, buf: &[u8]);
    fn write_varint(&mut self, val: i32);
    fn write_varlong(&mut self, val: i64);

    /// Write a string as UTF-8 bytes.
    fn write_byte_array_string(&mut self, s: &str) {
        self.write_byte_array(s.as_bytes());
    }

    /// Write a STRING: int16 length prefix + UTF-8 bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_string(&mut self, s: &str) {
        self.write_short(s.len() as i16);
        self.write_byte_array(s.as_bytes());
    }

    /// Write a COMPACT_STRING: unsigned varint length prefix (len+1) + UTF-8 bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_compact_string(&mut self, s: &str) {
        self.write_unsigned_varint(s.len() as u32 + 1);
        self.write_byte_array(s.as_bytes());
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

    /// Write a COMPACT_NULLABLE_STRING: unsigned varint (0 for null).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_compact_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.write_compact_string(s),
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

    /// Size of an ARRAY/COMPACT_ARRAY count.
    fn array_count_size(count: usize, flexible: bool) -> usize {
        if flexible {
            byte_utils::size_of_unsigned_varint(count as u32 + 1)
        } else {
            4
        }
    }

    /// Write a UUID (most significant bits first).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_uuid(&mut self, uuid: &Uuid) {
        self.write_long(uuid.get_most_significant_bits());
        self.write_long(uuid.get_least_significant_bits());
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

    /// Size of a STRING.
    fn size_of_string(s: &str) -> usize {
        2 + s.len()
    }

    /// Size of a COMPACT_STRING.
    fn size_of_compact_string(s: &str) -> usize {
        byte_utils::size_of_unsigned_varint(s.len() as u32 + 1) + s.len()
    }

    /// Size of a NULLABLE_STRING.
    fn size_of_nullable_string(s: Option<&str>) -> usize {
        match s {
            Some(s) => Self::size_of_string(s),
            None => 2,
        }
    }

    /// Size of a COMPACT_NULLABLE_STRING.
    fn size_of_compact_nullable_string(s: Option<&str>) -> usize {
        match s {
            Some(s) => Self::size_of_compact_string(s),
            None => 1,
        }
    }

    /// Write a single tagged field (tag + size + data) for flexible messages.
    fn write_tagged_field(&mut self, tag: i32, data: &[u8]) {
        self.write_unsigned_varint(tag as u32);
        self.write_unsigned_varint(data.len() as u32);
        self.write_byte_array(data);
    }

    /// Size of a tagged field entry.
    fn size_of_tagged_field(tag: i32, data: &[u8]) -> usize {
        byte_utils::size_of_unsigned_varint(tag as u32)
            + byte_utils::size_of_unsigned_varint(data.len() as u32)
            + data.len()
    }

    /// Write empty tagged fields section (just count = 0).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writer.java
    fn write_empty_tagged_fields(&mut self) {
        self.write_unsigned_varint(0);
    }
}

/// `Writer` implementation for `Vec<u8>` — appends bytes to the vector.
///
/// `Vec<u8>` is a mutable growth-capable sink, appropriate for building buffers.
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
    fn write_double(&mut self, val: f64) {
        self.extend_from_slice(&val.to_be_bytes());
    }
    fn write_byte_array(&mut self, arr: &[u8]) {
        self.extend_from_slice(arr);
    }
    fn write_unsigned_varint(&mut self, val: u32) {
        crate::byte_utils::write_unsigned_varint(val, self).unwrap();
    }
    fn write_byte_buffer(&mut self, buf: &[u8]) {
        self.extend_from_slice(buf);
    }
    fn write_varint(&mut self, val: i32) {
        let unsigned = ((val << 1) ^ (val >> 31)) as u32;
        crate::byte_utils::write_unsigned_varint(unsigned, self).unwrap();
    }
    fn write_varlong(&mut self, val: i64) {
        let unsigned = ((val << 1) ^ (val >> 63)) as u64;
        crate::byte_utils::write_unsigned_varlong(unsigned, self).unwrap();
    }
}

/// `Writer` implementation for `SizeCounter` — counts bytes without storing them.
///
/// Useful for pre-computing `body_size()` without allocating a buffer.
pub struct SizeCounter {
    size: usize,
}

impl SizeCounter {
    pub fn new() -> Self {
        SizeCounter { size: 0 }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl Default for SizeCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl Writer for SizeCounter {
    fn write_byte(&mut self, _val: u8) {
        self.size += 1;
    }
    fn write_short(&mut self, _val: i16) {
        self.size += 2;
    }
    fn write_int(&mut self, _val: i32) {
        self.size += 4;
    }
    fn write_long(&mut self, _val: i64) {
        self.size += 8;
    }
    fn write_double(&mut self, _val: f64) {
        self.size += 8;
    }
    fn write_byte_array(&mut self, arr: &[u8]) {
        self.size += arr.len();
    }
    fn write_unsigned_varint(&mut self, val: u32) {
        self.size += byte_utils::size_of_unsigned_varint(val);
    }
    fn write_byte_buffer(&mut self, buf: &[u8]) {
        self.size += buf.len();
    }
    fn write_varint(&mut self, val: i32) {
        self.size += byte_utils::size_of_varint(val);
    }
    fn write_varlong(&mut self, val: i64) {
        self.size += byte_utils::size_of_varlong(val);
    }
}
