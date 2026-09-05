//! Writable trait mirroring Java's binary I/O write interface.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writable.java

use kafka_common::uuid::Uuid;

/// Trait for writing primitive types to a byte sink, mirroring Java's `Writable`.
///
/// Uses generic parameter dispatch (no `dyn Trait`) per the no-virtual-dispatch rule.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writable.java
pub trait Writable {
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

    /// Write a string as UTF-8 bytes followed by the byte array.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writable.java
    fn write_string(&mut self, s: &str) {
        self.write_byte_array(s.as_bytes());
    }

    /// Write a UUID (most significant bits first).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writable.java
    fn write_uuid(&mut self, uuid: &Uuid) {
        self.write_long(uuid.get_most_significant_bits());
        self.write_long(uuid.get_least_significant_bits());
    }

    /// Write an unsigned short (2 bytes, big-endian).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writable.java
    fn write_unsigned_short(&mut self, val: u16) {
        self.write_short(val as i16);
    }

    /// Write an unsigned int (4 bytes, big-endian).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writable.java
    fn write_unsigned_int(&mut self, val: u32) {
        self.write_int(val as i32);
    }
}

/// `Writable` implementation for `Vec<u8>` — appends bytes to the vector.
///
/// `Vec<u8>` is a mutable growth-capable sink, appropriate for building buffers.
impl Writable for Vec<u8> {
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
