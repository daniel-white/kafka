//! Writable trait mirroring Java's binary I/O write interface.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Writable.java

use crate::uuid::Uuid;

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
