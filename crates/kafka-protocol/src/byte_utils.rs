//! Standalone byte-level utility functions.
//!
//! These are free functions (not tied to Reader/Writer traits) used by
//! network and record crates for tasks like computing varint sizes and
//! reading varints from slices.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java

use std::io;

/// Read an unsigned varint from a slice, returning (value, bytes_consumed).
///
/// Returns an `io::Error` if the slice is empty or the varint overflows.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_unsigned_varint_from_slice(buf: &[u8]) -> io::Result<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        if shift >= 35 || (shift == 28 && b > 0x0F) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Varint is too long",
            ));
        }
        result |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected end of varint",
    ))
}

/// Read an unsigned varlong from a slice, returning (value, bytes_consumed).
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_unsigned_varlong_from_slice(buf: &[u8]) -> io::Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        if shift >= 70 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Varlong is too long",
            ));
        }
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected end of varlong",
    ))
}

/// Compute the number of bytes needed to encode `value` as an unsigned varint.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_unsigned_varint(value: u32) -> usize {
    let mut v = value;
    let mut size = 1;
    while v >= 0x80 {
        v >>= 7;
        size += 1;
    }
    size
}

/// Compute the number of bytes needed to encode `value` as an unsigned varlong.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_unsigned_varlong(value: u64) -> usize {
    let mut v = value;
    let mut size = 1;
    while v >= 0x80 {
        v >>= 7;
        size += 1;
    }
    size
}

/// Alias for [`size_of_unsigned_varint`].
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_varint(value: u32) -> usize {
    size_of_unsigned_varint(value)
}

/// Alias for [`size_of_unsigned_varlong`].
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_varlong(value: u64) -> usize {
    size_of_unsigned_varlong(value)
}