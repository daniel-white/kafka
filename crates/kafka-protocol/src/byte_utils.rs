//! Low-level varint / varlong / double read-write helpers.
//!
//! Mirrors `org.apache.kafka.common.utils.internals.ByteUtils`.
//! 
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java

use std::io::{self, Read, Write};

/// Read an unsigned int stored in variable-length (protobuf) format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_unsigned_varint<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut result = 0u32;
    let mut shift = 0;
    loop {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf)?;
        let b = buf[0];
        if shift >= 35 || (shift == 28 && b > 0x0F) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Varint is too long, the most significant bit in the 5th byte is set, converted value: {:#x}",
                        result),
            ));
        }
        result |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

/// Read a zig-zag encoded signed int.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_varint<R: Read>(r: &mut R) -> io::Result<i32> {
    let value = read_unsigned_varint(r)?;
    Ok(((value >> 1) as i32) ^ (-((value & 1) as i32)))
}

/// Read an unsigned long in variable-length format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_unsigned_varlong<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf)?;
        let b = buf[0];
        if shift >= 70 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Varlong is too long, most significant bit in the 10th byte is set, converted value: {:#x}",
                        value),
            ));
        }
        value |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

/// Read a zig-zag encoded signed long.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_varlong<R: Read>(r: &mut R) -> io::Result<i64> {
    let raw = read_unsigned_varlong(r)?;
    Ok(((raw >> 1) as i64) ^ (-((raw & 1) as i64)))
}

/// Write an unsigned int in variable-length (protobuf) format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn write_unsigned_varint<W: Write>(value: u32, w: &mut W) -> io::Result<()> {
    let mut v = value;
    while (v & !0x7F) != 0 {
        w.write_all(&[(v as u8 & 0x7F) | 0x80])?;
        v >>= 7;
    }
    w.write_all(&[v as u8])
}

/// Write a zig-zag encoded signed int.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn write_varint<W: Write>(value: i32, w: &mut W) -> io::Result<()> {
    let zigzag = ((value << 1) ^ (value >> 31)) as u32;
    write_unsigned_varint(zigzag, w)
}

/// Write an unsigned long in variable-length format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn write_unsigned_varlong<W: Write>(value: u64, w: &mut W) -> io::Result<()> {
    let mut v = value;
    while (v & !0x7F) != 0 {
        w.write_all(&[(v as u8 & 0x7F) | 0x80])?;
        v >>= 7;
    }
    w.write_all(&[v as u8])
}

/// Write a zig-zag encoded signed long.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn write_varlong<W: Write>(value: i64, w: &mut W) -> io::Result<()> {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    write_unsigned_varlong(zigzag, w)
}

/// Read an unsigned int stored in variable-length (protobuf) format from a byte slice.
///
/// Returns the decoded value and the number of bytes consumed.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_unsigned_varint_from_slice(buf: &[u8]) -> Result<(u32, usize), std::io::Error> {
    let mut result = 0u32;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i >= 5 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Varint is too long",
            ));
        }
        result |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "Unexpected EOF while reading varint",
    ))
}

/// Number of bytes needed to encode an unsigned int in variable-length format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_unsigned_varint(value: u32) -> usize {
    let leading_zeros = value.leading_zeros();
    let leading_zeros_below_38_div_7 = ((38u64 - leading_zeros as u64) * 0x12493) >> 19;
    (leading_zeros_below_38_div_7 as usize) + (leading_zeros as usize >> 5)
}

/// Number of bytes needed to encode a signed int in variable-length format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_varint(value: i32) -> usize {
    size_of_unsigned_varint(((value << 1) ^ (value >> 31)) as u32)
}

/// Number of bytes needed to encode a signed long in variable-length format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_varlong(value: i64) -> usize {
    size_of_unsigned_varlong(((value << 1) ^ (value >> 63)) as u64)
}

/// Number of bytes needed to encode an unsigned long in variable-length format.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn size_of_unsigned_varlong(value: u64) -> usize {
    let leading_zeros = value.leading_zeros();
    let leading_zeros_below_70_div_7 = ((70u64 - leading_zeros as u64) * 0x12493) >> 19;
    (leading_zeros_below_70_div_7 as usize) + (leading_zeros as usize >> 6)
}

/// Read a double (IEEE 754 big-endian) from a byte stream.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn read_double<R: Read>(r: &mut R) -> io::Result<f64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(f64::from_be_bytes(buf))
}

/// Write a double (IEEE 754 big-endian) to a byte stream.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/utils/internals/ByteUtils.java
pub fn write_double<W: Write>(value: f64, w: &mut W) -> io::Result<()> {
    w.write_all(&value.to_be_bytes())
}