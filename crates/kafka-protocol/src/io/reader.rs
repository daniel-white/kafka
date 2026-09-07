use crate::{MessageContext, RawTaggedField};
use kafka_common::Uuid;
use std::io;

/// Trait for reading a protocol message from a byte source.
///
/// Implementations parse the body according to the version-specific wire format.
///
/// The `MessageContext` carries the API version, allowing each field to be
/// conditionally read based on its `versions` annotation.
pub trait Readable: Sized {
    /// Read the message body from `r`.
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, crate::errors::ProtocolError>;
}

/// Trait for reading primitive types from a byte source, mirroring Java's `Reader`.
///
/// Uses generic parameter dispatch (no `dyn Trait`) per the no-virtual-dispatch rule.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
pub trait Reader {
    fn read_byte(&mut self) -> io::Result<u8>;
    fn read_short(&mut self) -> io::Result<i16>;
    fn read_int(&mut self) -> io::Result<i32>;
    fn read_long(&mut self) -> io::Result<i64>;
    fn read_bytes_array<const len: usize>(&mut self) -> io::Result<[u8; len]> {
        let vec = self.read_bytes_vec(len)?;
        let arr = vec.try_into().expect("bytes vec short");
        Ok(arr)
    }
    fn read_bytes_vec(&mut self, len: usize) -> io::Result<Vec<u8>>;
    fn remaining(&self) -> usize;

    /// Returns a new Reader object whose content will be shared with this object.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
    fn slice(&self) -> Self
    where
        Self: Sized;

    /// Read a UTF-8 string of `len` bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
    fn read_string(&mut self, len: usize) -> io::Result<String> {
        let buf = self.read_bytes_vec(len)?;
        String::from_utf8(buf).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })
    }

    /// Read a STRING (int16 length prefix + UTF-8 bytes).
    fn read_string_prefixed(&mut self) -> io::Result<String> {
        let len = self.read_short()?;
        if len < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "String length is negative",
            ));
        }
        self.read_string(len as usize)
    }

    /// Read a COMPACT_STRING (unsigned varint length prefix, actual = raw - 1).
    fn read_compact_string(&mut self) -> io::Result<String> {
        let raw_len = self.read_unsigned_varint()?;
        let actual_len = raw_len.saturating_sub(1) as usize;
        self.read_string(actual_len)
    }

    /// Read a NULLABLE_STRING (int16 length, -1 for null).
    fn read_nullable_string(&mut self) -> io::Result<Option<String>> {
        let len = self.read_short()?;
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(self.read_string(len as usize)?))
    }

    /// Read a NULLABLE_BYTES (int32 length, -1 for null).
    fn read_nullable_bytes(&mut self) -> io::Result<Option<Vec<u8>>> {
        let len = self.read_int()?;
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(self.read_bytes_vec(len as usize)?))
    }

    /// Read a COMPACT_NULLABLE_STRING (unsigned varint, 0 for null).
    fn read_compact_nullable_string(&mut self) -> io::Result<Option<String>> {
        let raw_len = self.read_unsigned_varint()?;
        if raw_len == 0 {
            return Ok(None);
        }
        let actual_len = (raw_len - 1) as usize;
        Ok(Some(self.read_string(actual_len)?))
    }

    /// Read a COMPACT_NULLABLE_BYTES (unsigned varint, 0 for null).
    fn read_compact_nullable_bytes(&mut self) -> io::Result<Option<Vec<u8>>> {
        let raw_len = self.read_unsigned_varint()?;
        if raw_len == 0 {
            return Ok(None);
        }
        let actual_len = (raw_len - 1) as usize;
        Ok(Some(self.read_bytes_vec(actual_len)?))
    }

    /// Read an ARRAY/COMPACT_ARRAY count, returning the actual element count.
    /// For flexible (compact arrays), the wire value is `count + 1`.
    fn read_array_count(&mut self, flexible: bool) -> io::Result<usize> {
        if flexible {
            Ok(self.read_unsigned_varint()?.saturating_sub(1) as usize)
        } else {
            let count = self.read_int()?;
            if count < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Array count is negative",
                ));
            }
            Ok(count as usize)
        }
    }

    /// Skip tagged fields section (count + tag/size/data triples).
    fn skip_tagged_fields(&mut self) -> io::Result<()> {
        let count = self.read_unsigned_varint()?;
        for _ in 0..count {
            let _tag = self.read_unsigned_varint()?;
            let size = self.read_unsigned_varint()? as usize;
            let _ = self.read_bytes_vec(size)?;
        }
        Ok(())
    }

    /// Read unknown tagged fields, appending to `unknowns` if present.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
    fn read_unknown_tagged_field(
        &mut self,
        unknowns: Option<&mut Vec<RawTaggedField>>,
        tag: i32,
        size: usize,
    ) -> io::Result<()> {
        if let Some(u) = unknowns {
            let data = self.read_bytes_vec(size)?;
            u.push(RawTaggedField::new(tag, data));
        }
        Ok(())
    }

    /// Read a UUID with the most significant digits first.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
    fn read_uuid(&mut self) -> io::Result<Uuid> {
        let most = self.read_long()?;
        let least = self.read_long()?;
        Ok(Uuid::new(most, least))
    }

    /// Read an unsigned short as a u16.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
    fn read_unsigned_short(&mut self) -> io::Result<u16> {
        Ok(self.read_short()? as u16)
    }

    /// Read an unsigned int as a u32.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
    fn read_unsigned_int(&mut self) -> io::Result<u32> {
        Ok(self.read_int()? as u32)
    }

    /// Read a boolean value as a single byte.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Reader.java
    fn read_boolean(&mut self) -> io::Result<bool> {
        Ok(self.read_byte()? != 0)
    }

    /// Read an unsigned varint using `read_byte`.
    fn read_unsigned_varint(&mut self) -> io::Result<u32> {
        let mut result = 0u32;
        let mut shift = 0;
        loop {
            let b = self.read_byte()?;
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
    fn read_varint(&mut self) -> io::Result<i32> {
        let value = self.read_unsigned_varint()?;
        Ok(((value >> 1) as i32) ^ (-((value & 1) as i32)))
    }

    /// Read an unsigned varlong using `read_byte`.
    fn read_unsigned_varlong(&mut self) -> io::Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let b = self.read_byte()?;
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
    fn read_varlong(&mut self) -> io::Result<i64> {
        let raw = self.read_unsigned_varlong()?;
        Ok(((raw >> 1) as i64) ^ (-((raw & 1) as i64)))
    }

    /// Read a double (IEEE 754 big-endian) from a byte stream.
    fn read_double(&mut self) -> io::Result<f64> {
        let buf = self.read_bytes_array()?;
        Ok(f64::from_be_bytes(buf))
    }
}