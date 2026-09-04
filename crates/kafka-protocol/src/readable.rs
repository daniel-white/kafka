//! Readable trait mirroring Java's binary I/O read interface.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java

use crate::raw_tagged_field::RawTaggedField;
use crate::uuid::Uuid;
use std::io;
use std::vec::Vec;

/// Trait for reading primitive types from a byte source, mirroring Java's `Readable`.
///
/// Uses generic parameter dispatch (no `dyn Trait`) per the no-virtual-dispatch rule.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
pub trait Readable {
    fn read_byte(&mut self) -> io::Result<u8>;
    fn read_short(&mut self) -> io::Result<i16>;
    fn read_int(&mut self) -> io::Result<i32>;
    fn read_long(&mut self) -> io::Result<i64>;
    fn read_double(&mut self) -> io::Result<f64>;
    fn read_array(&mut self, len: usize) -> io::Result<Vec<u8>>;
    fn read_unsigned_varint(&mut self) -> io::Result<u32>;
    fn read_byte_buffer(&mut self, len: usize) -> io::Result<Vec<u8>>;
    fn read_varint(&mut self) -> io::Result<i32>;
    fn read_varlong(&mut self) -> io::Result<i64>;
    fn remaining(&self) -> usize;

    /// Returns a new Readable object whose content will be shared with this object.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
    fn slice(&self) -> Self
    where
        Self: Sized;

    /// Read a UTF-8 string of `len` bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
    fn read_string(&mut self, len: usize) -> io::Result<String> {
        let arr = self.read_array(len)?;
        String::from_utf8(arr).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })
    }

    /// Read unknown tagged fields, appending to `unknowns` if present.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
    fn read_unknown_tagged_field(
        &mut self,
        unknowns: Option<&mut Vec<RawTaggedField>>,
        tag: i32,
        size: usize,
    ) -> io::Result<()> {
        if let Some(u) = unknowns {
            let data = self.read_array(size)?;
            u.push(RawTaggedField::new(tag, data));
        }
        Ok(())
    }

    /// Read a UUID with the most significant digits first.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
    fn read_uuid(&mut self) -> io::Result<Uuid> {
        let most = self.read_long()?;
        let least = self.read_long()?;
        Ok(Uuid::new(most, least))
    }

    /// Read an unsigned short as a u16.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
    fn read_unsigned_short(&mut self) -> io::Result<u16> {
        Ok(self.read_short()? as u16)
    }

    /// Read an unsigned int as a u32.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
    fn read_unsigned_int(&mut self) -> io::Result<u32> {
        Ok(self.read_int()? as u32)
    }
}
