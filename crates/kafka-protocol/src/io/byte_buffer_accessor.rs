//! ByteBufferAccessor: a `Reader` + `Writer` backed by a growable byte buffer.
//!
//! Mirrors Java's `ByteBufferAccessor` which wraps `java.nio.ByteBuffer`.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java

use crate::io::reader::Reader;
use crate::io::writer::Writer;
use getset::{Getters, CopyGetters};
use std::io::{self, Read, Write};

/// A byte buffer that implements both [`Reader`] and [`Writer`].
///
/// Backed by a `Vec<u8>` with explicit position and limit tracking, mirroring
/// Java's `ByteBuffer` position/limit semantics. All fields are private;
/// access via generated `getset` accessors.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
#[derive(Getters, CopyGetters, Clone)]
pub struct ByteBufferAccessor {
    /// The backing byte store.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    #[get = "pub"]
    buf: Vec<u8>,

    /// Current read/write cursor.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    #[get_copy = "pub"]
    position: usize,

    /// Upper bound for reads / upper bound of valid data after `flip`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    #[get_copy = "pub"]
    limit: usize,
}

impl ByteBufferAccessor {
    /// Allocate a buffer with the given capacity for writing then reading.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    pub fn with_capacity(capacity: usize) -> Self {
        ByteBufferAccessor {
            buf: vec![0u8; capacity],
            position: 0,
            limit: capacity,
        }
    }

    /// Wrap an existing byte vector for reading.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    pub fn from_bytes(data: Vec<u8>) -> Self {
        let len = data.len();
        ByteBufferAccessor {
            buf: data,
            position: 0,
            limit: len,
        }
    }

    /// Wrap a byte slice for reading.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    pub fn from_slice(data: &[u8]) -> Self {
        Self::from_bytes(data.to_vec())
    }

    /// Flip: switch from write mode to read mode. Sets `limit = position`,
    /// then `position = 0`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    pub fn flip(&mut self) {
        self.limit = self.position;
        self.position = 0;
    }

    /// Reset to write mode: position = 0, limit = capacity.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    pub fn clear(&mut self) {
        self.position = 0;
        self.limit = self.buf.len();
    }

    /// Returns the bytes written so far (between position 0 and limit after flip).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    pub fn buffer(&self) -> &[u8] {
        &self.buf[..self.limit]
    }

    /// Ensure there is room for `additional` bytes.
    fn ensure_capacity(&mut self, additional: usize) {
        if self.position + additional > self.buf.len() {
            let new_len = (self.position + additional).max(self.buf.len() * 2);
            self.buf.resize(new_len, 0u8);
            self.limit = self.buf.len();
        }
    }
}

impl Read for ByteBufferAccessor {
    /// Implements `java.io.InputStream`-style reading into a byte slice.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ByteBufferAccessor.java
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let available = self.remaining();
        if available == 0 {
            return Ok(0);
        }
        let n = available.min(dst.len());
        dst[..n].copy_from_slice(&self.buf[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}

impl Write for ByteBufferAccessor {
    /// Implements `java.io.OutputStream`-style writing.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/DataOutputStreamWritable.java
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        self.ensure_capacity(src.len());
        self.buf[self.position..self.position + src.len()].copy_from_slice(src);
        self.position += src.len();
        if self.position > self.limit {
            self.limit = self.position;
        }
        Ok(src.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Reader for ByteBufferAccessor {
    fn read_byte(&mut self) -> io::Result<u8> {
        if self.position + 1 > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read a byte",
            ));
        }
        let val = self.buf[self.position];
        self.position += 1;
        Ok(val)
    }

    fn read_short(&mut self) -> io::Result<i16> {
        let remaining = self.remaining();
        if remaining < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Error reading byte array of 2 byte(s): only {} byte(s) available",
                    remaining
                ),
            ));
        }
        let val = i16::from_be_bytes([self.buf[self.position], self.buf[self.position + 1]]);
        self.position += 2;
        Ok(val)
    }

    fn read_int(&mut self) -> io::Result<i32> {
        let remaining = self.remaining();
        if remaining < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Error reading byte array of 4 byte(s): only {} byte(s) available",
                    remaining
                ),
            ));
        }
        let val = i32::from_be_bytes([
            self.buf[self.position],
            self.buf[self.position + 1],
            self.buf[self.position + 2],
            self.buf[self.position + 3],
        ]);
        self.position += 4;
        Ok(val)
    }

    fn read_long(&mut self) -> io::Result<i64> {
        let remaining = self.remaining();
        if remaining < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Error reading byte array of 8 byte(s): only {} byte(s) available",
                    remaining
                ),
            ));
        }
        let val = i64::from_be_bytes([
            self.buf[self.position],
            self.buf[self.position + 1],
            self.buf[self.position + 2],
            self.buf[self.position + 3],
            self.buf[self.position + 4],
            self.buf[self.position + 5],
            self.buf[self.position + 6],
            self.buf[self.position + 7],
        ]);
        self.position += 8;
        Ok(val)
    }



    fn read_bytes_vec(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let remaining = self.remaining();
        if len > remaining {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Error reading byte array of {} byte(s): only {} byte(s) available",
                    len, remaining
                ),
            ));
        }
        
        let arr = self.buf[self.position..self.position + len].to_vec();
        self.position += len;
        
        Ok(arr)
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.position)
    }

    fn slice(&self) -> Self {
        ByteBufferAccessor::from_slice(&self.buf[self.position..self.limit])
    }
}

impl Writer for ByteBufferAccessor {
    fn write_byte(&mut self, val: u8) {
        self.ensure_capacity(1);
        self.buf[self.position] = val;
        self.position += 1;
        if self.position > self.limit {
            self.limit = self.position;
        }
    }

    fn write_short(&mut self, val: i16) {
        self.write_bytes(&val.to_be_bytes());
    }

    fn write_int(&mut self, val: i32) {
        self.write_bytes(&val.to_be_bytes());
    }

    fn write_long(&mut self, val: i64) {
        self.write_bytes(&val.to_be_bytes());
    }

    fn write_bytes(&mut self, arr: &[u8]) {
        self.write_all(arr).unwrap();
    }
}
