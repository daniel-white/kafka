//! ByteBufferSend — sends data backed by a byte buffer with a 4-byte size header.
//!
//! Mirrors `org.apache.kafka.common.network.ByteBufferSend` and `NetworkSend`.
//! Wire format: 4-byte int32 (big-endian) size N, followed by N bytes of payload.

use crate::errors::NetworkError;

pub const SIZE_HEADER_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub struct ByteBufferSend {
    size: usize,
    buffers: Vec<Vec<u8>>,
    written: usize,
}

impl ByteBufferSend {
    pub fn new(data: Vec<u8>) -> Self {
        let total = data.len();
        ByteBufferSend {
            size: total,
            buffers: vec![data],
            written: 0,
        }
    }

    pub fn zero_copy() -> Self {
        ByteBufferSend {
            size: 0,
            buffers: vec![],
            written: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.size + SIZE_HEADER_SIZE
    }

    pub fn completed(&self) -> bool {
        self.written >= self.size
    }

    pub fn remaining(&self) -> usize {
        self.size.saturating_sub(self.written)
    }

    /// Serialize the full send (size header + payload) into a byte vector.
    pub fn serialize(&self) -> Result<Vec<u8>, NetworkError> {
        let mut result = Vec::with_capacity(self.size());
        result.extend_from_slice(&(self.size as i32).to_be_bytes());
        for buf in &self.buffers {
            result.extend_from_slice(buf);
        }
        Ok(result)
    }

    /// Write as much as possible to `out`, returning bytes written (including header if not yet written).
    /// Returns the total number of bytes consumed from this send.
    pub fn write_to(&mut self, out: &mut Vec<u8>) -> usize {
        let mut written = 0;

        // Write size header first (4 bytes)
        if self.written < SIZE_HEADER_SIZE {
            let header = (self.size as i32).to_be_bytes();
            let to_write = (SIZE_HEADER_SIZE - self.written).min(4);
            out.extend_from_slice(&header[self.written..self.written + to_write]);
            self.written += to_write;
            written += to_write;
        }

        // Write payload
        while self.written < self.size + SIZE_HEADER_SIZE {
            let payload_offset = self.written - SIZE_HEADER_SIZE;
            // Find which buffer we're in
            let mut buf_start = 0;
            let mut buf_idx = 0;
            for (i, buf) in self.buffers.iter().enumerate() {
                if buf_start + buf.len() > payload_offset {
                    buf_idx = i;
                    break;
                }
                buf_start += buf.len();
                buf_idx = i;
            }

            if buf_idx >= self.buffers.len() {
                break;
            }

            let buf = &self.buffers[buf_idx];
            let offset_in_buf = payload_offset - buf_start;
            let to_write = buf[offset_in_buf..].len().min(4096);
            out.extend_from_slice(&buf[offset_in_buf..offset_in_buf + to_write]);
            self.written += to_write;
            written += to_write;
        }

        written
    }
}

impl From<Vec<u8>> for ByteBufferSend {
    fn from(data: Vec<u8>) -> Self {
        ByteBufferSend::new(data)
    }
}
