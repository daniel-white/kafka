//! NetworkReceive — reads a 4-byte size header + N bytes of content.
//!
//! Mirrors `org.apache.kafka.common.network.NetworkReceive`.
//! The wire framing is: 4-byte int32 (big-endian) size N, followed by N bytes of payload.

use crate::errors::NetworkError;

pub const SIZE_HEADER_SIZE: usize = 4;

#[derive(Debug)]
pub struct NetworkReceive {
    source: String,
    max_size: i32,
    size_buffer: Vec<u8>,
    size_read: usize,
    payload: Option<Vec<u8>>,
    payload_read: usize,
    expected_payload_size: usize,
}

impl NetworkReceive {
    pub const UNLIMITED: i32 = -1;
    pub const UNKNOWN_SOURCE: &'static str = "";

    pub fn new() -> Self {
        Self::with_max_size(Self::UNLIMITED)
    }

    pub fn with_max_size(max_size: i32) -> Self {
        NetworkReceive {
            source: Self::UNKNOWN_SOURCE.to_string(),
            max_size,
            size_buffer: Vec::with_capacity(SIZE_HEADER_SIZE),
            size_read: 0,
            payload: None,
            payload_read: 0,
            expected_payload_size: 0,
        }
    }

    pub fn from_payload(source: impl Into<String>, payload: Vec<u8>) -> Self {
        let len = payload.len();
        NetworkReceive {
            source: source.into(),
            max_size: Self::UNLIMITED,
            size_buffer: Vec::new(),
            size_read: SIZE_HEADER_SIZE,
            payload: Some(payload),
            payload_read: len,
            expected_payload_size: len,
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn complete(&self) -> bool {
        self.size_read == SIZE_HEADER_SIZE
            && self.payload.is_some()
            && self.payload_read >= self.expected_payload_size
    }

    pub fn required_memory_amount_known(&self) -> bool {
        self.size_read == SIZE_HEADER_SIZE || self.payload.is_some()
    }

    pub fn memory_allocated(&self) -> bool {
        self.payload.is_some()
    }

    pub fn feed(&mut self, data: &[u8]) -> Result<usize, NetworkError> {
        let mut consumed = 0;

        if self.size_read < SIZE_HEADER_SIZE {
            let remaining = SIZE_HEADER_SIZE - self.size_read;
            let to_read = remaining.min(data.len());
            self.size_buffer.extend_from_slice(&data[..to_read]);
            self.size_read += to_read;
            consumed += to_read;

            if self.size_read < SIZE_HEADER_SIZE {
                return Ok(consumed);
            }

            let receive_size = i32::from_be_bytes(
                self.size_buffer[..SIZE_HEADER_SIZE]
                    .try_into()
                    .unwrap(),
            );
            if receive_size < 0 {
                return Err(NetworkError::InvalidReceive {
                    size: receive_size,
                    max: self.max_size,
                });
            }
            if self.max_size != Self::UNLIMITED && receive_size > self.max_size {
                return Err(NetworkError::InvalidReceive {
                    size: receive_size,
                    max: self.max_size,
                });
            }
            self.expected_payload_size = receive_size as usize;
            if receive_size == 0 {
                self.payload = Some(Vec::new());
            } else {
                self.payload = Some(Vec::with_capacity(self.expected_payload_size));
            }
        }

        if let Some(ref mut buf) = self.payload {
            if self.payload_read < self.expected_payload_size {
                let remaining = data.len() - consumed;
                let to_read = remaining.min(self.expected_payload_size - self.payload_read);
                if to_read > 0 {
                    buf.extend_from_slice(&data[consumed..consumed + to_read]);
                    self.payload_read += to_read;
                    consumed += to_read;
                }
            }
        }

        Ok(consumed)
    }

    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    pub fn size(&self) -> usize {
        self.size_read + self.payload_read
    }
}

impl Default for NetworkReceive {
    fn default() -> Self {
        Self::new()
    }
}
