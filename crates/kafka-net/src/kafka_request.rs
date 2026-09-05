//! KafkaRequest — full request framing: size header + RequestHeader + body.
//!
//! Mirrors `org.apache.kafka.clients.NetworkClient` send logic.
//! Wire format:
//!   request_size    int32  (4 bytes, big-endian) — size of (header + body)
//!   api_key         int16  (2 bytes)
//!   api_version     int16  (2 bytes)
//!   correlation_id  int32  (4 bytes)
//!   client_id       string (2-byte len prefix + UTF-8)
//!   ... body ...

use crate::byte_buffer_send::SIZE_HEADER_SIZE;
use crate::request_header::RequestHeader;

#[derive(Debug, Clone, Default)]
pub struct KafkaRequest {
    pub header: RequestHeader,
    pub body: Vec<u8>,
}

impl KafkaRequest {
    pub fn new(header: RequestHeader, body: Vec<u8>) -> Self {
        KafkaRequest { header, body }
    }

    pub fn size(&self) -> usize {
        SIZE_HEADER_SIZE + self.header.size() + self.body.len()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.size());
        let payload_size = self.header.size() + self.body.len();
        buf.extend_from_slice(&(payload_size as i32).to_be_bytes());
        self.header.write(&mut buf);
        buf.extend_from_slice(&self.body);
        buf
    }
}

