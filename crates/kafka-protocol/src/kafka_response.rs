//! KafkaResponse — full response framing: size header + ResponseHeader + body.
//!
//! Mirrors `org.apache.kafka.clients.NetworkClient` receive logic.
//! Wire format:
//!   response_size   int32  (4 bytes, big-endian) — size of (header + body)
//!   correlation_id  int32  (4 bytes)
//!   ... body ...

use crate::byte_buffer_send::SIZE_HEADER_SIZE;
use crate::response_header::ResponseHeader;

#[derive(Debug, Clone)]
pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub body: Box<[u8]>,
}

impl KafkaResponse {
    pub fn deserialize(data: &[u8]) -> Result<Self, crate::errors::NetworkError> {
        if data.len() < SIZE_HEADER_SIZE + 4 {
            return Err(crate::errors::NetworkError::Eof);
        }

        let _response_size = i32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut buf = &data[SIZE_HEADER_SIZE..];

        let correlation_id = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        buf = &buf[4..];

        let body: Box<[u8]> = buf.into();

        Ok(KafkaResponse {
            header: ResponseHeader::new(correlation_id),
            body,
        })
    }

    pub fn size(&self) -> usize {
        SIZE_HEADER_SIZE + self.header.size() + self.body.len()
    }
}
