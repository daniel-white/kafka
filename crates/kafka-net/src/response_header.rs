//! Response header deserialization.
//!
//! Mirrors `org.apache.kafka.common.requests.ResponseHeader` / `ResponseHeaderData`.
//!
//! Wire format (v0, the baseline):
//!   correlation_id  int32  (4 bytes)

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn new(correlation_id: i32) -> Self {
        ResponseHeader { correlation_id }
    }

    pub fn size(&self) -> usize {
        4
    }

    pub fn write(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.correlation_id.to_be_bytes());
    }

    pub fn read(buf: &mut &[u8]) -> Result<Self, crate::errors::NetworkError> {
        if buf.len() < 4 {
            return Err(crate::errors::NetworkError::Eof);
        }
        let correlation_id = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        *buf = &buf[4..];
        Ok(ResponseHeader { correlation_id })
    }
}
