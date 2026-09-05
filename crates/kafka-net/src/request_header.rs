//! Request header serialization.
//!
//! Mirrors `org.apache.kafka.common.requests.RequestHeader` / `RequestHeaderData`.
//!
//! Wire format (v1, the baseline since Kafka 4.0):
//!   api_key       int16  (4 bytes)
//!   api_version   int16  (4 bytes)
//!   correlation_id int32  (4 bytes)
//!   client_id     string  (2-byte length prefix + UTF-8 bytes)

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: impl Into<String>) -> Self {
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id: client_id.into(),
        }
    }

    pub fn size(&self) -> usize {
        2 + 2 + 4 + 2 + self.client_id.len()
    }

    pub fn write(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.api_key.to_be_bytes());
        buf.extend_from_slice(&self.api_version.to_be_bytes());
        buf.extend_from_slice(&self.correlation_id.to_be_bytes());
        buf.extend_from_slice(&(self.client_id.len() as i16).to_be_bytes());
        buf.extend_from_slice(self.client_id.as_bytes());
    }

    pub fn read(buf: &mut &[u8]) -> Result<Self, crate::errors::NetworkError> {
        if buf.len() < 8 {
            return Err(crate::errors::NetworkError::Eof);
        }
        let api_key = i16::from_be_bytes(buf[0..2].try_into().unwrap());
        let api_version = i16::from_be_bytes(buf[2..4].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(buf[4..8].try_into().unwrap());
        *buf = &buf[8..];

        if buf.len() < 2 {
            return Err(crate::errors::NetworkError::Eof);
        }
        let client_id_len = i16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
        *buf = &buf[2..];

        if buf.len() < client_id_len {
            return Err(crate::errors::NetworkError::Eof);
        }
        let client_id = String::from_utf8_lossy(&buf[..client_id_len]).into_owned();
        *buf = &buf[client_id_len..];

        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}
