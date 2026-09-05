//! Versioned API message wrapper.
//!
//! Wraps a metadata record body with its API key and version.
//! Mirrors `ApiMessageAndVersion` from `server-common`.
//!
//! Wire format (from `AbstractApiMessageSerde`):
//!   frame_version    unsigned varint = 1
//!   api_key          unsigned varint
//!   api_version      unsigned varint
//!   body              message-specific serialization
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ApiMessageAndVersion.java

use crate::MetadataError;
use crate::types::MetadataRecordType;

/// A metadata record message tagged with its API key and version.
///
/// `data` is an opaque byte buffer containing the serialized message body.
/// Immutable after construction — uses `Box<[u8]>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiMessageAndVersion {
    data: Box<[u8]>,
    api_key: i16,
    version: i16,
}

impl ApiMessageAndVersion {
    /// Create a new versioned API message.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ApiMessageAndVersion.java
    pub fn new(data: Vec<u8>, api_key: i16, version: i16) -> Self {
        ApiMessageAndVersion {
            data: data.into_boxed_slice(),
            api_key,
            version,
        }
    }

    /// Create from already-serialized boxed bytes.
    pub fn from_bytes(data: Box<[u8]>, api_key: i16, version: i16) -> Self {
        ApiMessageAndVersion {
            data,
            api_key,
            version,
        }
    }

    /// Number of bytes in the message body.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// API key identifying the message type.
    pub fn api_key(&self) -> i16 {
        self.api_key
    }

    /// Message version.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Record type corresponding to the API key.
    ///
    /// Returns `Err` if the API key is not a recognized metadata type.
    pub fn record_type(&self) -> Result<MetadataRecordType, MetadataError> {
        MetadataRecordType::from_api_key(self.api_key)
            .ok_or(MetadataError::UnknownRecordType(self.api_key))
    }
}

impl Default for ApiMessageAndVersion {
    fn default() -> Self {
        ApiMessageAndVersion {
            data: Box::new([]),
            api_key: 0,
            version: 0,
        }
    }
}
