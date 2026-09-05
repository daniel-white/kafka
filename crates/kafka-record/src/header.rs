//! Header type for records.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/header/Header.java`
//! and `clients/src/main/java/org/apache/kafka/common/header/internals/RecordHeader.java`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    key: String,
    value: Option<Vec<u8>>,
}

impl Header {
    pub fn new(key: impl Into<String>, value: Option<Vec<u8>>) -> Self {
        Header {
            key: key.into(),
            value,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

pub const EMPTY_HEADERS: [Header; 0] = [];
