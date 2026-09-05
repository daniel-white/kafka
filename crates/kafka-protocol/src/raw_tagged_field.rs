//! An immutable (tag, data) pair used when deserializing unknown tagged fields.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/types/RawTaggedField.java

use getset::Getters;
use serde::{Deserialize, Serialize};

/// A raw tagged field: a tag number and its opaque payload bytes.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/types/RawTaggedField.java
#[derive(Getters, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct RawTaggedField {
    #[get = "pub"]
    tag: i32,
    #[get = "pub"]
    data: Box<[u8]>,
}

impl RawTaggedField {
    /// Construct a new RawTaggedField.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/types/RawTaggedField.java
    pub fn new(tag: i32, data: Vec<u8>) -> Self {
        RawTaggedField { tag, data: data.into_boxed_slice() }
    }

    /// Number of payload bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/types/RawTaggedField.java
    pub fn size(&self) -> usize {
        self.data.len()
    }
}
