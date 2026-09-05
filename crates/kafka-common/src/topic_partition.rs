//! TopicPartition: a pair of topic name and partition index.
//!
//! Mirrors Java's `TopicPartition` class — a serializable, hashable pair used
//! as a key throughout the Kafka client and broker code.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/TopicPartition.java

use getset::{CopyGetters, Getters};
use std::hash::{Hash, Hasher};

/// A topic name and partition index pair.
///
/// Implements `Hash` and `Eq` for use as a HashMap/HashSet key.
/// The hash is cached after first computation (mirrors Java's lazy `hash` field).
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/TopicPartition.java
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct TopicPartition {
    #[get_copy = "pub"]
    partition: i32,
    #[get = "pub"]
    topic: String,
    /// Cached hash code (0 = uncomputed). Mirrors Java's lazy hash field.
    cached_hash: std::cell::Cell<u32>,
}

impl TopicPartition {
    /// Create a new TopicPartition.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/TopicPartition.java
    pub fn new(topic: String, partition: i32) -> Self {
        TopicPartition {
            partition,
            topic,
            cached_hash: std::cell::Cell::new(0),
        }
    }

    /// Compute the hash code, caching it for subsequent calls.
    /// Mirrors Java's lazy `hashCode()`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/TopicPartition.java
    fn cached_hash_code(&self) -> u32 {
        let cached = self.cached_hash.get();
        if cached != 0 {
            return cached;
        }
        let prime: u32 = 31;
        let mut result = prime.wrapping_add(self.partition as u32);
        let topic_hash = {
            let mut h: u32 = 0;
            for byte in self.topic.bytes() {
                h = h.wrapping_mul(prime).wrapping_add(byte as u32);
            }
            h
        };
        result = result.wrapping_mul(prime).wrapping_add(topic_hash);
        self.cached_hash.set(result);
        result
    }
}

impl PartialEq for TopicPartition {
    fn eq(&self, other: &Self) -> bool {
        self.partition == other.partition && self.topic == other.topic
    }
}

impl Eq for TopicPartition {}

impl Hash for TopicPartition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.cached_hash_code());
    }
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Java: "topic-partition"
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

impl Default for TopicPartition {
    fn default() -> Self {
        TopicPartition::new(String::new(), -1)
    }
}
