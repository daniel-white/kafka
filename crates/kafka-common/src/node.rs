//! Node: a broker/controller node in the Kafka cluster.
//!
//! Mirrors Java's `Node` class.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java

use getset::CopyGetters;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A Kafka cluster node (broker or controller).
///
/// Fields are private; access via `getset` generated accessors.
/// Includes a cached hash code (mirrors Java's lazy `hash` field).
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
#[derive(Debug, Clone, CopyGetters)]
pub struct Node {
    #[get_copy = "pub"]
    id: i32,
    #[get_copy = "pub"]
    port: i32,
    host: String,
    id_string: String,
    rack: Option<String>,
    #[get_copy = "pub"]
    is_fenced: bool,
    cached_hash: std::cell::Cell<Option<u64>>,
}

impl Node {
    /// Sentinel: no node.
    ///
    /// Mirrors Java's `Node.NO_NODE`.
    #[allow(clippy::declare_interior_mutable_const)]
    pub const NO_NODE: Node = Node {
        id: -1,
        port: -1,
        host: String::new(),
        id_string: String::new(),
        rack: None,
        is_fenced: false,
        cached_hash: std::cell::Cell::new(None),
    };

    /// Create a new Node with the given id, host, port.
    ///
    /// Mirrors Java's `Node(int id, String host, int port)`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn new(id: i32, host: String, port: i32) -> Self {
        Node::with_rack(id, host, port, None, false)
    }

    /// Create a new Node with the given id, host, port, and rack.
    ///
    /// Mirrors Java's `Node(int id, String host, int port, String rack)`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn with_rack(id: i32, host: String, port: i32, rack: Option<String>, is_fenced: bool) -> Self {
        let id_string = id.to_string();
        Node {
            id,
            port,
            host,
            id_string,
            rack,
            is_fenced,
            cached_hash: std::cell::Cell::new(None),
        }
    }

    /// Sentinel access: returns `NO_NODE`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn no_node() -> Node {
        Node::NO_NODE
    }

    /// Check if this node is empty (host is null/empty or port < 0).
    ///
    /// Mirrors Java's `isEmpty()`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn is_empty(&self) -> bool {
        self.host.is_empty() || self.port < 0
    }

    /// String representation of the node id.
    ///
    /// Mirrors Java's `idString()`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn id_string(&self) -> &str {
        &self.id_string
    }

    /// The host name for this node.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn host(&self) -> &str {
        &self.host
    }

    /// True if this node has a defined rack.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn has_rack(&self) -> bool {
        self.rack.is_some()
    }

    /// The rack for this node, or None.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Node.java
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Compute hash code with caching (mirrors Java's lazy `hashCode()`).
    fn cached_hash_code(&self) -> u64 {
        if let Some(h) = self.cached_hash.get() {
            return h;
        }
        let prime: u64 = 31;
        let mut result = prime;
        result = result.wrapping_mul(prime).wrapping_add(hash_str(&self.host));
        result = result.wrapping_add(self.id as u64);
        result = result.wrapping_mul(prime).wrapping_add(self.port as u64);
        result = result
            .wrapping_mul(prime)
            .wrapping_add(self.rack.as_ref().map(|r| hash_str(r)).unwrap_or(0));
        result = result
            .wrapping_mul(prime)
            .wrapping_add(if self.is_fenced { 1 } else { 0 });
        self.cached_hash.set(Some(result));
        result
    }
}

fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0;
    for byte in s.bytes() {
        h = h.wrapping_mul(31).wrapping_add(byte as u64);
    }
    h
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.port == other.port
            && self.host == other.host
            && self.rack == other.rack
            && self.is_fenced == other.is_fenced
    }
}

impl Eq for Node {}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.cached_hash_code());
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} (id: {} rack: {:?} isFenced: {})",
            self.host, self.port, self.id_string, self.rack, self.is_fenced
        )
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::no_node()
    }
}
