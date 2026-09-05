//! BrokerEndPoint: endpoint info for a broker.
//!
//! Mirrors Java's `BrokerEndPoint` record.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/network/BrokerEndPoint.java

use getset::CopyGetters;

/// Endpoint information for a broker: id, host, port.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/.../network/BrokerEndPoint.java
#[derive(Debug, Clone, PartialEq, Eq, CopyGetters)]
pub struct BrokerEndPoint {
    #[get_copy = "pub"]
    id: i32,
    host: String,
    #[get_copy = "pub"]
    port: i32,
}

impl BrokerEndPoint {
    /// Create a new BrokerEndPoint.
    ///
    /// MIGRATION_SOURCE: server-common/.../network/BrokerEndPoint.java
    pub fn new(id: i32, host: String, port: i32) -> Self {
        BrokerEndPoint { id, host, port }
    }

    /// The host name for this broker.
    ///
    /// MIGRATION_SOURCE: server-common/.../network/BrokerEndPoint.java
    pub fn host(&self) -> &str {
        &self.host
    }
}

impl Default for BrokerEndPoint {
    fn default() -> Self {
        BrokerEndPoint::new(-1, String::new(), -1)
    }
}
