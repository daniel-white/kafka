//! ClientIdAndBroker: pairing of client ID with broker host and port.
//!
//! Mirrors Java's `ClientIdAndBroker` class.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/ClientIdAndBroker.java

use getset::{CopyGetters, Getters};
use std::fmt;

/// A client ID paired with a broker endpoint.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/.../ClientIdAndBroker.java
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct ClientIdAndBroker {
    #[get = "pub"]
    client_id: String,
    #[get = "pub"]
    broker_host: String,
    #[get_copy = "pub"]
    broker_port: i32,
}

impl ClientIdAndBroker {
    /// Create a new ClientIdAndBroker.
    ///
    /// MIGRATION_SOURCE: server-common/.../ClientIdAndBroker.java
    pub fn new(client_id: String, broker_host: String, broker_port: i32) -> Self {
        ClientIdAndBroker {
            client_id,
            broker_host,
            broker_port,
        }
    }
}

impl PartialEq for ClientIdAndBroker {
    fn eq(&self, other: &Self) -> bool {
        self.client_id == other.client_id
            && self.broker_host == other.broker_host
            && self.broker_port == other.broker_port
    }
}

impl Eq for ClientIdAndBroker {}

impl fmt::Display for ClientIdAndBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.client_id, self.broker_host, self.broker_port
        )
    }
}

impl Default for ClientIdAndBroker {
    fn default() -> Self {
        ClientIdAndBroker::new(String::new(), String::new(), -1)
    }
}
